use crate::hash::StreamingHashWriter;
use crate::planner::ConexFile;
use crate::planner::ConexPlanner;
use crate::progress::{ProgressStreamer, ProgressWriter, UpdateItem};
use crate::repo_info::RepoInfo;
use bytes::Bytes;
use oci_spec::image::Descriptor;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::vec;
use tar::{Builder as TarBuilder, EntryType, Header};
use std::fs::File;
use std::io::prelude::*;
use std::fs;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use std::fs::Metadata;

use zstd::stream::write::Encoder as ZstdEncoder;
pub struct BlockingWriter<T>
where
    T: Write,
{
    inner: T,
}

impl<T> BlockingWriter<T>
where
    T: Write,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Write for BlockingWriter<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.write(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // note: spin here
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
pub struct ConexUploader {
    client: Client,
    repo_info: RepoInfo,
    progress_sender: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SplitMetadata {
    path: String,
    start_offset: u32,
    chunk_size: u32,
    total_size: u64,
}

const UPLOAD_CHUNK_SIZE: usize = 512 * 1024 * 1024;
async fn upload_layer(
    client: Client,
    repo_info: RepoInfo,
    key: String,
    files: Vec<ConexFile>,
    progress_sender: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
) -> Descriptor {
    let (mut producer, mut consumer) =
        async_ringbuf::AsyncHeapRb::<u8>::new(2 * UPLOAD_CHUNK_SIZE).split();
    let (hash_socket_tx, hash_socket_rx) = tokio::sync::oneshot::channel::<String>();
    let (hash_descriptor_tx, hash_descriptor_rx) = tokio::sync::oneshot::channel::<String>();
    let (raw_hash_tx, raw_hash_rx) = tokio::sync::oneshot::channel::<String>();
    let mut upload_tasks = tokio::task::JoinSet::new();

    let key_ = key.clone();
    let progress_sender_ = progress_sender.clone();
    upload_tasks.spawn_blocking(move || {
        let (bytes_written_tx, bytes_written_rx) = tokio::sync::oneshot::channel::<usize>();
        let blocking_writer = BlockingWriter::new(producer.as_mut_base());
        let compressed_hasher = StreamingHashWriter::new(
            blocking_writer,
            Some(Box::new(move |digest, bytes_written| {
                hash_socket_tx.send(digest.clone()).unwrap();
                hash_descriptor_tx.send(digest).unwrap();
                bytes_written_tx.send(bytes_written).unwrap();
            })),
        );
        let encoder = ZstdEncoder::new(compressed_hasher, 0)
            .unwrap()
            .auto_finish();
        let raw_tar_hasher = StreamingHashWriter::new(
            encoder,
            Some(Box::new(move |digest, _| {
                raw_hash_tx.send(digest).unwrap();
            })),
        );
        let progress_writer = ProgressWriter::new(
            raw_tar_hasher,
            key_,
            crate::progress::UpdateType::TarAdd,
            progress_sender_,
        );
        let mut tar_builder = TarBuilder::new(progress_writer);
        tar_builder.follow_symlinks(false);
        for file in files {
            let meta = file.path.symlink_metadata().unwrap().clone();
            match file.hard_link_to {
                Some(hard_link_to) => {
                    let mut header = Header::new_gnu();
                    header.set_metadata(&meta.clone());
                    header.set_size(file.size.clone() as u64);
                    header.set_entry_type(EntryType::Link);
                    tar_builder
                    .append_link(&mut header, file.relative_path.clone(), hard_link_to)
                    .unwrap();        
                }
                None => {
                    let mut relative_path = file.relative_path.clone().to_str().unwrap().to_string();
                    if meta.is_dir()
                        // && !meta.is_symlink()
                        && !relative_path.is_empty()
                        && !relative_path.ends_with('/')
                    {
                        relative_path.push('/');
                    }

                    if !meta.is_dir() && !meta.is_file() && !meta.is_symlink() {
                        // info!(
                        //     "Unsupported file type {:?} for file {:?}",
                        //     meta.file_type(),
                        //     file.path
                        // );
                        continue;
                    }
                    if meta.is_file() && file.start_offset.is_some() {
                        let path = file.path.to_str().unwrap().to_string();
                        //Write fragment metadata into tar
                        let split_metadata = SplitMetadata {
                            path: path.clone(),
                            start_offset: file.start_offset.unwrap() as u32,
                            chunk_size: file.chunk_size.unwrap() as u32,
                            total_size: file.size as u64,
                        };
                        let metadata_json = serde_json::to_string(&split_metadata).unwrap();
                        let metadata_json_bytes = metadata_json.as_bytes();
                        let mut metadata_header = Header::new_gnu();
                        metadata_header.set_size(metadata_json_bytes.len() as u64);
                        metadata_header.set_cksum();
                        let rel_path = file.relative_path.to_str().unwrap().to_string();
                        tar_builder
                            .append_data(
                                &mut metadata_header,
                                format!("{}.split-metadata.{}.json", rel_path, file.segment_idx.unwrap().clone()),
                                metadata_json_bytes,
                            )
                            .unwrap();
                            //Write fragment into tar
                        let mut chunk_header = Header::new_gnu();
                        chunk_header.set_size(file.chunk_size.unwrap() as u64);
                        chunk_header.set_entry_type(tar::EntryType::Regular);
                        chunk_header.set_path(&rel_path).unwrap();
                        chunk_header.set_uid(meta.clone().uid().into());                            
                        chunk_header.set_gid(meta.clone().gid().into());
                        chunk_header.set_cksum();
                        //let mut chunk_data = entry.take(chunk_size as u64);
                        //Is it okay to do this + memory inefficient?
                        let mut hard_file = File::open(path.clone()).unwrap();
                        let mut buffer = Vec::new();
                        let _ = hard_file.read_to_end(&mut buffer);
                        let start = file.start_offset.unwrap();
                        let end = start + file.chunk_size.unwrap();
                        assert!(start <= end);
                        assert!(end <= file.size);
                        let mut chunk_data = &buffer[start..end];
                        tar_builder
                            .append(&chunk_header, &mut chunk_data)
                            .unwrap();
                    } else {
                        tar_builder
                        .append_path_with_name(&file.path, &relative_path)
                        .unwrap_or_else(|e| {
                            panic!(
                                "Failed to add file {:?}, {}. Original error: {:?}",
                                file.path, relative_path, e
                            )
                        });
                    
                    }
                }
            } 
        }

        tar_builder.finish().unwrap();
        // Trigger encoding and hash compute to finish.
        // This should also drop the producer trigger buffer to close.
        drop(tar_builder);

        bytes_written_rx.blocking_recv().unwrap()
    });

    //let mut layer_cpy = File::create("layer.txt").unwrap();

    // let client = client.clone();
    upload_tasks.spawn(async move {
        // Implementing the chunked upload protocol.
        // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pushing-a-blob-in-chunks

        let create_upload_resp = client
            .execute(repo_info.upload_blob_request())
            .await
            .unwrap();
        assert_eq!(create_upload_resp.status(), 202);
        let mut location = create_upload_resp
            .headers()
            .get("Location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let min_chunk_size = create_upload_resp
            .headers()
            .get("OCI-Chunk-Min-Length")
            .unwrap_or(&reqwest::header::HeaderValue::from_static("0"))
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap();

        let chunk_size = std::cmp::max(min_chunk_size, UPLOAD_CHUNK_SIZE);

        let mut start_offset = 0;
        loop {
            let mut send_buffer = vec![0; chunk_size];

            consumer.wait(chunk_size).await;
            let buf_len = {
                let slice = send_buffer.as_mut_slice();
                match consumer.pop_slice(slice).await {
                    Ok(()) => chunk_size,
                    Err(count) => count,
                }
            };

            let streamer = ProgressStreamer::new(
                progress_sender.clone(),
                key.clone(),
                crate::progress::UpdateType::SocketSend,
                Bytes::copy_from_slice(&send_buffer[0..buf_len]),
            );
            
            //let chunk_data = Bytes::copy_from_slice(&send_buffer[0..buf_len]);
            //layer_cpy.write_all(&chunk_data);
            
            // Note that because content range is inclusive, we need to subtract 1 from the end offset.
            let upload_chunk_resp = client
                .execute({
                    let req = client
                        .patch(location)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", buf_len)
                        .header(
                            "Content-Range",
                            format!("{}-{}", start_offset, start_offset + buf_len - 1),
                        )
                        // .body(send_buffer[0..buf_len].to_vec())
                        .body(reqwest::Body::wrap_stream(streamer));

                    if let Some(token) = repo_info.auth_token.as_ref() {
                        req.header("Authorization", token).build().unwrap()
                    } else {
                        req.build().unwrap()
                    }
                })
                .await
                .unwrap();
            assert!(
                upload_chunk_resp.status() == 202,
                "{}",
                upload_chunk_resp.text().await.unwrap()
            );
            start_offset += buf_len;

            location = upload_chunk_resp
                .headers()
                .get("Location")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            if consumer.is_closed() && consumer.is_empty() {
                break;
            }
        }

        // Finish the upload with the final PUT request
        let hash = hash_socket_rx.await.unwrap();
        let url = format!("{}&digest=sha256:{}", location, hash); //TODO: proper url parse
        let upload_final_resp = client
            .execute({
                let req = client.put(&url);
                if let Some(token) = repo_info.auth_token.as_ref() {
                    req.header("Authorization", token).build().unwrap()
                } else {
                    req.build().unwrap()
                }
            })
            .await
            .unwrap();
        assert!(upload_final_resp.status() == 201);

        start_offset
    });

    let mut bytes_count = HashSet::new();
    while let Some(result) = upload_tasks.join_next().await {
        let bytes_written = result.unwrap();
        bytes_count.insert(bytes_written);
    }
    assert!(
        bytes_count.len() == 1,
        "Bytes written mismatch, expected 1, got {:?}",
        bytes_count
    );
    let size = bytes_count.iter().next().unwrap();

    let hash = hash_descriptor_rx.await.unwrap();

    let mut annotations = HashMap::<String, String>::new();
    annotations.insert(
        "org.conex.diff_id".to_string(),
        format!("sha256:{}", raw_hash_rx.await.unwrap()),
    );
    oci_spec::image::DescriptorBuilder::default()
        .media_type("application/vnd.oci.image.layer.v1.tar+zstd")
        .annotations(annotations) // TODO: add annotatoins
        .digest(format!("sha256:{}", hash))
        .size(*size as i64)
        .build()
        .unwrap()
}

impl ConexUploader {
    pub fn new(
        client: Client,
        repo_info: RepoInfo,
        progress_sender: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
    ) -> Self {
        Self {
            client,
            repo_info,
            progress_sender,
        }
    }

    pub async fn upload(
        &self,
        plan: Vec<(String, Vec<ConexFile>)>,
        jobs: usize,
    ) -> Vec<Descriptor> {
        let keys = plan
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<String>>();
        let mut parallel_uploads = tokio::task::JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(jobs));
        plan.into_iter().for_each(|(layer_id, paths)| {
            let semaphore = semaphore.clone();
            let client = self.client.clone();
            let repo_info = self.repo_info.clone();
            let progress_sender = self.progress_sender.clone();
            
            parallel_uploads.spawn(async move {
                let permit = semaphore.acquire().await;
                let content_hash = upload_layer(
                    client,
                    repo_info,
                    layer_id.clone(),
                    paths.clone(),
                    progress_sender,
                )
                .await;

                drop(permit);

                (layer_id, content_hash)
            });
        });

        let mut result_map = HashMap::<String, Descriptor>::new();
        while let Some(result) = parallel_uploads.join_next().await {
            let (layer_id, content_hash) = result.unwrap();
            result_map.insert(layer_id.clone(), content_hash);
        }

        // Order the descriptors in the same order as the plan.
        keys.iter()
            .map(|layer_id| result_map[layer_id].clone())
            .collect()
    }
}
