use crate::hash::StreamingHashWriter;
use crate::planner::ConexFile;
use reqwest::Client;
use std::collections::HashMap;
use std::vec;
use tar::Builder as TarBuilder;
use zstd::stream::write::Encoder as ZstdEncoder;

pub struct ConexUploader {
    client: Client,
    repo_name: String,
}

const UPLOAD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
async fn upload_layer(
    client: Client,
    repo_name: String,
    _key: String,
    files: Vec<ConexFile>,
) {
    let (mut producer, mut consumer) =
        async_ringbuf::AsyncHeapRb::<u8>::new(2 * UPLOAD_CHUNK_SIZE).split();
    let (hash_tx, hash_rx) = tokio::sync::oneshot::channel::<String>();
    let mut upload_tasks = tokio::task::JoinSet::new();

    upload_tasks.spawn_blocking(move || {
        let hasher = StreamingHashWriter::new(
            producer.as_mut_base(),
            Some(Box::new(move |digest| {
                hash_tx.send(digest).unwrap();
            })),
        );
        let encoder = ZstdEncoder::new(hasher, 0).unwrap().auto_finish();
        let mut tar_builder = TarBuilder::new(encoder);

        for file in files {
            if file.relative_path.ends_with("layer-1") {
                println!("packing layer-1 {:?}", file);
            }
            tar_builder
                .append_path_with_name(file.path, file.relative_path)
                .unwrap();
        }

        tar_builder.finish().unwrap();
        // Trigger encoding and hash compute to finish.
        // This should also drop the producer trigger buffer to close.
        drop(tar_builder);
    });

    // let client = client.clone();
    upload_tasks.spawn(async move {
        // Implementing the chunked upload protocol.
        // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pushing-a-blob-in-chunks

        let create_upload_resp = client
            .post(format!(
                "http://localhost:5000/v2/{}/blobs/uploads/",
                repo_name
            ))
            .send()
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

            println!(
                "Sending chunk: {}-{}",
                start_offset,
                start_offset + buf_len - 1
            );

            // Note that because content range is inclusive, we need to subtract 1 from the end offset.
            let upload_chunk_resp = client
                .patch(location)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", buf_len)
                .header(
                    "Content-Range",
                    format!("{}-{}", start_offset, start_offset + buf_len - 1),
                )
                .body(send_buffer[0..buf_len].to_vec())
                .send()
                .await
                .unwrap();
            assert!(upload_chunk_resp.status() == 202);
            start_offset += buf_len + 1;

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
        let hash = hash_rx.await.unwrap();
        let url = format!("{}&digest=sha256:{}", location, hash); //TODO: proper url parse
        let upload_final_resp = client.put(&url).send().await.unwrap();
        assert!(upload_final_resp.status() == 201);
    });

    while let Some(result) = upload_tasks.join_next().await {
        result.unwrap();
    }
}

impl ConexUploader {
    pub fn new(repo_name: String) -> Self {
        Self {
            client: Client::new(),
            repo_name,
        }
    }

    pub async fn upload(&self, plan: HashMap<String, Vec<ConexFile>>) {
        let mut parallel_uploads = tokio::task::JoinSet::new();
        plan.into_iter().for_each(|(layer_id, paths)| {
            parallel_uploads.spawn(upload_layer(
                self.client.clone(),
                self.repo_name.clone(),
                layer_id,
                paths,
            ));
        });
        while let Some(result) = parallel_uploads.join_next().await {
            result.unwrap();
        }
    }
}
