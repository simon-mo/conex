use std::path::PathBuf;
use std::{io::Read, path::Path};

use bollard::Docker;
use futures::stream::TryStreamExt;
use oci_spec::image::{Descriptor, ImageConfiguration, ImageManifest, MediaType};
use reqwest::Client;
use tracing::{info, warn};

use crate::repo_info::RepoInfo;

pub struct BlockingReader<T>
where
    T: Read,
{
    inner_reader: T,
}

impl<T> BlockingReader<T>
where
    T: Read,
{
    pub fn new(inner_reader: T) -> Self {
        BlockingReader { inner_reader }
    }
}
impl<T> Read for BlockingReader<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner_reader.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // note: spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }
}

async fn download_layer(
    client: Client,
    blob_store_path: PathBuf,
    repo_info: RepoInfo,
    descriptor: Descriptor,
) {
    assert!(
        descriptor.media_type() == &MediaType::ImageLayerZstd,
        "conex only support zstd layer at the moment"
    );

    let resp = client
        .execute(repo_info.get_blob_request(descriptor.digest()))
        .await
        .unwrap();

    // TODO: handle redirect
    // if resp.status() == 307 {
    //     unimplemented!("redirect is not supported yet");
    // } else {

    assert!(resp.status() == 200, "layer status: {}", resp.status());
    assert!(resp.content_length().unwrap() == descriptor.size() as u64);

    // Make the directory for the layer, trim ":" because it create troubles in
    // overlayfs mount array.
    let digest = descriptor.digest().split(':').nth(1).unwrap().to_string();
    let unpack_location = blob_store_path.join(&digest);
    // TODO: if the location is not empty, we should check it has the entirity of the content
    // but for now, let's clean it up and download again.
    if unpack_location.exists() {
        warn!("{} already exists, removing", unpack_location.display());
        std::fs::remove_dir_all(&unpack_location).unwrap();
    }
    std::fs::create_dir_all(&unpack_location).unwrap();

    let start = std::time::Instant::now();
    let total_bytes = resp.content_length().unwrap();

    let raw_to_decode_buff = async_ringbuf::AsyncHeapRb::<u8>::new(2 * 1024 * 1024);
    let (mut write_raw, read_raw) = raw_to_decode_buff.split();
    let decode_to_untar_buff = async_ringbuf::AsyncHeapRb::<u8>::new(2 * 1024 * 1024);
    let (mut write_decoded, mut read_decoded) = decode_to_untar_buff.split();

    let mut work_set = tokio::task::JoinSet::new();
    work_set.spawn(async move {
        let bufreader = tokio::io::BufReader::with_capacity(64 * 1024, read_raw);
        let mut reader = async_compression::tokio::bufread::ZstdDecoder::new(bufreader);
        tokio::io::copy(&mut reader, &mut write_decoded)
            .await
            .unwrap();
    });

    let untar_thread = tokio::task::spawn_blocking(move || {
        let read_decoded_sync = read_decoded.as_mut_base();
        let mut tar = tar::Archive::new(BlockingReader::new(read_decoded_sync));

        let unpack_to = unpack_location.clone();

        for entry in tar.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().display().to_string();

            match entry.header().entry_type() {
                tar::EntryType::Directory => {
                    info!("Creating directory {}", &path);
                    let path = Path::new(&unpack_to).join(path.clone());
                    std::fs::create_dir_all(&path).unwrap();
                    continue;
                }
                tar::EntryType::Symlink => {
                    let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                    let path = Path::new(&unpack_to).join(path.clone());

                    // symlink should point to an "abosolute" path as seen in the layer.
                    // this we are writing the target_path "as is".

                    // let target_path = Path::new(&unpack_to).join(target_path);
                    info!("Creating symlink {:?} -> {:?}", &path, &target_path);
                    std::os::unix::fs::symlink(&target_path, &path).unwrap();
                    continue;
                }
                tar::EntryType::Link => {
                    let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                    let path: std::path::PathBuf = Path::new(&unpack_to).join(path.clone());
                    let target_path = Path::new(&unpack_to).join(target_path);
                    // the target_path should already exist in the same archive.
                    assert!(target_path.is_file(), "{:?}", target_path.display());
                    info!("Creating hard link {:?} -> {:?}", &path, &target_path);
                    std::fs::hard_link(&target_path, &path).unwrap();
                    continue;
                }
                tar::EntryType::Regular => {
                    info!("Handling regular file {:?}", &path);
                    let full_path = Path::new(&unpack_to.clone()).join(path);
                    let file_parent_dir = full_path.parent().unwrap();
                    std::fs::create_dir_all(&file_parent_dir).unwrap();

                    // TODO: set the actual mode
                    use std::os::unix::fs::OpenOptionsExt;
                    let mut file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .mode(777)
                        .open(&full_path)
                        .unwrap();
                    std::io::copy(&mut entry, &mut file).unwrap();
                }
                _ => {}
            }
        }
    });

    let mut output_from_socket = tokio_util::io::StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );
    tokio::io::copy_buf(&mut output_from_socket, &mut write_raw)
        .await
        .unwrap();

    while let Some(res) = work_set.join_next().await {
        res.unwrap();
    }
    untar_thread.await.unwrap();

    info!(
        "Fetched {} in {:.3}s, {:.2}mb",
        digest,
        start.elapsed().as_millis() as f64 / 1e3,
        total_bytes as f64 / 1e6
    );
}

pub struct ContainerPuller {
    docker: Docker,
    client: Client,
    blob_store_path: PathBuf,
}

impl ContainerPuller {
    pub fn new(docker: Docker, blob_store_path: PathBuf) -> Self {
        // ensure blob_store_path is a directory, create if not exists
        if !blob_store_path.exists() {
            std::fs::create_dir_all(&blob_store_path).unwrap();
        }

        Self {
            docker,
            client: Client::new(),
            blob_store_path,
        }
    }

    pub async fn pull(&self, image_tag: String, jobs: usize, show_progress: bool) {
        let repo_info = RepoInfo::from_string(image_tag).await;

        let manifest = self.download_manifest(&repo_info).await;
        let _config = self.download_config(&repo_info, &manifest.config()).await;
        self.download_layers(&repo_info, manifest.layers(), jobs, show_progress)
            .await;
    }

    async fn download_manifest(&self, repo_info: &RepoInfo) -> ImageManifest {
        let manifest = self
            .client
            .execute(repo_info.get_manifest_request())
            .await
            .unwrap();
        let manifest = manifest.json::<ImageManifest>().await.unwrap();
        manifest
    }

    async fn download_config(
        &self,
        repo_info: &RepoInfo,
        descriptor: &Descriptor,
    ) -> ImageConfiguration {
        let config = self
            .client
            .execute(repo_info.get_config_request(descriptor.digest()))
            .await
            .unwrap();

        // handle redirect
        if config.status() == 307 {
            unimplemented!("redirect is not supported yet");
        } else {
            assert!(config.status() == 200, "config status: {}", config.status());
            let config = config.json::<ImageConfiguration>().await.unwrap();
            config
        }
    }

    async fn download_layers(
        &self,
        repo_info: &RepoInfo,
        descriptors: &Vec<Descriptor>,
        jobs: usize,
        show_progress: bool,
    ) {
        let mut tasks = tokio::task::JoinSet::new();
        let sema = std::sync::Arc::new(tokio::sync::Semaphore::new(jobs));

        descriptors.iter().for_each(|descriptor| {
            let client = self.client.clone();
            let blob_store_path = self.blob_store_path.clone();
            let repo_info = repo_info.clone();
            let descriptor = descriptor.clone();
            let sema = sema.clone();

            tasks.spawn(async move {
                let _ = sema.acquire().await.unwrap();
                download_layer(client, blob_store_path, repo_info, descriptor).await;
            });
        });

        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }
}
