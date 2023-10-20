use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use oci_spec::image::{Descriptor, ImageConfiguration, ImageManifest, MediaType};
use reqwest::Client;
use tracing::debug;

use crate::progress::{AsyncProgressReader, UpdateItem, UpdateType};
use crate::repo_info::RepoInfo;
use crate::{BLOB_LOCATION, MOUNT_LOCATION};

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
    progress_tx: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
) {
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
    let unpack_location_short = blob_store_path.join(&digest[..8]);
    // TODO: if the location is not empty, we should check it has the entirity of the content
    // but for now, let's clean it up and download again.
    if unpack_location.exists() {
        debug!("{} already exists, removing", unpack_location.display());
        std::fs::remove_dir_all(&unpack_location).unwrap();
        std::fs::remove_dir_all(&unpack_location_short).unwrap();
    }
    std::fs::create_dir_all(&unpack_location).unwrap();
    std::os::unix::fs::symlink(&unpack_location, &unpack_location_short).unwrap();

    let start = std::time::Instant::now();
    let total_bytes = resp.content_length().unwrap();

    let raw_to_decode_buff = async_ringbuf::AsyncHeapRb::<u8>::new(2 * 1024 * 1024);
    let (mut write_raw, read_raw) = raw_to_decode_buff.split();
    let decode_to_untar_buff = async_ringbuf::AsyncHeapRb::<u8>::new(2 * 1024 * 1024);
    let (mut write_decoded, mut read_decoded) = decode_to_untar_buff.split();

    let mut work_set = tokio::task::JoinSet::new();
    let progress_key = digest.clone();
    let media_type = descriptor.media_type().clone();
    work_set.spawn(async move {
        let progress_reader =
            AsyncProgressReader::new(read_raw, progress_key, UpdateType::SocketRecv, progress_tx);
        let bufreader = tokio::io::BufReader::with_capacity(64 * 1024, progress_reader);

        let mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = match media_type {
            MediaType::ImageLayerGzip => Box::new(
                async_compression::tokio::bufread::GzipDecoder::new(bufreader),
            ),
            MediaType::Other(s) => {
                assert!(s == "application/vnd.docker.image.rootfs.diff.tar.gzip");
                Box::new(async_compression::tokio::bufread::GzipDecoder::new(
                    bufreader,
                ))
            }
            MediaType::ImageLayerZstd => Box::new(
                async_compression::tokio::bufread::ZstdDecoder::new(bufreader),
            ),
            _ => {
                unimplemented!(
                    "conex only support zstd or gzip layer at the moment, but got {:?}",
                    media_type
                );
            }
        };

        tokio::io::copy(&mut reader, &mut write_decoded)
            .await
            .unwrap();
    });

    let untar_thread = tokio::task::spawn_blocking(move || {
        let read_decoded_sync = read_decoded.as_mut_base();
        let mut tar = tar::Archive::new(BlockingReader::new(read_decoded_sync));
        tar.set_preserve_ownerships(true);
        tar.set_preserve_permissions(true);
        tar.set_preserve_mtime(true);

        let unpack_to = unpack_location.clone();

        for entry in tar.entries().unwrap() {
            let mut entry = entry.unwrap();
            entry.unpack_in(&unpack_to).unwrap();

            // let path = entry.path().unwrap().display().to_string();
            // match entry.header().entry_type() {
            //     tar::EntryType::Directory => {
            //         // info!("Creating directory {}", &path);
            //         let path = Path::new(&unpack_to).join(path.clone());
            //         std::fs::create_dir_all(&path).unwrap();
            //         continue;
            //     }
            //     tar::EntryType::Symlink => {
            //         let target_path = entry.link_name().unwrap().unwrap().display().to_string();
            //         let path = Path::new(&unpack_to).join(path.clone());

            //         // symlink should point to an "abosolute" path as seen in the layer.
            //         // this we are writing the target_path "as is".

            //         // let target_path = Path::new(&unpack_to).join(target_path);
            //         // info!("Creating symlink {:?} -> {:?}", &path, &target_path);
            //         std::os::unix::fs::symlink(&target_path, &path).unwrap();
            //         continue;
            //     }
            //     tar::EntryType::Link => {
            //         let target_path = entry.link_name().unwrap().unwrap().display().to_string();
            //         let path: std::path::PathBuf = Path::new(&unpack_to).join(path.clone());
            //         let target_path = Path::new(&unpack_to).join(target_path);
            //         // the target_path should already exist in the same archive.
            //         assert!(target_path.is_file(), "{:?}", target_path.display());
            //         // info!("Creating hard link {:?} -> {:?}", &path, &target_path);
            //         std::fs::hard_link(&target_path, &path).unwrap();
            //         continue;
            //     }
            //     tar::EntryType::Regular => {
            //         // info!("Handling regular file {:?}", &path);
            //         let full_path = Path::new(&unpack_to.clone()).join(path);
            //         let file_parent_dir = full_path.parent().unwrap();
            //         std::fs::create_dir_all(file_parent_dir).unwrap();

            //         // TODO: set the actual mode
            //         use std::os::unix::fs::OpenOptionsExt;
            //         let mut file = std::fs::OpenOptions::new()
            //             .create(true)
            //             .write(true)
            //             .mode(0o777)
            //             .open(&full_path)
            //             .unwrap();
            //         std::io::copy(&mut entry, &mut file).unwrap();
            //     }
            //     _ => {}
            // }
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

    debug!(
        "Fetched {} in {:.3}s, {:.2}mb",
        digest,
        start.elapsed().as_millis() as f64 / 1e3,
        total_bytes as f64 / 1e6
    );
}

pub struct ContainerPuller {
    client: Client,
    blob_store_path: PathBuf,
}

impl ContainerPuller {
    pub fn new(blob_store_path: PathBuf) -> Self {
        // ensure blob_store_path is a directory, create if not exists
        if !blob_store_path.exists() {
            std::fs::create_dir_all(&blob_store_path).unwrap();
        }

        Self {
            client: Client::new(),
            blob_store_path,
        }
    }

    pub async fn pull(
        &self,
        image_tag: String,
        jobs: usize,
        show_progress: bool,
        skip_digests: Option<HashSet<String>>,
    ) -> Vec<String> {
        let repo_info = RepoInfo::from_string(image_tag).await;

        let manifest = self.download_manifest(&repo_info).await;
        let _config = self.download_config(&repo_info, manifest.config()).await;
        let layers_to_fetch = manifest
            .layers()
            .iter()
            .filter(|desc| match skip_digests {
                Some(ref skip_digests) => {
                    !skip_digests.contains(&desc.digest().replace("sha256:", ""))
                }
                None => true,
            })
            .collect::<Vec<&Descriptor>>();
        self.download_layers(&repo_info, layers_to_fetch.clone(), jobs, show_progress)
            .await;

        // let mount_cmd = self.make_overlay_command(manifest.layers());
        // info!("Created the following command to mount the image:");
        // info!("{}", mount_cmd);

        layers_to_fetch
            .into_iter()
            .map(|desc| desc.digest().replace("sha256:", "").to_string())
            .collect()

        // docker run --rm --mount type=bind,source=/tmp/conex-mount/flying-aphid/merged,target=/conex busybox chroot /conex ls -lh
    }

    async fn download_manifest(&self, repo_info: &RepoInfo) -> ImageManifest {
        let manifest = self
            .client
            .execute(repo_info.get_manifest_request())
            .await
            .unwrap();

        assert!(
            manifest.status() == 200,
            "manifest response:{}",
            manifest.text().await.unwrap()
        );

        manifest.json::<ImageManifest>().await.unwrap()
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

            config.json::<ImageConfiguration>().await.unwrap()
        }
    }

    async fn download_layers(
        &self,
        repo_info: &RepoInfo,
        descriptors: Vec<&Descriptor>,
        jobs: usize,
        show_progress: bool,
    ) {
        let mut tasks = tokio::task::JoinSet::new();
        let sema = std::sync::Arc::new(tokio::sync::Semaphore::new(jobs));

        // TODO: move this to shared code with puller later.
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<UpdateItem>();
        let m = Arc::new(MultiProgress::with_draw_target(
            ProgressDrawTarget::stdout_with_hz(20),
        ));
        let sty = ProgressStyle::with_template("{spinner:.green} ({msg}) [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes:>12}/{total_bytes:>12} ({bytes_per_sec:>15}, {eta:>5})")
                .unwrap()
                .progress_chars("#>-");

        descriptors.iter().for_each(|descriptor| {
            let client = self.client.clone();
            let blob_store_path = self.blob_store_path.clone();
            let repo_info = repo_info.clone();
            let descriptor = (*descriptor).to_owned();
            let sema = sema.clone();
            let progress_tx = progress_tx.clone();

            tasks.spawn(async move {
                let _ = sema.acquire().await.unwrap();
                download_layer(client, blob_store_path, repo_info, descriptor, progress_tx).await;
            });
        });

        drop(progress_tx);
        if show_progress {
            let bars = descriptors
                .iter()
                .map(|desc| {
                    let pbar = m.add(indicatif::ProgressBar::new(0));
                    pbar.set_style(sty.clone());
                    pbar.set_message(desc.digest().clone());
                    pbar.set_length(desc.size() as u64);
                    (desc.digest().split(':').nth(1).unwrap().to_owned(), pbar)
                })
                .collect::<HashMap<String, ProgressBar>>();

            while let Some(item) = progress_rx.recv().await {
                if item.update_type == UpdateType::SocketRecv {
                    let pbar = bars.get(&item.key).unwrap();
                    pbar.inc(item.delta as u64);
                }
            }

            // Completed, close the progress bars.
            bars.values().for_each(|pbar| pbar.finish());
        }

        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }

    fn _make_overlay_command(&self, layers: &[Descriptor]) -> String {
        let blob_path = PathBuf::from(BLOB_LOCATION);
        let mount_path = PathBuf::from(MOUNT_LOCATION);

        let mount_dir = {
            let p = mount_path.join(petname::petname(2, "-"));
            std::fs::create_dir_all(&p).unwrap();
            p
        };

        let lower_dirs = layers
            .iter()
            .map(|desc| {
                blob_path
                    .join(desc.digest().replace("sha256:", ""))
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .rev()
            .collect::<Vec<String>>()
            .join(":");
        let upper_dir = {
            let p = mount_dir.join("upper");
            std::fs::create_dir_all(&p).unwrap();
            p
        };
        let work_dir = {
            let p = mount_dir.join("work");
            std::fs::create_dir_all(&p).unwrap();
            p
        };
        let merged_dir = {
            let p = mount_dir.join("merged");
            std::fs::create_dir_all(&p).unwrap();
            p
        };

        let mut options: Vec<String> = vec![];
        options.push("index=off".to_string());
        options.push("userxattr".to_string());
        options.push(format!("upperdir={}", upper_dir.to_str().unwrap()));
        options.push(format!("workdir={}", work_dir.to_str().unwrap()));
        options.push(format!("lowerdir={}", lower_dirs));

        format!(
            "sudo mount -t overlay overlay -o {} {}",
            options.join(","),
            merged_dir.to_str().unwrap()
        )
    }
}
