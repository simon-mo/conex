use bollard::{service::ContainerConfig, Docker};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use oci_spec::image::Descriptor;
use reqwest::{Client, RequestBuilder};
use std::{collections::HashMap, env, path::PathBuf, sync::Arc, time::Instant};
use tracing::info;

use crate::{
    data_store::DataStore,
    planner::ConexPlanner,
    progress::{UpdateItem, UpdateType},
    repo_info::RepoInfo,
    uploader::ConexUploader,
    METADATA_DB_PATH,
};

pub struct ContainerPusher {
    docker: Docker,
    client: Client,
}

fn convert_docker_config_to_oci_config(src_config: ContainerConfig) -> oci_spec::image::Config {
    let mut config = oci_spec::image::Config::default();
    config.set_user(src_config.user.filter(|u| !u.is_empty()));
    config.set_exposed_ports(
        src_config
            .exposed_ports
            .map(|p| p.keys().cloned().collect::<Vec<String>>()),
    );
    config.set_env(src_config.env);
    config.set_cmd(src_config.cmd);
    config.set_volumes(
        src_config
            .volumes
            .map(|v| v.keys().cloned().collect::<Vec<String>>()),
    );
    config.set_working_dir(src_config.working_dir.filter(|u| !u.is_empty()));
    config.set_entrypoint(src_config.entrypoint);
    config.set_labels(src_config.labels);
    config.set_stop_signal(src_config.stop_signal);
    config
}

impl ContainerPusher {
    pub fn new(docker: Docker) -> Self {
        Self {
            docker,
            client: Client::new(),
        }
    }
    pub async fn push(&self, source_image: String, jobs: usize, show_progress: bool, local_image_path: PathBuf) {
        let repo_info = RepoInfo::from_string(source_image.clone()).await;

        let mut layer_descriptors = self
            .push_blobs(repo_info.clone(), jobs, show_progress, local_image_path.clone())
            .await;
        info!("Pushed blobs: {:?}", layer_descriptors);
        let config_descriptor = self
            .push_config(repo_info.clone(), layer_descriptors.clone())
            .await;
        self.push_manifest(repo_info.clone(), config_descriptor, layer_descriptors)
            .await;
    }

    pub async fn push_config(
        &self,
        repo_info: RepoInfo,
        layer_descriptors: Vec<Descriptor>,
    ) -> Descriptor {
        // Upload the config
        let image_info = self
            .docker
            .inspect_image(&repo_info.raw_tag)
            .await
            .expect("Unable to find image");

        let src_config = image_info.config.unwrap();
        let config = convert_docker_config_to_oci_config(src_config);

        let root_fs_builder = oci_spec::image::RootFsBuilder::default().diff_ids(
            layer_descriptors
                .iter()
                // TODO: i think the diff ids should be the uncomressed sha256 but we are just using the compressed atm.
                .map(|d| {
                    d.annotations()
                        .as_ref()
                        .unwrap()
                        .get("org.conex.diff_id")
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>(),
        );
        let root_fs = root_fs_builder.build().unwrap();

        let raw_history = self.docker.image_history(&repo_info.raw_tag).await.unwrap();
        let history = raw_history
            .into_iter()
            .map(|h| {
                oci_spec::image::HistoryBuilder::default()
                    .created_by(h.created_by)
                    .comment(h.comment)
                    .empty_layer(h.size == 0)
                    .build()
                    .unwrap()
            })
            // .rev()
            .collect::<Vec<_>>();

        let image_config_builder = oci_spec::image::ImageConfigurationBuilder::default()
            .architecture(oci_spec::image::Arch::Amd64)
            .config(config)
            .os(oci_spec::image::Os::Linux)
            .rootfs(root_fs)
            .history(history);
        let image_config = image_config_builder.build().unwrap();

        // TODO: use image config to bring in diff ids.
        let resp = self
            .client
            .execute(repo_info.upload_blob_request())
            .await
            .unwrap();
        let upload_url = resp.headers().get("location").unwrap();
        let serialized_config = serde_json::to_string(&image_config).unwrap();
        let sha256_hash = data_encoding::HEXLOWER.encode(
            ring::digest::digest(&ring::digest::SHA256, serialized_config.as_bytes()).as_ref(),
        );
        let content_length = serialized_config.len();
        let resp = self
            .client
            .execute({
                let req = self
                    .client
                    .put(upload_url.to_str().unwrap())
                    .body(serialized_config)
                    .query(&[("digest", format!("sha256:{}", sha256_hash))]);

                if let Some(token) = repo_info.auth_token.as_ref() {
                    req.header("Authorization", token).build().unwrap()
                } else {
                    req.build().unwrap()
                }
            })
            .await
            .unwrap();
        assert!(
            resp.status() == 201,
            "Failed to upload config: {}",
            resp.text().await.unwrap()
        );

        info!("Uploaded config: {:?}", image_config);

        oci_spec::image::DescriptorBuilder::default()
            .media_type("application/vnd.oci.image.config.v1+json")
            .digest(format!("sha256:{}", sha256_hash))
            .size(content_length as i64)
            .build()
            .unwrap()
    }

    pub async fn push_manifest(
        &self,
        repo_info: RepoInfo,
        config_descriptor: Descriptor,
        layer_descriptors: Vec<Descriptor>,
    ) {
        let manifest = oci_spec::image::ImageManifestBuilder::default()
            .schema_version(oci_spec::image::SCHEMA_VERSION)
            .media_type("application/vnd.oci.image.manifest.v1+json")
            .config(config_descriptor)
            .layers(layer_descriptors)
            .build()
            .unwrap();
        let serialized_manifest = serde_json::to_string(&manifest).unwrap();
        let sha256_hash = data_encoding::HEXLOWER.encode(
            ring::digest::digest(&ring::digest::SHA256, serialized_manifest.as_bytes()).as_ref(),
        );
        let content_length = serialized_manifest.len();
        info!("Uploading manifest: {:?}", manifest);

        for tag in &[
            repo_info.reference.tag().unwrap_or("latest"),
            &sha256_hash.clone()[..6],
        ] {
            let resp = self
                .client
                .execute(
                    RequestBuilder::from_parts(
                        self.client.clone(),
                        repo_info.upload_manifest_request(tag),
                    )
                    .body(serialized_manifest.clone())
                    .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                    .header("Content-Length", content_length)
                    .build()
                    .unwrap(),
                )
                .await
                .unwrap();
            assert!(resp.status() == 201);
        }
    }

    pub async fn push_blobs(
        &self,
        repo_info: RepoInfo,
        jobs: usize,
        show_progress: bool,
        local_image_path: PathBuf,
    ) -> Vec<Descriptor> {
        let image_info = self
            .docker
            .inspect_image(&repo_info.raw_tag)
            .await
            .expect("Unable to find image");

        let graph_driver = image_info.graph_driver.expect("No graph driver");

        let mut layers: Vec<String> = {
            if graph_driver.name == "overlay2" {
                let upper_layer = graph_driver
                    .data
                    .get("UpperDir")
                    .expect("No UpperDir")
                    .to_owned();
                let lower_layers: Vec<String> = match graph_driver.data.get("LowerDir") {
                    Some(lower_dir) => lower_dir.split(':').map(|s| s.to_owned()).collect(),
                    None => Vec::new(),
                };

                println!("upper layer: {}", upper_layer);
                println!("lower layers: {:?}", lower_layers);

                vec![upper_layer].into_iter().chain(lower_layers).collect()
            } else if graph_driver.name == "conex" {
                let store = DataStore::new(METADATA_DB_PATH.into());
                let all_names = store
                    .get_all_info()
                    .iter()
                    .map(|info| info.name.clone())
                    .collect::<Vec<_>>();
                image_info
                    .root_fs
                    .unwrap()
                    .layers
                    .unwrap()
                    .iter()
                    .map(|diff_id| {
                        info!("Looking for diff id: {}", diff_id);
                        let sha_option_1 = all_names
                            .iter()
                            .find(|name| name.contains(diff_id))
                            .map(|found_name| store.find_sha_by_key(found_name).unwrap());

                        let sha_option_2 = {
                            let metadata_path = PathBuf::from("/var/lib/docker/image/overlay2/distribution/v2metadata-by-diffid/sha256/")
                                .join(diff_id.strip_prefix("sha256:").unwrap());
                            if metadata_path.exists() {
                                let metadata = std::fs::read_to_string(metadata_path).unwrap();
                                let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();
                                let sha = metadata[0]["Digest"].as_str().unwrap().strip_prefix("sha256:").unwrap();
                                Some(sha.to_owned())
                            } else {
                                None
                            }
                        };
                        
                        // Note(ex) unclear to me what this is doing with each layer diff_id: isn't diff_id already the sha?
                        info!("Option 1: {:?}, Option 2: {:?}", sha_option_1, sha_option_2);

                        sha_option_1.or(sha_option_2).unwrap()
                    })
                    .collect()
            } else {
                unreachable!("Unknown graph driver, expected overlay2 or conex")
            }
        };
        // We need to reverse the layers because top layer is currently the newest layer.
        // layers.reverse();

        info!("Image {} has {} layers", &repo_info.raw_tag, layers.len());

        // TODO: move this to shared code with puller later.
        let m = Arc::new(MultiProgress::with_draw_target(
            ProgressDrawTarget::stdout_with_hz(20),
        ));
        let sty = ProgressStyle::with_template("{spinner:.green} ({msg}) [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes:>12}/{total_bytes:>12} ({bytes_per_sec:>15}, {eta:>5})")
        .unwrap()
        .progress_chars("#>-");

        // TODO: the next step is re-use the logic from sky-packer to generate a "plan"  for the
        // split files.
        // TODO: upload the oci config as well. ensure we can actually pull it.
        // TODO: add some progress bar niceties: remove the completed pbar, make the as logs
        let timer_planner_start = Instant::now();
        let mut planner = ConexPlanner::default();
        for layer in layers {
            planner.ingest_dir(&layer);
        }
        let plan = planner.generate_plan();
        info!("Generated plan in {:?}", timer_planner_start.elapsed());

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<UpdateItem>();
        let layers_to_size = plan
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .sorted_by(|a, b| a.inode.cmp(&b.inode))
                        .group_by(|a| a.inode)
                        .into_iter()
                        .map(|(_, g)| g.last().unwrap().size)
                        .sum(),
                )
            })
            .collect::<Vec<(String, usize)>>();
        info!("Layer sizes: {:?}", layers_to_size);
        let uploader = ConexUploader::new(self.client.clone(), repo_info.clone(), progress_tx, local_image_path.clone());
        let upload_task = tokio::spawn(async move { uploader.upload(plan, jobs).await });

        if show_progress {
            let bars = layers_to_size
                .into_iter()
                .map(|(layer_id, total_size)| {
                    let pbar = m.add(indicatif::ProgressBar::new(0));
                    pbar.set_style(sty.clone());
                    pbar.set_message(layer_id.clone());
                    pbar.set_length(total_size as u64);
                    (layer_id, pbar)
                })
                .collect::<HashMap<String, ProgressBar>>();

            while let Some(item) = progress_rx.recv().await {
                if item.update_type == UpdateType::TarAdd {
                    let pbar = bars.get(&item.key).unwrap();
                    pbar.inc(item.delta as u64);
                }
            }

            // Completed, close the progress bars.
            bars.values().for_each(|pbar| pbar.finish());
        }

        upload_task.await.unwrap()
    }
}
