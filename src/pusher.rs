use bollard::{service::ContainerConfig, Docker};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use oci_spec::image::{Descriptor, History};
use reqwest::Client;
use std::{collections::HashMap, sync::Arc, time::Instant};
use tracing::info;

use crate::{
    planner::ConexPlanner,
    progress::{UpdateItem, UpdateType},
    uploader::ConexUploader,
};

#[derive(Clone, Debug)]
pub struct RepoInfo {
    pub raw_tag: String, // e.g. localhost:5000/my-repo-name-image-name:latest

    pub protocol: String,    // e.g. http
    pub host: String,        // e.g. localhost:5000
    pub repo_name: String,   // e.g. my-repo-name-image-name
    pub version_tag: String, // e.g. latest
}

impl RepoInfo {
    pub fn from_string(source_image: String) -> RepoInfo {
        // Currently we are assuming source_image = localhost:5000/my-name
        let parts = source_image.split('/').collect::<Vec<&str>>();
        let protocol = "http";
        let host = parts[0];
        let repo_name = parts[1];
        let version_tag = "latest";

        Self {
            raw_tag: source_image.to_string(),
            protocol: protocol.to_string(),
            host: host.to_string(),
            repo_name: repo_name.to_string(),
            version_tag: version_tag.to_string(),
        }
    }
}

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
            .and_then(|p| Some(p.iter().map(|(k, _)| k.clone()).collect::<Vec<String>>())),
    );
    config.set_env(src_config.env);
    config.set_cmd(src_config.cmd);
    config.set_volumes(
        src_config
            .volumes
            .and_then(|v| Some(v.iter().map(|(k, _)| k.clone()).collect::<Vec<String>>())),
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
    pub async fn push(&self, source_image: String, jobs: usize) {
        let repo_info = RepoInfo::from_string(source_image.clone());

        let mut layer_descriptors = self.push_blobs(repo_info.clone(), jobs).await;
        info!("Pushed blobs: {:?}", layer_descriptors);
        layer_descriptors.reverse();
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
            .rev()
            .collect::<Vec<_>>();

        let image_config_builder = oci_spec::image::ImageConfigurationBuilder::default()
            .architecture(oci_spec::image::Arch::Amd64)
            .config(config)
            .os(oci_spec::image::Os::Linux)
            .rootfs(root_fs)
            .history(history);
        let image_config = image_config_builder.build().unwrap();

        // TODO: use image config to bring in diff ids.
        let resp = reqwest::Client::new()
            .post(format!(
                "{}://{}/v2/{}/blobs/uploads/",
                repo_info.protocol, repo_info.host, repo_info.repo_name
            ))
            .send()
            .await
            .unwrap();
        let upload_url = resp.headers().get("location").unwrap();
        let serialized_config = serde_json::to_string(&image_config).unwrap();
        let sha256_hash = data_encoding::HEXLOWER.encode(
            ring::digest::digest(&ring::digest::SHA256, serialized_config.as_bytes()).as_ref(),
        );
        let content_length = serialized_config.len();
        let resp = reqwest::Client::new()
            .put(upload_url.to_str().unwrap())
            .body(serialized_config)
            .query(&[("digest", format!("sha256:{}", sha256_hash))])
            .send()
            .await
            .unwrap();
        assert!(resp.status() == 201);

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

        for tag in vec![repo_info.version_tag, sha256_hash.clone()[..6].to_string()] {
            let resp = reqwest::Client::new()
                .put(format!(
                    "{}://{}/v2/{}/manifests/{}",
                    repo_info.protocol, repo_info.host, repo_info.repo_name, tag
                ))
                .body(serialized_manifest.clone())
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .header("Content-Length", content_length)
                .send()
                .await
                .unwrap();
            assert!(resp.status() == 201);
        }
    }

    pub async fn push_blobs(&self, repo_info: RepoInfo, jobs: usize) -> Vec<Descriptor> {
        let image_info = self
            .docker
            .inspect_image(&repo_info.raw_tag)
            .await
            .expect("Unable to find image");

        let graph_driver = image_info.graph_driver.expect("No graph driver");
        assert!(graph_driver.name == "overlay2");
        let upper_layer = graph_driver
            .data
            .get("UpperDir")
            .expect("No UpperDir")
            .to_owned();
        let lower_layers: Vec<String> = match graph_driver.data.get("LowerDir") {
            Some(lower_dir) => lower_dir.split(':').map(|s| s.to_owned()).collect(),
            None => Vec::new(),
        };
        let layers: Vec<String> = vec![upper_layer].into_iter().chain(lower_layers).collect();

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
        let uploader = ConexUploader::new(self.client.clone(), repo_info.clone(), progress_tx);
        let upload_task = tokio::spawn(async move { uploader.upload(plan, jobs).await });

        let pbar_enabled = false;

        if pbar_enabled {
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
