use bollard::Docker;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use std::{collections::HashMap, sync::Arc, time::Instant};
use tracing::info;

use crate::{
    planner::ConexPlanner,
    progress::{UpdateItem, UpdateType},
    uploader::ConexUploader,
};

pub struct ContainerPusher {
    docker: Docker,
}

impl ContainerPusher {
    pub fn new(docker: Docker) -> Self {
        Self { docker }
    }

    pub async fn push(&self, source_image: String) {
        let image_info = self
            .docker
            .inspect_image(&source_image)
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

        info!("Image {} has {} layers", source_image, layers.len());

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
            .collect::<HashMap<String, usize>>();
        info!("Layer sizes: {:?}", layers_to_size);
        let uploader = ConexUploader::new(
            source_image.split(':').collect::<Vec<&str>>()[1].to_owned(),
            progress_tx.clone(),
        );
        let upload_task = tokio::spawn(async move { uploader.upload(plan).await });

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

        upload_task.await.unwrap();
    }
}
