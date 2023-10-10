use bollard::Docker;
use indicatif::{MultiProgress, ProgressDrawTarget, ProgressStyle};
use std::sync::Arc;

use crate::{planner::ConexPlanner, uploader::ConexUploader};

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
        println!("Image: {:?}", image_info);

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

        println!("Layers: {:?}", layers);

        // TODO: move this to shared code with puller later.
        let _m = Arc::new(MultiProgress::with_draw_target(
            ProgressDrawTarget::stdout_with_hz(2),
        ));
        let _sty = ProgressStyle::with_template("{spinner:.green} ({msg}) [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes:>12}/{total_bytes:>12} ({bytes_per_sec:>15}, {eta:>5})")
        .unwrap()
        .progress_chars("#>-");

        // TODO: the next step is re-use the logic from sky-packer to generate a "plan"  for the
        // split files.
        // TODO: upload the oci config as well. ensure we can actually pull it.
        // TODO: add some progress bar niceties: remove the completed pbar, make the as logs
        let mut planner = ConexPlanner::default();
        for layer in layers {
            planner.ingest_dir(&layer);
        }
        // println!("tree {:?}", planner.layer_to_files);
        let plan = planner.generate_plan();

        let uploader =
            ConexUploader::new(source_image.split(':').collect::<Vec<&str>>()[1].to_owned());
        uploader.upload(plan).await;
    }
}
