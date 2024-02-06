mod data_store;
mod hash;
mod planner;
mod progress;
mod puller;
mod pusher;
mod reference;
mod repo_info;
mod snapshotter;
mod uploader;

use bollard::Docker;
use clap::{Parser, Subcommand};
use puller::ContainerPuller;
use pusher::ContainerPusher;
use snapshotter::serve_snapshotter;
use tracing::info;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(name = "Sky Container", version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Commands,

    // #[clap(long, short, help = "Verbose mode")]
    // verbose: bool,
    #[clap(
        long,
        short,
        help = "Number of jobs to run in parallel, default to number of cores"
    )]
    jobs: Option<usize>,

    #[clap(
        long,
        short,
        help = "Show progress bar, default to true",
        default_value = "false"
    )]
    no_progress: bool,

    #[clap(
        long,
        short,
        help = "Set the location on disk to save the OCI image at, default to the current working directory",
    )]
    local_image_path: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Push { name: String , threshold: usize},

    Pull { name: String },

    Snapshotter {},

    Clean {},
}

const BLOB_LOCATION: &str = "/tmp/conex-blob-store";
const MOUNT_LOCATION: &str = "/tmp/conex-mount";
const SOCKET_LOCATION: &str = "/tmp/conex-snapshotter.sock";
const METADATA_DB_PATH: &str = "/tmp/conex-metadata.db";

fn ensure_conex_dirs() {
    std::fs::create_dir_all(BLOB_LOCATION).unwrap();
    std::fs::create_dir_all(MOUNT_LOCATION).unwrap();
    std::fs::create_dir_all(METADATA_DB_PATH).unwrap();
}

#[tokio::main]
async fn main() {
    // Exit the process upon panic, this is used for debugging purpose.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let docker = Docker::connect_with_local_defaults().unwrap();
    docker.version().await.expect("Can't talk to dockerd");

    let jobs = args.jobs.unwrap_or_else(num_cpus::get);
    info!("Running with {} threads in parallel", jobs);

    let show_progress = !args.no_progress;

    // TODO: validate the passed-in path
    let local_image_path = args.local_image_path;

    match args.command {
        Commands::Push { name, threshold} => {
            println!("Pushing container: {:?}", name);
            let pusher = ContainerPusher::new(docker);
            pusher.push(name, jobs, show_progress, local_image_path, threshold).await;
        }
        Commands::Pull { name } => {
            println!("Pulling container: {:?}", name);
            ensure_conex_dirs();
            let puller = ContainerPuller::new(BLOB_LOCATION.into());
            puller.pull(name, jobs, show_progress, None).await;
        }
        Commands::Snapshotter {} => {
            println!("Running snapshotter");
            ensure_conex_dirs();
            serve_snapshotter(
                BLOB_LOCATION.into(),
                MOUNT_LOCATION.into(),
                SOCKET_LOCATION.into(),
                METADATA_DB_PATH.into(),
            )
            .await;
        }
        Commands::Clean {} => {
            println!("Cleaning up");
            let _ = std::fs::remove_dir_all(BLOB_LOCATION);
            let _ = std::fs::remove_dir_all(MOUNT_LOCATION);
            let _ = std::fs::remove_dir_all(METADATA_DB_PATH);
        }
    }
}
