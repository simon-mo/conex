mod hash;
mod planner;
mod progress;
mod pusher;
mod reference;
mod uploader;

use bollard::Docker;
use clap::{Parser, Subcommand};
use pusher::ContainerPusher;
use tracing::info;

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
        default_value = "true"
    )]
    progress: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Push { name: String },

    Pull { name: Option<String> },

    Snapshotter { name: Option<String> },
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

    let show_progress = args.progress;

    match args.command {
        Commands::Push { name } => {
            let pusher = ContainerPusher::new(docker);
            pusher.push(name, jobs, show_progress).await;
        }
        Commands::Pull { name } => {
            println!("Pulling container: {:?}", name);
        }
        Commands::Snapshotter { name } => {
            println!("Snapshotting container: {:?}", name);
        }
    }
}
