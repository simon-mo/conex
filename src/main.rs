mod hash;
mod planner;
mod progress;
mod pusher;
mod uploader;

use bollard::Docker;
use clap::{Parser, Subcommand};
use pusher::ContainerPusher;

#[derive(Parser, Debug)]
#[clap(name = "Sky Container", version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Commands,

    #[clap(long, short, help = "Verbose mode")]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Push { name: String },

    Pull { name: Option<String> },

    Snapshotter { name: Option<String> },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let docker = Docker::connect_with_local_defaults().unwrap();
    docker.version().await.expect("Can't talk to dockerd");

    match args.command {
        Commands::Push { name } => {
            let pusher = ContainerPusher::new(docker);
            pusher.push(name).await;
        }
        Commands::Pull { name } => {
            println!("Pulling container: {:?}", name);
        }
        Commands::Snapshotter { name } => {
            println!("Snapshotting container: {:?}", name);
        }
    }
}
