mod backup;
mod list;
mod restore;

use aws_config::BehaviorVersion;
use aws_sdk_s3::{self as s3};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const BUCKET_NAME: &str = "bs-private-backup";

#[derive(clap::Parser)]
#[command(
    name = "bs_unlicensed_backup",
    version,
    about = "Backup private banana shooter unlicensed assets to cloudflare"
)]
struct Args {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Clone, clap::Subcommand)]
enum Commands {
    /// Backup a new snapshot to the cloud
    Backup,
    /// List available snapshots from cloud
    List,
    /// Restore a snapshot from the cloud
    Restore {
        /// Snapshot ID; if omitted, you'll be prompted to pick one
        id: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let client = build_client().await;

    match args.commands {
        Commands::Backup => {
            if let Err(e) = backup::run(&client).await {
                tracing::error!(error = %e, "Failed to backup unlicensed private snapshot");
            }
        }
        Commands::List => {
            if let Err(e) = list::run(&client).await {
                tracing::error!(error = %e, "Failed to list snapshots")
            }
        }
        Commands::Restore { id } => {
            if let Err(e) = restore::run(&client, id).await {
                tracing::error!(error = %e, "Failed to restore snapshot")
            }
        }
    }
}

async fn build_client() -> s3::Client {
    let account_id = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID must be set");
    let access_key_id = std::env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID must be set");
    let secret_access_key =
        std::env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY must be set");

    // Configure the R2 endpoint
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!("https://{}.r2.cloudflarestorage.com", account_id))
        .credentials_provider(s3::config::Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "R2",
        ))
        .region("auto")
        .load()
        .await;

    s3::Client::new(&config)
}

pub fn make_bar(total: u64) -> ProgressBar {
    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    bar
}
