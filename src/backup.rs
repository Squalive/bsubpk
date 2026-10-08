use crate::{BUCKET_NAME, make_bar};
use aws_sdk_s3::{Client, primitives::ByteStream};
use std::path::Path;
use tokio::task::JoinSet;
use walkdir::WalkDir;

pub async fn run(client: &Client) -> anyhow::Result<()> {
    if !inquire::Confirm::new(
        "Upload new Assets/Unlicensed/ and Assets/Unlicensed.meta snapshot to cloudflare storage",
    )
    .with_default(false)
    .with_help_message("This will use your network to upload to cloudflare storage")
    .prompt()?
    {
        return Ok(());
    }

    let unlicensed_root_path = Path::new("Assets/Unlicensed/");

    let snapshot_id = new_snapshot_id();

    upload_unlicensed_dir_recursive(client, &snapshot_id, unlicensed_root_path).await?;
    upload_file(client, &snapshot_id, Path::new("Assets/Unlicensed.meta")).await?;

    tracing::info!("New unlicensed private snapshot is backed up");
    Ok(())
}

async fn upload_file(client: &Client, snapshot_id: &str, file_path: &Path) -> anyhow::Result<()> {
    let body = ByteStream::from_path(&file_path).await?;
    client
        .put_object()
        .bucket(BUCKET_NAME)
        .key(format!("{snapshot_id}/{}", file_path.display()))
        .body(body)
        .send()
        .await?;
    Ok(())
}

async fn upload_unlicensed_dir_recursive(
    client: &Client,
    snapshot_id: &str,
    path: &Path,
) -> anyhow::Result<()> {
    const MAX_PARALLEL: usize = 10;

    let mut entries = Vec::new();
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue; // skip directories and symlinks
        }

        entries.push(entry.path().to_path_buf());
    }

    let bar = make_bar(entries.len() as u64);

    let mut joinset = JoinSet::new();

    for local_path in entries {
        let remote_key = format!("{snapshot_id}/{}", local_path.display());

        let client = client.clone();
        let bar = bar.clone();

        joinset.spawn(async move {
            let body = ByteStream::from_path(&local_path).await?;
            client
                .put_object()
                .bucket(BUCKET_NAME)
                .key(&remote_key)
                .body(body)
                .send()
                .await?;

            // Update inside the task so it reflects real completion
            bar.set_message(remote_key.clone());
            bar.inc(1);
            Ok::<_, anyhow::Error>(())
        });

        // Throttle concurrency
        while joinset.len() >= MAX_PARALLEL {
            joinset.join_next().await;
        }
    }

    while let Some(res) = joinset.join_next().await {
        res??;
    }
    bar.finish_with_message("dir upload complete");
    Ok(())
}

fn new_snapshot_id() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ").to_string()
}
