use crate::{
    BUCKET_NAME,
    list::{get_snapshots, list_all_keys},
    make_bar,
};
use aws_sdk_s3::Client;
use std::path::Path;
use tokio::{io::AsyncWriteExt, task::JoinSet};

pub async fn run(client: &Client, id: Option<String>) -> anyhow::Result<()> {
    let snapshot_id = match id {
        Some(id) => id,
        None => {
            let snapshots = get_snapshots(client).await?;
            if snapshots.is_empty() {
                anyhow::bail!("no snapshots found");
            }

            let mut choices = snapshots;
            choices.reverse();
            inquire::Select::new("Which snapshot?", choices).prompt()?
        }
    };

    if !inquire::Confirm::new(&format!("Restore snapshot `{snapshot_id}`"))
        .with_default(false)
        .with_help_message("This will use your network to download from the cloudflare storage")
        .prompt()?
    {
        return Ok(());
    }

    tracing::info!(snapshot = %snapshot_id, "starting restore");

    restore_snapshot(client, &snapshot_id, Path::new(".")).await?;

    tracing::info!(snapshot = %snapshot_id, "restore complete");

    Ok(())
}

async fn restore_snapshot(
    client: &Client,
    snapshot_id: &str,
    dst_root: &Path,
) -> anyhow::Result<()> {
    const MAX_PARALLEL: usize = 10;

    let prefix = format!("{}/", snapshot_id);
    let keys = list_all_keys(client, &prefix).await?;

    if keys.is_empty() {
        anyhow::bail!("snapshot {snapshot_id} has no objects");
    }

    let bar = make_bar(keys.len() as u64);
    let mut joinset = JoinSet::new();

    for key in keys {
        // Strip the snapshot prefix so files land at their original paths.
        let relative = key
            .strip_prefix(&prefix)
            .ok_or_else(|| anyhow::anyhow!("key {key} does not start with prefix {prefix}"))?
            .to_string();

        let dest = dst_root.join(Path::new(&relative));
        let client = client.clone();
        let bar = bar.clone();

        joinset.spawn(async move {
            download_file(&client, &key, &dest).await?;
            bar.set_message(relative.clone());
            bar.inc(1);
            Ok::<_, anyhow::Error>(())
        });

        while joinset.len() >= MAX_PARALLEL {
            joinset.join_next().await;
        }
    }

    while let Some(res) = joinset.join_next().await {
        res??;
    }

    bar.finish_with_message("restore complete");
    Ok(())
}

/// Stream a single object from R2 to a local path.
async fn download_file(client: &Client, key: &str, dest: &Path) -> anyhow::Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut resp = client
        .get_object()
        .bucket(BUCKET_NAME)
        .key(key)
        .send()
        .await?;

    let mut file = tokio::fs::File::create(dest).await?;

    // Stream chunk-by-chunk so large files don't sit in memory.
    while let Some(chunk) = resp.body.next().await {
        let bytes = chunk?;
        file.write_all(&bytes).await?;
    }

    file.flush().await?;
    Ok(())
}
