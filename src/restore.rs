use crate::{
    BUCKET_NAME,
    list::{get_snapshots, list_all_keys},
    make_bar,
};
use aws_sdk_s3::Client;
use std::path::{Path, PathBuf};
use tokio::{io::AsyncWriteExt, task::JoinSet};

pub async fn run(client: &Client, id: Option<String>, needs_confirm: bool) -> anyhow::Result<()> {
    let snapshot_id = match id {
        Some(id) => {
            if id == "latest" {
                let mut snapshots = get_snapshots(client).await?;
                if let Some(latest_id) = snapshots.pop() {
                    latest_id
                } else {
                    anyhow::bail!("no snapshots found");
                }
            } else {
                id
            }
        }
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

    if needs_confirm
        && !inquire::Confirm::new(&format!("Restore snapshot `{snapshot_id}`"))
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
            .ok_or_else(|| anyhow::anyhow!("key {key} does not start with prefix {prefix}"))?;
        let rel_path = safe_relative(relative)?;

        let label = rel_path.display().to_string();
        let dest = dst_root.join(rel_path);

        let client = client.clone();
        let bar = bar.clone();

        joinset.spawn(async move {
            download_file(&client, &key, &dest).await?;
            bar.set_message(label);
            bar.inc(1);
            Ok::<_, anyhow::Error>(())
        });

        while joinset.len() >= MAX_PARALLEL {
            if let Some(res) = joinset.join_next().await {
                res??;
            }
        }
    }

    while let Some(res) = joinset.join_next().await {
        res??;
    }

    bar.finish_with_message("restore complete");
    Ok(())
}

fn safe_relative(p: &str) -> anyhow::Result<PathBuf> {
    let mut out = PathBuf::new();
    for seg in p.replace('\\', "/").split('/') {
        match seg {
            "" | "." => continue,
            ".." => anyhow::bail!("refusing path traversal in key: {p}"),
            s => out.push(s),
        }
    }
    if out.as_os_str().is_empty() {
        anyhow::bail!("key resolves to empty path: {p}");
    }
    Ok(out)
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
