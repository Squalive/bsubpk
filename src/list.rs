use crate::BUCKET_NAME;
use aws_sdk_s3::Client;
use std::fmt::Write;

pub async fn run(client: &Client) -> anyhow::Result<()> {
    let snapshots = get_snapshots(client).await?;

    let mut msg = format!("{} snapshots: \n", snapshots.len());

    for snapshot in snapshots {
        writeln!(msg, "\t{snapshot}")?;
    }

    tracing::info!("{msg}");

    Ok(())
}

pub async fn get_snapshots(client: &Client) -> anyhow::Result<Vec<String>> {
    let mut snapshots = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(BUCKET_NAME).delimiter("/");

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for prefix in resp.common_prefixes() {
            if let Some(p) = prefix.prefix() {
                let id = p.trim_end_matches('/').to_string();
                if !id.is_empty() {
                    snapshots.push(id);
                }
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(String::from);
        } else {
            break;
        }
    }

    Ok(snapshots)
}

pub async fn list_all_keys(client: &Client, prefix: &str) -> anyhow::Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(BUCKET_NAME).prefix(prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                keys.push(key.to_string());
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(String::from);
        } else {
            break;
        }
    }

    Ok(keys)
}
