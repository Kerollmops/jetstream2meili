use clap::Parser;
use meilisearch_sdk::client::*;
use serde_json::json;
use url::Url;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:7700")]
    meili_url: String,
    #[arg(long)]
    meili_api_key: Option<String>,
    #[arg(long, default_value = "bsky-posts")]
    meili_index: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let Args { meili_url, meili_api_key, meili_index } = Args::parse();

    let meili_client = Client::new(&meili_url, meili_api_key.as_ref())?;
    let bsky_posts = meili_client.index(meili_index);
    bsky_posts.set_searchable_attributes(&["text"]).await?;
    bsky_posts
        .set_filterable_attributes(&[
            "rkey",
            "createdAtTimestamp",
            "mentions",
            "tags",
            "lang",
            "likes",
        ])
        .await?;
    bsky_posts.set_sortable_attributes(&["createdAtTimestamp"]).await?;
    bsky_posts.set_proximity_precision("byAttribute".into()).await.unwrap();
    eprintln!("Updated the settings");

    let raw_client = reqwest::Client::new();
    let url = Url::parse(&meili_url)?.join("experimental-features")?;
    let mut request = raw_client.patch(url);
    request = request.json(&json!({ "editDocumentsByFunction": true }));
    if let Some(key) = meili_api_key {
        request = request.bearer_auth(key);
    }
    request.send().await?.error_for_status()?;
    eprintln!("Enabled the editDocumentsByFunction experimental feature");

    Ok(())
}
