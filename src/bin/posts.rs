use atrium_api::record::KnownRecord::AppBskyFeedPost;
use clap::Parser;
use jetstream_oxide::{
    events::{commit::CommitEvent, JetstreamEvent::Commit},
    exports::Nsid,
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
};
use meilisearch_sdk::client::*;
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BskyPost {
    rkey: String,
    text: String,
    mentions: Vec<String>,
    tags: Vec<String>,
    langs: Vec<String>,
    created_at_timestamp: i64,
    // https://bsky.app/profile/did:plc:olsofbpplu7b2hd7amjxrei5/post/3ll2v3rx4ss23
    link: Url,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let Args { meili_url, meili_api_key, meili_index } = Args::parse();

    let collection: Nsid = "app.bsky.feed.post".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let meili_client = Client::new(meili_url, meili_api_key)?;
    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    let bsky_posts = meili_client.index(meili_index);
    bsky_posts.set_searchable_attributes(&["text"]).await?;
    bsky_posts
        .set_filterable_attributes(&["createdAtTimestamp", "mentions", "tags", "lang"])
        .await?;
    bsky_posts.set_sortable_attributes(&["createdAtTimestamp"]).await?;
    bsky_posts.set_proximity_precision("byAttribute".into()).await.unwrap();

    eprintln!("Listening for '{:?}' events", collection);

    let mut cache = Vec::new();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info, commit } | CommitEvent::Update { info, commit } => {
                    if let AppBskyFeedPost(record) = commit.record {
                        let record = record.data;
                        let link = format!(
                            "https://bsky.app/profile/{did}/post/{rkey}",
                            did = info.did.as_str(),
                            rkey = commit.info.rkey,
                        );
                        let post = BskyPost {
                            rkey: commit.info.rkey.to_string(),
                            langs: record.langs.map_or_else(Vec::new, |langs| {
                                langs
                                    .into_iter()
                                    .map(|lang| lang.as_ref().as_str().to_string())
                                    .collect()
                            }),
                            text: record.text,
                            mentions: record.entities.map_or_else(Vec::new, |entities| {
                                entities
                                    .into_iter()
                                    .flat_map(|entity| {
                                        if entity.data.r#type == "mention" {
                                            Some(entity.data.value)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect()
                            }),
                            tags: record.tags.unwrap_or_default(),
                            created_at_timestamp: record.created_at.as_ref().timestamp(),
                            link: Url::parse(&link).unwrap(),
                        };

                        cache.push(post);

                        if cache.len() == 20 {
                            bsky_posts.add_or_update(&cache, Some("rkey")).await?;
                            cache.clear();
                        }
                    }
                }
                CommitEvent::Delete { info: _, commit } => {
                    let rkey = commit.rkey.to_string();
                    bsky_posts.delete_document(rkey).await.unwrap();
                }
            }
        }
    }

    Ok(())
}
