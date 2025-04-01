use std::{collections::HashMap, num::NonZeroUsize};

use atrium_api::{app::bsky::feed::post, record::KnownRecord::AppBskyFeedPost};
use clap::Parser;
use jetstream_oxide::{
    events::{
        commit::{CommitEvent, CommitInfo},
        EventInfo,
        JetstreamEvent::Commit,
    },
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
    #[arg(long, default_value = "500")]
    payload_size: NonZeroUsize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let Args { meili_url, meili_api_key, meili_index, payload_size } = Args::parse();

    let post_collection: Nsid = "app.bsky.feed.post".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![post_collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let meili_client = Client::new(&meili_url, meili_api_key.as_ref())?;
    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;
    let bsky_posts = meili_client.index(meili_index);

    eprintln!("Listening for '{:?}' events", post_collection);

    let mut cache = HashMap::new();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info, commit } | CommitEvent::Update { info, commit } => {
                    if let AppBskyFeedPost(record) = commit.record {
                        let post = BskyPost::new(info, commit.info, record.data);
                        cache.insert(post.rkey.clone(), Some(post));

                        if cache.len() == payload_size.get() {
                            let (posts, deletions) =
                                partition_additions_and_deletions(cache.drain());
                            if !posts.is_empty() {
                                bsky_posts.add_or_update(&posts, Some("rkey")).await?;
                            }
                            if !deletions.is_empty() {
                                bsky_posts.delete_documents(&deletions).await?;
                            }
                        }
                    }
                }
                CommitEvent::Delete { info: _, commit } => {
                    if commit.collection == post_collection {
                        let rkey = commit.rkey.to_string();
                        cache.insert(rkey, None);

                        if cache.len() == payload_size.get() {
                            let (posts, deletions) =
                                partition_additions_and_deletions(cache.drain());
                            if !posts.is_empty() {
                                bsky_posts.add_or_update(&posts, Some("rkey")).await?;
                            }
                            if !deletions.is_empty() {
                                bsky_posts.delete_documents(&deletions).await?;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn partition_additions_and_deletions(
    ops: impl Iterator<Item = (String, Option<BskyPost>)>,
) -> (Vec<BskyPost>, Vec<String>) {
    let mut posts = Vec::new();
    let mut deletions = Vec::new();

    for (rkey, post) in ops {
        match post {
            Some(post) => posts.push(post),
            None => deletions.push(rkey),
        }
    }

    (posts, deletions)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BskyPost {
    rkey: String,
    text: String,
    mentions: Vec<String>,
    tags: Vec<String>,
    langs: Vec<String>,
    created_at: String,
    created_at_timestamp: u64,
    // https://bsky.app/profile/did:plc:olsofbpplu7b2hd7amjxrei5/post/3ll2v3rx4ss23
    link: Url,
    #[serde(skip_serializing_if = "Option::is_some")]
    likes: Option<usize>,
}

impl BskyPost {
    pub fn new(
        event_info: EventInfo,
        commit_info: CommitInfo,
        record_data: post::RecordData,
    ) -> Self {
        let link = format!(
            "https://bsky.app/profile/{did}/post/{rkey}",
            did = event_info.did.as_str(),
            rkey = commit_info.rkey,
        );

        BskyPost {
            rkey: commit_info.rkey.to_string(),
            langs: record_data.langs.map_or_else(Vec::new, |langs| {
                langs.into_iter().map(|lang| lang.as_ref().as_str().to_string()).collect()
            }),
            text: record_data.text,
            mentions: record_data.entities.map_or_else(Vec::new, |entities| {
                entities
                    .into_iter()
                    .flat_map(|e| (e.data.r#type == "mention").then_some(e.data.value))
                    .collect()
            }),
            tags: record_data.tags.unwrap_or_default(),
            created_at: record_data.created_at.as_ref().to_string(),
            created_at_timestamp: event_info.time_us,
            link: link.parse().unwrap(),
            likes: None,
        }
    }
}
