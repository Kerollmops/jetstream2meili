use std::{collections::HashSet, num::NonZeroUsize};

use atrium_api::{app::bsky::feed::post, record::KnownRecord::AppBskyFeedLike};
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
use redis::AsyncCommands as _;
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
    #[arg(long, default_value = "300")]
    payload_size: NonZeroUsize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let Args { meili_url, meili_api_key, meili_index, payload_size } = Args::parse();

    let like_collection: Nsid = "app.bsky.feed.like".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![like_collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let redis = redis::Client::open("redis://127.0.0.1/")?;
    let mut redis = redis.get_multiplexed_async_connection().await?;
    let meili_client = Client::new(&meili_url, meili_api_key.as_ref())?;

    let answer: String = redis.ping().await?;
    anyhow::ensure!(
        answer == "PONG",
        "Server didn't anwsered PONG. Is there a redis/valkey server running?"
    );

    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;
    let bsky_posts = meili_client.index(meili_index);

    eprintln!("Listening for '{:?}' events", like_collection);

    let mut outdated = HashSet::new();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info: _, commit }
                | CommitEvent::Update { info: _, commit } => {
                    if let AppBskyFeedLike(record) = commit.record {
                        // at://did:plc:wa7b35aakoll7hugkrjtf3xf/app.bsky.feed.post/3l3pte3p2e325
                        let (_, post_rkey) = record.data.subject.uri.rsplit_once('/').unwrap();

                        if let Some(BskyPostLikesOnly { rkey: _, likes }) = bsky_posts
                            .get_document(post_rkey)
                            .await
                            .map(Some)
                            .or_else(convert_invalid_request_to_none)?
                        {
                            outdated.insert(post_rkey.to_string());

                            let () = redis.set_nx(post_rkey, likes).await?;
                            let _count: isize = redis.incr(post_rkey, 1).await?;
                        }

                        if outdated.len() == payload_size.get() {
                            let rkeys: Vec<_> = outdated.drain().collect();
                            let values: Vec<usize> = redis.mget(rkeys.clone()).await?;
                            let updated: Vec<_> = rkeys
                                .into_iter()
                                .zip(values)
                                .map(|(rkey, likes)| BskyPostLikesOnly { rkey, likes })
                                .collect();
                            bsky_posts.add_or_update(&updated, None).await?;
                            eprintln!("Sent {payload_size} likes updates.");
                        }
                    }
                }
                CommitEvent::Delete { info: _, commit: _ } => {
                    /* if commit.collection == like_collection {
                        at://did:plc:wa7b35aakoll7hugkrjtf3xf/app.bsky.feed.post/3l3pte3p2e325
                        let (_, post_rkey) = record.data.subject.uri.rsplit_once('/').unwrap();
                        if bsky_posts
                            .get_document::<EmptyBskyPost>(post_rkey)
                            .await
                            .map(Some)
                            .or_else(convert_invalid_request_to_none)?
                            .is_some()
                        {
                            likes_accumulator.decrease(post_rkey.to_string());
                        }
                    } */
                }
            }
        }
    }

    Ok(())
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BskyPostLikesOnly {
    rkey: String,
    #[serde(default)]
    likes: usize,
}

fn convert_invalid_request_to_none<T>(
    err: meilisearch_sdk::errors::Error,
) -> Result<Option<T>, meilisearch_sdk::errors::Error> {
    match &err {
        meilisearch_sdk::errors::Error::Meilisearch(error) => match error.error_code {
            meilisearch_sdk::errors::ErrorCode::DocumentNotFound => Ok(None),
            _ => Ok(None),
        },
        _otherwise => Err(err),
    }
}
