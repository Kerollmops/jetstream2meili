use std::{
    collections::{BTreeMap, HashSet},
    mem,
    num::NonZeroUsize,
};

use atrium_api::{
    app::bsky::feed::post,
    record::KnownRecord::{AppBskyFeedLike, AppBskyFeedPost},
};
use clap::Parser;
use itertools::Itertools;
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
    #[arg(long, default_value = "100")]
    payload_size: NonZeroUsize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let Args { meili_url, meili_api_key, meili_index, payload_size } = Args::parse();

    let post_collection: Nsid = "app.bsky.feed.post".parse().unwrap();
    let like_collection: Nsid = "app.bsky.feed.like".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![post_collection.clone(), like_collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let meili_client = Client::new(&meili_url, meili_api_key.as_ref())?;
    let raw_client = reqwest::Client::new();
    let url = Url::parse(&meili_url)?
        .join("indexes")?
        .join(&meili_index)?
        .join("documents")?
        .join("edit")?;
    let mut editions_request = raw_client.post(url);
    if let Some(key) = meili_api_key {
        editions_request = editions_request.bearer_auth(key);
    }

    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;
    let bsky_posts = meili_client.index(meili_index);

    eprintln!("Listening for '{:?}' and '{:?}' events", post_collection, like_collection);

    let mut cache = Vec::new();
    let mut cache_sent: usize = 0;
    let mut likes_accumulator = LikesAccumulator::default();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info, commit } | CommitEvent::Update { info, commit } => {
                    if let AppBskyFeedPost(record) = commit.record {
                        cache.push(BskyPost::new(info, commit.info, record.data));

                        if cache.len() == payload_size.get() {
                            bsky_posts.add_or_update(&cache, Some("rkey")).await?;
                            cache_sent += 1;
                            cache.clear();
                        }

                        if cache_sent == 500 {
                            for editions in mem::take(&mut likes_accumulator).into_editions() {
                                let mut request = editions_request.try_clone().unwrap();
                                request = request.json(&editions);
                                request.send().await?;
                            }
                        }
                    } else if let AppBskyFeedLike(record) = commit.record {
                        // at://did:plc:wa7b35aakoll7hugkrjtf3xf/app.bsky.feed.post/3l3pte3p2e325
                        let (_, post_rkey) = record.data.subject.uri.rsplit_once('/').unwrap();

                        if bsky_posts
                            .get_document::<EmptyBskyPost>(post_rkey)
                            .await
                            .map(Some)
                            .or_else(convert_invalid_request_to_none)?
                            .is_some()
                        {
                            likes_accumulator.increase(post_rkey.to_string());
                        }
                    }
                }
                CommitEvent::Delete { info: _, commit } => {
                    if commit.collection == post_collection {
                        let rkey = commit.rkey.to_string();
                        bsky_posts.delete_document(rkey).await?;
                    } /* else if commit.collection == like_collection {
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

#[derive(Serialize, Debug)]
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
        }
    }
}

#[derive(Deserialize, Debug)]
struct EmptyBskyPost {}

#[derive(Debug, Default)]
struct LikesAccumulator {
    likes: BTreeMap<isize, HashSet<String>>,
}

impl LikesAccumulator {
    fn increase(&mut self, id: String) {
        self.change(id, |like| like + 1);
    }

    // fn decrease(&mut self, id: String) {
    //     self.change(id, |like| like - 1);
    // }

    fn change(&mut self, id: String, f: impl Fn(isize) -> isize) {
        match self.likes.iter_mut().find(|(_, ids)| ids.contains(&id)) {
            Some((&like, ids)) => {
                ids.remove(&id);
                if f(like) != 0 {
                    self.likes.entry(f(like)).or_default().insert(id);
                }
            }
            None => {
                self.likes.entry(1).or_default().insert(id);
            }
        }
    }

    fn into_editions(self) -> Vec<EditDocumentsByFunction> {
        self.likes
            .into_iter()
            .flat_map(|(diff, ids)| EditDocumentsByFunction::new(ids, diff))
            .collect()
    }
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

#[derive(Debug, Serialize)]
struct EditDocumentsByFunction {
    context: EditContext,
    filter: String,
    function: &'static str,
}

#[derive(Debug, Serialize)]
struct EditContext {
    diff: isize,
}

impl EditDocumentsByFunction {
    fn new(ids: impl IntoIterator<Item = String>, diff: isize) -> Vec<EditDocumentsByFunction> {
        ids.into_iter()
            .chunks(300)
            .into_iter()
            .map(|ids| {
                let mut filter = "rkey IN [".to_string();
                let mut ids = ids.into_iter().peekable();
                while let Some(id) = ids.next() {
                    filter.push_str(&id);
                    if ids.peek().is_some() {
                        filter.push(',');
                    }
                }
                filter.push(']');

                EditDocumentsByFunction {
                    context: EditContext { diff },
                    filter,
                    function: "doc.likes = (doc.likes ?? 0) + context.diff;",
                }
            })
            .collect()
    }
}
