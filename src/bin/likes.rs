use std::{collections::HashMap, mem};

use atrium_api::record::KnownRecord::AppBskyFeedLike;
use clap::Parser;
use iter_identify_first_last::IteratorIdentifyFirstLastExt;
use jetstream_oxide::{
    events::{commit::CommitEvent, JetstreamEvent::Commit},
    exports::Nsid,
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
};
use serde::Serialize;
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

    let collection: Nsid = "app.bsky.feed.like".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let client = reqwest::Client::new();
    let url = Url::parse(&meili_url)?
        .join("indexes")?
        .join(&meili_index)?
        .join("documents")?
        .join("edit")?;
    let mut request = client.post(url);
    if let Some(key) = meili_api_key {
        request = request.bearer_auth(key);
    }

    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    eprintln!("Listening for '{:?}' events", collection);

    let mut cache = HashMap::new();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info: _, commit }
                | CommitEvent::Update { info: _, commit } => {
                    if let AppBskyFeedLike(_) = commit.record {
                        *cache.entry(commit.info.rkey).or_insert(0) += 1;
                        if cache.len() == 20 {
                            let (increases, decreases) =
                                EditDocumentsByFunction::new(mem::take(&mut cache));
                            if let Some(increases) = increases {
                                let mut request = request.try_clone().unwrap();
                                request = request.json(&increases);
                                request.send().await?;
                            }
                            if let Some(decreases) = decreases {
                                let mut request = request.try_clone().unwrap();
                                request = request.json(&decreases);
                                request.send().await?;
                            }
                        }
                    }
                }
                CommitEvent::Delete { info: _, commit } => {
                    *cache.entry(commit.rkey).or_insert(0) -= 1;
                    if cache.len() == 20 {
                        let (increases, decreases) =
                            EditDocumentsByFunction::new(mem::take(&mut cache));
                        if let Some(increases) = increases {
                            let mut request = request.try_clone().unwrap();
                            request = request.json(&increases);
                            request.send().await?;
                        }
                        if let Some(decreases) = decreases {
                            let mut request = request.try_clone().unwrap();
                            request = request.json(&decreases);
                            request.send().await?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct EditDocumentsByFunction {
    context: EditContext,
    filter: String,
    // Either increase_likes.rhai or decrease_likes.rhai
    function: &'static str,
}

#[derive(Debug, Serialize)]
struct EditContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    increases: Option<HashMap<String, usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decreases: Option<HashMap<String, usize>>,
}

impl EditDocumentsByFunction {
    fn new(
        likes: HashMap<String, isize>,
    ) -> (Option<EditDocumentsByFunction>, Option<EditDocumentsByFunction>) {
        let mut increases = HashMap::new();
        let mut decreases = HashMap::new();

        for (key, change) in likes {
            if change > 0 {
                increases.insert(key, change.unsigned_abs());
            } else if change < 0 {
                decreases.insert(key, change.unsigned_abs());
            }
        }

        let mut increase_payload = None;
        if !increases.is_empty() {
            let mut filter = format!("rkey IN [");
            for (is_last, key) in increases.keys().identify_last() {
                filter.push_str(key);
                if !is_last {
                    filter.push_str(",");
                }
            }
            filter.push_str("]");

            increase_payload = Some(EditDocumentsByFunction {
                context: EditContext { increases: Some(increases), decreases: None },
                filter,
                function: include_str!("increase_likes.rhai"),
            });
        }

        let mut decrease_payload = None;
        if !decreases.is_empty() {
            let mut filter = format!("rkey IN [");
            for (is_last, key) in decreases.keys().identify_last() {
                filter.push_str(key);
                if !is_last {
                    filter.push_str(",");
                }
            }
            filter.push_str("]");

            decrease_payload = Some(EditDocumentsByFunction {
                context: EditContext { increases: None, decreases: Some(decreases) },
                filter,
                function: include_str!("decrease_likes.rhai"),
            });
        }

        (increase_payload, decrease_payload)
    }
}
