use atrium_api::record::KnownRecord::AppBskyFeedPost;
use jetstream_oxide::{
    events::{commit::CommitEvent, JetstreamEvent::Commit},
    exports::Nsid,
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
};
use meilisearch_sdk::client::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BskyPost {
    cid: String,
    text: String,
    mentions: Vec<String>,
    tags: Vec<String>,
    langs: Vec<String>,
    created_at_timestamp: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let collection: Nsid = "app.bsky.feed.post".parse().unwrap();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![collection.clone()],
        wanted_dids: Vec::new(),
        compression: JetstreamCompression::Zstd,
        cursor: None,
    };

    let meili_client = Client::new("http://localhost:7700", Option::<String>::None)?;
    let jetstream = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    let bsky_posts = meili_client.index("bsky-posts");
    bsky_posts.set_searchable_attributes(&["text"]).await?;
    bsky_posts
        .set_filterable_attributes(&["createdAtTimestamp", "mentions", "tags", "lang"])
        .await?;
    bsky_posts.set_sortable_attributes(&["createdAtTimestamp"]).await?;
    bsky_posts.set_proximity_precision("byAttribute".into()).await.unwrap();

    eprintln!("Listening for '{:?}' events", collection);

    let mut posts_sent = 0;
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info: _, commit } => {
                    if let AppBskyFeedPost(record) = commit.record {
                        let record = record.data;
                        bsky_posts
                            .add_documents(
                                &[BskyPost {
                                    cid: commit.cid.as_ref().to_string(),
                                    langs: record.langs.map_or_else(Vec::new, |langs| {
                                        langs
                                            .into_iter()
                                            .map(|lang| lang.as_ref().as_str().to_string())
                                            .collect()
                                    }),
                                    text: record.text,
                                    mentions: Vec::new(),
                                    tags: Vec::new(),
                                    created_at_timestamp: record.created_at.as_ref().timestamp(),
                                }],
                                Some("cid"),
                            )
                            .await?;

                        posts_sent += 1;
                        if posts_sent % 1000 == 0 {
                            eprintln!("{posts_sent} posts sent");
                        }
                    }
                }
                // CommitEvent::Delete { info: _, commit } => {
                //     // println!("A post has been deleted. ({})", commit.rkey);
                //     if let AppBskyFeedPost(record) = commit.record {
                //         // bsky_posts.delete_document(record)
                //     }
                // }
                _ => (),
            }
        }
    }

    Ok(())
}
