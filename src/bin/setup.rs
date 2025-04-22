use clap::Parser;
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

    let meili_client = reqwest::Client::new();
    let mut request = meili_client.patch(format!("{meili_url}/indexes/{meili_index}/settings"));
    if let Some(api_key) = meili_api_key.as_ref() {
        request = request.bearer_auth(api_key);
    };

    let request = request.json(&json!({
      "displayedAttributes": [
        "*"
      ],
      "searchableAttributes": [
        "text",
        "embed.title",
        "embed.description"
      ],
      "filterableAttributes": [
        {
          "attributePatterns": [
            "rkey",
            "likesIds"
          ],
          "features": {
            "facetSearch": false,
            "filter": {
              "equality": true,
              "comparison": false
            }
          }
        },
        {
          "attributePatterns": [
            "mentions",
            "lang", // langs in fact
            "tags"
          ],
          "features": {
            "facetSearch": true,
            "filter": {
              "equality": true,
              "comparison": false
            }
          }
        },
        {
          "attributePatterns": [
            "createdAtTimestamp",
            "likes"
          ],
          "features": {
            "facetSearch": false,
            "filter": {
              "equality": true,
              "comparison": true
            }
          }
        }
      ],
      "sortableAttributes": [
        "createdAtTimestamp",
        "likes"
      ],
      "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness"
      ],
      "stopWords": [],
      "nonSeparatorTokens": [],
      "separatorTokens": [],
      "dictionary": [],
      "synonyms": {},
      "distinctAttribute": null,
      "proximityPrecision": "byAttribute",
      "typoTolerance": {
        "enabled": true,
        "minWordSizeForTypos": {
          "oneTypo": 5,
          "twoTypos": 9
        },
        "disableOnWords": [],
        "disableOnAttributes": []
      },
      "faceting": {
        "maxValuesPerFacet": 100,
        "sortFacetValuesBy": {
          "*": "alpha"
        }
      },
      "pagination": {
        "maxTotalHits": 1000
      },
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime",
      "executeAfterUpdate": include_str!("apply_likes.rhai"),
    }));

    let response = request.send().await?;
    response.error_for_status().unwrap();

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
