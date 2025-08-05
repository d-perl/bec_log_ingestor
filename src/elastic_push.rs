use elasticsearch::{Elasticsearch, http::request::JsonBody};
use rmp_serde;
use serde_json::to_string;
use tokio::sync::mpsc;

use std::{error::Error, iter::once};

use crate::redis_logs::{LogRecord, producer_loop};

fn elastic_client(api_key: &str) -> Result<Elasticsearch, elasticsearch::Error> {
    let url = elasticsearch::http::Url::parse("http://localhost:9200")?;
    let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
    let credentials = elasticsearch::auth::Credentials::EncodedApiKey(api_key.into());
    let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool)
        .auth(credentials)
        .build()?;
    Ok(Elasticsearch::new(transport))
}

fn make_docs_values(
    records: &Vec<LogRecord>,
) -> Result<Vec<JsonBody<serde_json::Value>>, serde_json::Error> {
    let action = serde_json::json!({ "create": {} });

    let values = records
        .iter()
        .map(|e| serde_json::to_value(e))
        .collect::<Result<Vec<serde_json::Value>, serde_json::Error>>()?;

    Ok(values
        .iter()
        .flat_map(|e| {
            once(JsonBody::from(action.clone())).chain(once(JsonBody::from(e.to_owned())))
        })
        .collect())
}

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     let elastic_client =
//         elastic_client("ZUl6dGVaZ0JEbkl1Njd5RGl2MVc6UUluVTVWZ0FqNDZ2VlQzYkRwRHJzQQ==")?;

//     let mut redis_conn = redis_conn()?;
//     // get logs data from redis and convert to record structs
//     let data = read_logs(&mut redis_conn)?;
//     let unpacked = process_data(data)?;
//     let records = extract_records(unpacked);

//     dbg!(&records);

//     let body = make_docs_values(&records)?;
//     // Send bulk request
//     let response = elastic_client
//         .bulk(BulkParts::Index("test-index"))
//         .body(body)
//         .send()
//         .await?;

//     // Optional: check response
//     let response_body = response.json::<serde_json::Value>().await?;
//     println!("{:#}", response_body);

//     Ok(())
// }

pub async fn consumer_loop(mut rx: mpsc::UnboundedReceiver<LogRecord>) {
    let elastic_client =
        elastic_client("ZUl6dGVaZ0JEbkl1Njd5RGl2MVc6UUluVTVWZ0FqNDZ2VlQzYkRwRHJzQQ==")
            .expect("Failed to connect to Elastic!");

    while let Some(value) = rx.recv().await {
        // let body = make_docs_values(&records)?;
        // // Send bulk request
        // let response = elastic_client
        //     .bulk(BulkParts::Index("test-index"))
        //     .body(body)
        //     .send()
        //     .await?;
        println!("{:?}", value);
    }
    println!("Producer dropped, consumer exiting");
}
