use elasticsearch::{Elasticsearch, http::request::JsonBody};
use redis::Commands;
use rmp_serde;
use serde_json::to_string;
use tokio::sync::mpsc;

use std::{error::Error, iter::once};

mod redis_logs;
use crate::redis_logs::{LogRecord, producer_loop};

mod elastic_push;
use crate::elastic_push::consumer_loop;

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

#[tokio::main]
async fn main() {
    // Create a channel with a buffer size of 100
    let (tx, rx) = mpsc::unbounded_channel::<LogRecord>();

    // Spawn a task that pushes items to the queue
    let producer = tokio::spawn(producer_loop(tx));

    // Spawn a task that consumes items from the queue
    let consumer = tokio::spawn(consumer_loop(rx));

    // Wait for both tasks (this example never ends, so Ctrl+C to stop)
    let _ = tokio::join!(producer, consumer);
}
