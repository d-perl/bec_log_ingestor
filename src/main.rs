use elasticsearch::{
    BulkParts, Elasticsearch,
    http::{request::JsonBody, transport::Transport},
};
use redis::Commands;
use rmp_serde;
use serde_json::to_string;

use std::{error::Error, iter::once};

use crate::log_msg::LogRecord;
mod log_msg;

const LOGGING_ENDPOINT: [&str; 1] = ["info/log"];
const KEY_MISMATCH: &str = "We got a response for request with one key, there must be one key!";
const NO_DATA: &str = "Uh oh, log message contained no data";

fn stream_read_opts() -> redis::streams::StreamReadOptions {
    redis::streams::StreamReadOptions::default().count(5)
}

fn str_error(err: &str) -> Box<dyn Error> {
    Box::<dyn Error>::from(err)
}

fn redis_conn() -> Result<redis::Connection, redis::RedisError> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    client.get_connection()
}

fn elastic_client(api_key: &str) -> Result<Elasticsearch, elasticsearch::Error> {
    let url = elasticsearch::http::Url::parse("http://localhost:9200")?;
    let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
    let credentials = elasticsearch::auth::Credentials::EncodedApiKey(api_key.into());
    let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool)
        .auth(credentials)
        .build()?;
    Ok(Elasticsearch::new(transport))
}

fn read_logs(redis_conn: &mut redis::Connection) -> Result<Vec<redis::Value>, Box<dyn Error>> {
    let raw_reply: redis::streams::StreamReadReply =
        redis_conn.xread_options(&LOGGING_ENDPOINT, &[0], &stream_read_opts())?;

    let log_key = raw_reply
        .keys
        .get(0)
        .ok_or_else(|| str_error(KEY_MISMATCH))?;

    log_key
        .ids
        .iter()
        .map(|e| e.map.get("data").ok_or_else(|| str_error(NO_DATA)).cloned())
        .collect::<Result<Vec<redis::Value>, Box<dyn Error>>>()
}

fn process_data(values: Vec<redis::Value>) -> Result<Vec<log_msg::LogMessagePack>, Box<dyn Error>> {
    let un_valued: Vec<Vec<u8>> = values
        .iter()
        .map(|e| match e {
            redis::Value::BulkString(x) => Ok(x.iter().cloned().collect()),
            _ => Err(str_error("Log message data not binary-data!")),
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn Error>>>()?;

    Ok(un_valued
        .iter()
        .map(|e| rmp_serde::from_slice::<log_msg::LogMessagePack>(e))
        .collect::<Result<Vec<log_msg::LogMessagePack>, rmp_serde::decode::Error>>()?)
}

fn extract_records(messages: Vec<log_msg::LogMessagePack>) -> Vec<log_msg::LogRecord> {
    messages
        .iter()
        .map(|e| e.bec_codec.data.log_msg.record.clone())
        .collect()
}

fn make_index_action_body(records: &Vec<LogRecord>) -> Result<String, serde_json::Error> {
    let action = serde_json::json!({ "index": { "_index": "test-index" } });
    let body_vec: Vec<serde_json::Value> = records
        .iter()
        .flat_map(|e| once(Ok(action.clone())).chain(once(serde_json::to_value(&e))))
        .collect::<Result<Vec<serde_json::Value>, serde_json::Error>>()?;
    Ok(body_vec
        .iter()
        .map(|e| to_string(e))
        .collect::<Result<Vec<String>, serde_json::Error>>()?
        .join("\n"))
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut redis_conn = redis_conn()?;
    let elastic_client =
        elastic_client("ZUl6dGVaZ0JEbkl1Njd5RGl2MVc6UUluVTVWZ0FqNDZ2VlQzYkRwRHJzQQ==")?;

    // get logs data from redis and convert to record structs
    let data = read_logs(&mut redis_conn)?;
    let unpacked = process_data(data)?;
    let records = extract_records(unpacked);

    dbg!(&records);

    let body = make_docs_values(&records)?;
    // Send bulk request
    let response = elastic_client
        .bulk(BulkParts::Index("test-index"))
        .body(body)
        .send()
        .await?;

    // Optional: check response
    let response_body = response.json::<serde_json::Value>().await?;
    println!("{:#}", response_body);

    Ok(())
}
