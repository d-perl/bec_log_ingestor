use std::error::Error;

use redis::Commands;
use rmp_serde;
mod log_msg;

const LOGGING_ENDPOINT: [&str; 1] = ["info/log"];
const KEY_MISMATCH: &str = "We got a response for request with one key, there must be one key!";
const NO_DATA: &str = "Uh oh, log message contained no data";

fn str_error(err: &str) -> Box<dyn Error> {
    Box::<dyn Error>::from(err)
}

fn redis_conn() -> Result<redis::Connection, redis::RedisError> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    client.get_connection()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut conn = redis_conn()?;

    let opts = redis::streams::StreamReadOptions::default().count(5);
    let var: redis::streams::StreamReadReply =
        conn.xread_options(&LOGGING_ENDPOINT, &[0], &opts)?;

    let key = var.keys.get(0).ok_or_else(|| str_error(KEY_MISMATCH))?;

    let data: Vec<&redis::Value> = key
        .ids
        .iter()
        .map(|e| e.map.get("data").ok_or_else(|| str_error(NO_DATA)))
        .collect::<Result<Vec<&redis::Value>, Box<dyn Error>>>()?;

    let un_valued: Vec<Vec<u8>> = data
        .iter()
        .map(|e| match e {
            redis::Value::BulkString(x) => Ok(x.iter().cloned().collect()),
            _ => Err(str_error("Log message data not binary-data!")),
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn Error>>>()?;

    let un_mspacked: Vec<log_msg::LogMessagePack> = un_valued
        .iter()
        .map(|e| rmp_serde::from_slice::<log_msg::LogMessagePack>(e))
        .collect::<Result<Vec<log_msg::LogMessagePack>, rmp_serde::decode::Error>>()?;

    dbg!(un_mspacked);

    Ok(())
}
