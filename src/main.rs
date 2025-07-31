#[macro_use]
extern crate custom_derive;
#[macro_use]
extern crate enum_derive;
use msgpack_simple::{Extension, MapElement, MsgPack};
use std::{collections::HashMap, error::Error, ops::Index};

use redis::Commands;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

const LOGGING_ENDPOINT: [&str; 1] = ["info/log"];
const KEY_MISMATCH: &str = "We got a response for request with one key, there must be one key!";
const NO_DATA: &str = "Uh oh, log message contained no data";

#[derive(Debug, PartialEq, Deserialize, Serialize)]
enum LogMsg {
    Str(String),
    Map(HashMap<String, String>),
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogMessage {
    msg_type: String,
    log_type: String,
    log_msg: LogMsg,
    metadata: Option<HashMap<String, String>>,
}

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
        .map(|e| e.map.get("data").ok_or_else(|| str_error(NO_DATA)).into())
        .collect::<Result<Vec<&redis::Value>, Box<dyn Error>>>()?;

    let un_valued: Vec<Vec<u8>> = data
        .iter()
        .map(|e| match e {
            redis::Value::BulkString(x) => Ok(x.iter().cloned().collect()),
            _ => Err(str_error("Log message data not binary-data!")),
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn Error>>>()?;

    let un_mspacked = un_valued
        .iter()
        .map(|e| MsgPack::parse(e))
        .collect::<Result<Vec<MsgPack>, msgpack_simple::ParseError>>()?;

    dbg!(un_mspacked);

    Ok(())
}
