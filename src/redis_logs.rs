use redis::Commands;
use rmp_serde;

use std::error::Error;

use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Elapsed {
    pub repr: String,
    pub seconds: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct File {
    pub name: String,
    pub path: String,
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct LogLevel {
    pub icon: String,
    pub name: String,
    pub no: usize,
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct NameId {
    pub name: String,
    pub id: usize,
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Timestamp {
    pub repr: String,
    pub timestamp: f64,
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct LogRecord {
    pub elapsed: Elapsed,
    pub exception: Option<serde_json::Value>,
    pub extra: serde_json::Value,
    pub file: File,
    pub function: String,
    pub level: LogLevel,
    pub line: usize,
    pub message: String,
    pub module: String,
    pub name: String,
    pub process: NameId,
    pub thread: NameId,
    pub time: Timestamp,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogMsg {
    record: LogRecord,
    service_name: String,
    text: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogMessage {
    log_type: String,
    log_msg: LogMsg,
    metadata: serde_json::Value,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogMessagePackInternal {
    encoder_name: String,
    type_name: String,
    data: LogMessage,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogMessagePack {
    #[serde(rename = "__bec_codec__")]
    bec_codec: LogMessagePackInternal,
}

const LOGGING_ENDPOINT: [&str; 1] = ["info/log"];
const KEY_MISMATCH: &str = "We got a response for request with one key, there must be one key!";
const NO_DATA: &str = "Uh oh, log message contained no data";

fn error_log_item() -> LogMessagePack {
    LogMessagePack {
        bec_codec: LogMessagePackInternal {
            encoder_name: "".into(),
            type_name: "".into(),
            data: LogMessage {
                log_type: "".into(),
                log_msg: LogMsg {
                    record: LogRecord {
                        elapsed: Elapsed {
                            repr: "".into(),
                            seconds: 0.0,
                        },
                        exception: None,
                        extra: {}.into(),
                        file: File {
                            name: "".into(),
                            path: "".into(),
                        },
                        function: "".into(),
                        level: LogLevel {
                            icon: "".into(),
                            name: "ERROR".into(),
                            no: 100,
                        },
                        line: 0,
                        message: "Error processing log messages from Redis!".into(),
                        module: "".into(),
                        name: "".into(),
                        process: NameId {
                            name: "".into(),
                            id: 0,
                        },
                        thread: NameId {
                            name: "".into(),
                            id: 0,
                        },
                        time: Timestamp {
                            repr: "".into(),
                            timestamp: 0.0,
                        },
                    },
                    service_name: "".into(),
                    text: "".into(),
                },
                metadata: {}.into(),
            },
        },
    }
}

fn str_error(err: &str) -> Box<dyn Error> {
    Box::<dyn Error>::from(err)
}

fn redis_conn() -> Result<redis::Connection, redis::RedisError> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    client.get_connection()
}

fn stream_read_opts() -> redis::streams::StreamReadOptions {
    redis::streams::StreamReadOptions::default()
        .count(5)
        .block(1000)
        .group("log-ingestor", "log-ingestor")
}

fn read_logs(
    redis_conn: &mut redis::Connection,
    last_id: &String,
) -> Result<(Option<String>, Vec<redis::Value>), Box<dyn Error>> {
    let raw_reply: redis::streams::StreamReadReply =
        redis_conn.xread_options(&LOGGING_ENDPOINT, &[last_id], &stream_read_opts())?;

    let log_key = raw_reply
        .keys
        .get(0)
        .ok_or_else(|| str_error(KEY_MISMATCH))?;

    let last_id = log_key.ids.last().map(|i| i.id.clone());
    let logs = log_key
        .ids
        .iter()
        .map(|e| e.map.get("data").ok_or_else(|| str_error(NO_DATA)).cloned())
        .collect::<Result<Vec<redis::Value>, Box<dyn Error>>>()?;

    Ok((last_id, logs))
}

fn process_data(values: Vec<redis::Value>) -> Result<Vec<LogMessagePack>, Box<dyn Error>> {
    let un_valued: Vec<Vec<u8>> = values
        .iter()
        .map(|e| match e {
            redis::Value::BulkString(x) => Ok(x.iter().cloned().collect()),
            _ => Err(str_error("Log message data not binary-data!")),
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn Error>>>()?;

    Ok(un_valued
        .iter()
        .map(|e| rmp_serde::from_slice::<LogMessagePack>(e))
        .collect::<Result<Vec<LogMessagePack>, rmp_serde::decode::Error>>()?)
}

fn extract_records(messages: Vec<LogMessagePack>) -> Vec<LogRecord> {
    messages
        .iter()
        .map(|e| e.bec_codec.data.log_msg.record.clone())
        .collect()
}

pub async fn producer_loop(mut tx: mpsc::UnboundedSender<LogRecord>) {
    let mut redis_conn = redis_conn().expect("Could not connect to Redis!");
    let mut stream_read_id: String = ">".into();

    let _: Result<(), redis::RedisError> =
        redis_conn.xgroup_create(&LOGGING_ENDPOINT, "log-ingestor", "0");
    let _: Result<(), redis::RedisError> =
        redis_conn.xgroup_createconsumer(&LOGGING_ENDPOINT, "log-ingestor", "log-ingestor");

    'main: loop {
        if let Ok((Some(id), packed)) = read_logs(&mut redis_conn, &stream_read_id) {
            let unpacked = process_data(packed).unwrap_or(vec![error_log_item()]);
            let records = extract_records(unpacked);

            for record in records {
                if tx.send(record).is_err() {
                    println!("Receiver dropped, stopping...");
                    break 'main;
                }
            }
        }
    }
}
