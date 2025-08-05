use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Elapsed {
    repr: String,
    seconds: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct File {
    name: String,
    path: String,
}
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogLevel {
    icon: String,
    name: String,
    no: usize,
}
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct NameId {
    name: String,
    id: usize,
}
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Timestamp {
    repr: String,
    timestamp: f64,
}
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct LogRecord {
    elapsed: Elapsed,
    exception: Option<serde_json::Value>,
    extra: serde_json::Value,
    file: File,
    function: String,
    level: LogLevel,
    line: usize,
    message: String,
    module: String,
    name: String,
    process: NameId,
    thread: NameId,
    time: Timestamp,
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
pub struct LogMessagePack {
    #[serde(rename = "__bec_codec__")]
    bec_codec: LogMessagePackInternal,
}
