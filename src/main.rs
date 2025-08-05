use tokio::sync::mpsc;

mod redis_logs;
use crate::redis_logs::{LogRecord, producer_loop};

mod elastic_push;
use crate::elastic_push::consumer_loop;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::unbounded_channel::<LogRecord>();

    let producer = tokio::spawn(producer_loop(tx));
    consumer_loop(&mut rx).await;

    let _ = tokio::join!(producer);
}
