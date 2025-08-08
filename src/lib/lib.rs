use tokio::sync::mpsc;
pub mod config;
pub mod elastic_push;
pub mod redis_logs;
use redis_logs::{LogMsg, producer_loop};

use elastic_push::consumer_loop;

use config::IngestorConfig;

pub async fn main_loop(config: IngestorConfig) {
    println!("Starting log ingestor with config: \n {:?}", &config);

    let (tx, mut rx) = mpsc::unbounded_channel::<LogMsg>();
    let producer = tokio::spawn(producer_loop(tx, config.redis.clone()));
    consumer_loop(&mut rx, config.elastic.clone()).await;

    let _ = tokio::join!(producer);
}

#[pyo3::pymodule]
mod bec_log_ingestor {
    use crate::config::IngestorConfig;
    use pyo3::prelude::*;
    use tokio::runtime::Runtime;

    /// Run forever. Will block forever.
    #[pyfunction]
    fn run_with_config(filename: String) {
        let config = IngestorConfig::from_file(filename);
        let rt = Runtime::new().unwrap();
        rt.block_on(super::main_loop(config));
    }
}
