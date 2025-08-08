use bec_log_ingestor::{config::IngestorConfig, main_loop};
use clap::Parser;
use std::process::exit;

#[derive(clap::Parser, Debug)]
struct Args {
    /// Specify a config file
    #[arg(short = 'c', long = "config")]
    config: std::path::PathBuf,
}

fn config_path() -> std::path::PathBuf {
    let args = Args::parse();
    if !args.config.exists() {
        println!(
            "Error, config file not found at {:?}",
            args.config.as_path()
        );
        exit(1)
    }
    args.config
}

fn entry() -> IngestorConfig {
    let path = config_path();
    IngestorConfig::from_file(path)
}

#[tokio::main]
async fn main() {
    let config = entry();
    main_loop(config).await
}
