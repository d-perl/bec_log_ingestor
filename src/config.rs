use std::io::Read;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct UrlPort {
    pub url: String,
    pub port: u16,
}

impl UrlPort {
    pub fn full_url(&self) -> String {
        self.url.to_owned() + ":" + &self.port.to_string()
    }
}

fn default_chunk_size() -> u16 {
    100
}

#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
    pub url: UrlPort,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u16,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ElasticConfig {
    pub url: UrlPort,
    pub api_key: String,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u16,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IngestorConfig {
    pub redis: RedisConfig,
    pub elastic: ElasticConfig,
}

impl IngestorConfig {
    /// Parse a toml file for an IngestorConfig. Assumes the file exists and is readable.
    pub fn from_file(path: std::path::PathBuf) -> Self {
        let mut file = std::fs::File::open(path).expect("Cannot open supplied config file!");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Cannot read supplied config file!");
        toml::from_str(&contents).unwrap()
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_port() {
        let test_str = "
url = \"http://127.0.0.1\"
port = 12345
";
        let url: UrlPort = toml::from_str(&test_str).unwrap();
        assert_eq!(url.full_url(), "http://127.0.0.1:12345")
    }

    #[test]
    fn test_redis() {
        let test_str = "
[url]
url = \"http://127.0.0.1\"
port = 12345
";
        let redis: RedisConfig = toml::from_str(&test_str).unwrap();
        assert_eq!(redis.url.full_url(), "http://127.0.0.1:12345")
    }

    #[test]
    fn test_ingestor() {
        let test_str = "
[redis.url]
url = \"http://127.0.0.1\"
port = 12345

[elastic]
api_key = \"abcdefgh==\"

[elastic.url]
url = \"http://127.0.0.1\"
port = 9876
";
        let config: IngestorConfig = toml::from_str(&test_str).unwrap();
        assert_eq!(config.redis.url.full_url(), "http://127.0.0.1:12345");
        assert_eq!(config.elastic.url.full_url(), "http://127.0.0.1:9876");
        assert_eq!(config.elastic.api_key, "abcdefgh==");
    }
}
