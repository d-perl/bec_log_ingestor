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

/// Default number of records to read from Redis or push to Elastic at once
fn default_chunk_size() -> u16 {
    100
}
/// Default timeout for blocking XREAD calls
fn default_blocktime_millis() -> usize {
    1000
}
/// Default value for both the consumer group and consumer ID
fn default_consumer() -> String {
    "log-ingestor".into()
}
/// Default value for the elastic index
fn default_index() -> String {
    "logstash-bec_test123".into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
    pub url: UrlPort,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u16,
    #[serde(default = "default_blocktime_millis")]
    pub blocktime_millis: usize,
    #[serde(default = "default_consumer")]
    pub consumer_group: String,
    #[serde(default = "default_consumer")]
    pub consumer_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ElasticConfig {
    pub url: UrlPort,
    pub api_key: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u16,
    #[serde(default = "default_index")]
    pub index: String,
}

impl ElasticConfig {
    pub fn credentials(&self) -> Result<elasticsearch::auth::Credentials, &str> {
        if let Some(api_key) = &self.api_key {
            Ok(elasticsearch::auth::Credentials::EncodedApiKey(
                api_key.into(),
            ))
        } else if let Some(username) = &self.username
            && let Some(password) = &self.password
        {
            Ok(elasticsearch::auth::Credentials::Basic(
                username.to_owned(),
                password.to_owned(),
            ))
        } else {
            Err("No credentials in config!")
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
pub struct IngestorConfig {
    pub redis: RedisConfig,
    pub elastic: ElasticConfig,
}

impl IngestorConfig {
    /// Parse a toml file for an IngestorConfig. Assumes the file exists and is readable.
    pub fn from_file<P>(path: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        let mut file = std::fs::File::open(path).expect("Cannot open supplied config file!");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Cannot read supplied config file!");
        toml::from_str(&contents).unwrap()
    }
}

#[cfg(test)]
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
        assert_eq!(config.elastic.api_key, Some("abcdefgh==".into()));
    }

    #[test]
    fn test_redis_defaults() {
        let test_str = "
[url]
url = \"http://localhost\"
port = 6379
";
        let redis: RedisConfig = toml::from_str(&test_str).unwrap();
        assert_eq!(redis.chunk_size, 100);
        assert_eq!(redis.blocktime_millis, 1000);
        assert_eq!(redis.consumer_group, "log-ingestor");
        assert_eq!(redis.consumer_id, "log-ingestor");
    }

    #[test]
    fn test_elastic_defaults() {
        let test_str = "
url = { url = \"http://localhost\", port = 9200 }
api_key = \"testkey\"
";
        let elastic: ElasticConfig = toml::from_str(&test_str).unwrap();
        assert_eq!(elastic.chunk_size, 100);
        assert_eq!(elastic.api_key, Some("testkey".into()));
        assert_eq!(elastic.url.full_url(), "http://localhost:9200");
    }

    #[test]
    fn test_invalid_urlport_missing_field() {
        let test_str = "
url = \"http://localhost\"
";
        let result: Result<UrlPort, _> = toml::from_str(&test_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_toml() {
        let test_str = "
this is not toml
";
        let result: Result<RedisConfig, _> = toml::from_str(&test_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_file_error() {
        use std::path::PathBuf;
        let path = PathBuf::from("/nonexistent/config.toml");
        let result = std::panic::catch_unwind(|| {
            IngestorConfig::from_file(path);
        });
        assert!(result.is_err());
    }
}
