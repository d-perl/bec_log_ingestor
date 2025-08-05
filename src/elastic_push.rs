use elasticsearch::{Elasticsearch, http::request::JsonBody};
use tokio::sync::mpsc;

use std::iter::once;

use crate::redis_logs::LogRecord;

fn elastic_client(api_key: &str) -> Result<Elasticsearch, elasticsearch::Error> {
    let url = elasticsearch::http::Url::parse("http://localhost:9200")?;
    let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
    let credentials = elasticsearch::auth::Credentials::EncodedApiKey(api_key.into());
    let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool)
        .auth(credentials)
        .build()?;
    Ok(Elasticsearch::new(transport))
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

pub async fn consumer_loop(rx: &mut mpsc::UnboundedReceiver<LogRecord>) {
    let elastic_client =
        elastic_client("ZUl6dGVaZ0JEbkl1Njd5RGl2MVc6UUluVTVWZ0FqNDZ2VlQzYkRwRHJzQQ==")
            .expect("Failed to connect to Elastic!");

    let mut buffer: Vec<LogRecord> = Vec::with_capacity(100);

    loop {
        let open = rx.recv_many(&mut buffer, 100).await;
        if open == 0 {
            break;
        }
        let body = make_docs_values(&buffer).unwrap_or(vec![]);
        let response = elastic_client
            .bulk(elasticsearch::BulkParts::Index("test-index"))
            .body(body)
            .send()
            .await;
        println!("sent {} logs to elastic, response: {:?}", open, response);
        buffer = Vec::with_capacity(100);
    }
    println!("Producer dropped, consumer exiting");
}
