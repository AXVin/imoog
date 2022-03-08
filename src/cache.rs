use serde::Deserialize;
use async_trait::async_trait;
use redis::{
    aio,
    Client,
    AsyncCommands
};
use dashmap::DashMap;

#[derive(Debug, Deserialize)]
struct RedisCacheOptions {
    connection_uri: String
}

#[derive(Debug, Deserialize)]
struct MemoryCacheOptions {
    max_size: usize
}

#[async_trait]
pub trait CacheImpl<OptionsT> {
    async fn connect(options: OptionsT) -> Self;
    async fn insert(mut self, identifier: String, bytes: Vec<u8>, mime: String);
    async fn delete(mut self, identifier: String);
    async fn fetch(mut self, identifier: String) -> Option<(Vec<u8>, String)>;
}

pub struct CacheDriver<OptionsT, ConnectionT> {
    connection: ConnectionT,
    options: OptionsT
}

#[async_trait]
impl CacheImpl<RedisCacheOptions> for CacheDriver<RedisCacheOptions, aio::Connection> {
    async fn connect(options: RedisCacheOptions) -> Self {
        let client = Client::open(options.connection_uri.as_str())
            .expect("Failed to open redis client");
        
        let conn = client.get_async_connection()
            .await
            .expect("Failed to retrieve redis connection");

        Self {
            connection: conn,
            options
        }
    }

    async fn insert(mut self, identifier: String, bytes: Vec<u8>, mime: String) {
        let image_key = format!("{}-image", &identifier);
        let mime_key = format!("{}-mime", &identifier);

        // set our image key first
        self.connection.set::<String, Vec<u8>, ()>(image_key, bytes)
            .await
            .expect("Failed to set image cache (while setting image key) (Redis)");

        self.connection.set::<String, String, ()>(mime_key, mime)
            .await
            .expect("Failed to set image cache (while setting mime key) (Redis)");
    }

    async fn fetch(mut self, identifier: String) -> Option<(Vec<u8>, String)> {
        let image_key = format!("{}-image", &identifier);
        let mime_key = format!("{}-mime", &identifier);

        let image = self.connection.get::<String, Vec<u8>>(image_key)
            .await
            .unwrap();

        let mime = self.connection.get::<String, String>(mime_key)
            .await
            .unwrap();

        Some((image, mime))
    }

    async fn delete(mut self, identifier: String) {
        let image_key = format!("{}-image", &identifier);
        let mime_key = format!("{}-mime", &identifier);

        self.connection.del::<String, ()>(image_key)
            .await
            .unwrap();

        self.connection.del::<String, ()>(mime_key)
            .await
            .unwrap();

    }
}