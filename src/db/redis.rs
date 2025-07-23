use crate::types::RedisClient;
use crate::error::StackDuckError; 
use deadpool_redis::{Config, Runtime};

pub async fn connect_to_redis(redis_url: &str) -> Result<RedisClient, StackDuckError> {
    let mut cfg = Config::default();
    cfg.url = Some(redis_url.to_string());

    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| StackDuckError::RedisConnectionError(e.to_string()))?;

    Ok(RedisClient { pool })
}

impl RedisClient {
    pub async fn get_redis_client(&self) -> Result<deadpool_redis::Connection, StackDuckError> {
        self.pool
            .get()
            .await
            .map_err(|e| StackDuckError::RedisConnectionError(e.to_string()))
    }
}

