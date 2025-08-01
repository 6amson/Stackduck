use crate::error::StackDuckError;
use crate::types::RedisClient;
use deadpool_redis::{Config, Runtime};

pub async fn connect_to_redis(redis_url: &str) -> Result<RedisClient, StackDuckError> {
    let cfg = Config::from_url(redis_url);

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
