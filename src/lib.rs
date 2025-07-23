pub mod db;
pub mod error;
pub mod types;
pub mod job;


use crate::error::StackDuckError;
use sqlx::{Postgres, pool::PoolConnection};

use crate::db::postgres::{DbPool, connect_to_db};
use crate::db::redis::connect_to_redis;

use crate::types::RedisClient;

// use deadpool_redis::redis::Client;

pub struct StackDuck {
    pub db_pool: DbPool,

    pub redis_client: Option<RedisClient>,
}

impl StackDuck {
    pub async fn new(database_url: &str) -> Result<Self, StackDuckError> {
        let db_pool = connect_to_db(database_url).await?;
        Ok(Self {
            db_pool,
            redis_client: None,
        })
    }

    pub async fn new_with_redis(
        database_url: &str,
        redis_url: &str,
    ) -> Result<Self, StackDuckError> {
        let db_pool = connect_to_db(database_url).await?;
        let redis_client = connect_to_redis(redis_url)
            .await
            .map_err(|err| StackDuckError::JobError((err.to_string())))?;
        Ok(Self {
            db_pool,
            redis_client: Some(redis_client),
        })
    }

    pub async fn get_postgres_conn(&self) -> Result<PoolConnection<Postgres>, StackDuckError> {
        self.db_pool
            .acquire()
            .await
            .map_err(|e| StackDuckError::DbConnectionError(e.to_string()))
    }

    pub async fn get_redis_conn(&self) -> Result<deadpool_redis::Connection, StackDuckError> {
        match &self.redis_client {
            Some(client) => client.get_redis_client().await,
            None => Err(StackDuckError::RedisConnectionError(
                "Redis not configured".into(),
            )),
        }
    }
}
