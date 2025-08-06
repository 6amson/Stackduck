pub mod db;
pub mod error;
pub mod job;
pub mod types;
pub mod stackduck {
    tonic::include_proto!("stackduck");
}
pub mod grpc;

use crate::db::postgres::{connect_to_db, DbPool};
use crate::db::redis::connect_to_redis;
use crate::error::StackDuckError;
use crate::types::RedisClient;
use deadpool_redis;
use sqlx::{pool::PoolConnection, Postgres};

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
            .map_err(|err| StackDuckError::JobError(err.to_string()))?;
        Ok(Self {
            db_pool,
            redis_client: Some(redis_client),
        })
    }

    pub async fn run_migrations(&self) -> Result<(), StackDuckError> {
        sqlx::migrate!("./migrations")
            .run(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::DbConnectionError(format!("Migration failed: {}", e)))?;
        Ok(())
    }

    pub fn has_redis(&self) -> bool {
        self.redis_client.is_some()
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
