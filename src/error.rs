use thiserror::Error;
use serde_json;
use deadpool_redis::redis::RedisError;


#[derive(Debug, Error)]
pub enum StackDuckError {
    #[error("Database connection failed: {0}")]
    DbConnectionError(String),

    #[error("Migration failed: {0}")]
    MigrationError(String),

    #[error("Job Error: {0}")]
    JobError(String),

    #[error("Redis error: {0}")]
    RedisJobError(#[from] RedisError),

    #[error("Redis connection error: {0}")]
    RedisConnectionError(String),

    #[error("Serde JSON error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("Unknown error occurred")]
    Unknown,
}


