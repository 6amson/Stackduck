use crate::db::postgres;
use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use deadpool_redis::Pool;
use serde::{self, Deserialize, Serialize};
use sqlx::{Type,};

#[derive(Serialize)]
pub struct Job {
    pub id: Uuid,
    pub status: JobStatus,
    pub job_type: String, 
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "TEXT")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Retrying,
}

#[derive(Clone)]
pub struct RedisClient {
    pub pool: Pool,
}

#[derive(Clone)]
pub struct JobManager {
    pub db_pool: postgres::DbPool,
    pub redis_pool: Option<RedisClient>,
    pub in_memory_queue: Arc<Mutex<VecDeque<Job>>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RedisJob {
    pub id: i64,
    pub queue: String,
}
