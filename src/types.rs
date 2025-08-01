use crate::db::postgres;
use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use serde::{self, Deserialize, Serialize};
use sqlx::{FromRow, Type};
use std::collections::{HashMap, VecDeque};
use strum_macros::Display;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type InMemoryQueue = Mutex<HashMap<String, VecDeque<Job>>>;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub priority: Option<i32>,
    pub retry_count: Option<i32>,
    pub max_retries: Option<i32>,
    pub error_message: Option<String>,
    pub delay: Option<i32>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Display, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Deferred,
    Cancelled,
}

#[derive(Clone)]
pub struct RedisClient {
    pub pool: Pool,
}

#[derive()]
pub struct JobManager {
    pub db_pool: postgres::DbPool,
    pub redis_pool: Option<RedisClient>,
    pub in_memory_queue: InMemoryQueue,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RedisJob {
    pub id: i64,
    pub queue: String,
}

#[derive(Clone, Debug)]
pub struct JobNotification {
    pub job_type: String,
    pub notification_type: NotificationType,
}

#[derive(Debug, Clone, Display, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
pub enum NotificationType {
    NewJob,
    ExistingJob,
    RetryJob,
    CancelJob,
    CompleteJob,
    // ScheduledJob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcJob {
    pub id: String,
    pub job_type: String,
    pub payload: String,
    pub status: String,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub error_message: String,
    pub delay: i32,
    pub scheduled_at: i64,
    pub started_at: i64,
    pub completed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}
