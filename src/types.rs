use crate::db::postgres;
use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use serde::{self, Deserialize, Serialize};
use sqlx::{Type, FromRow};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use strum_macros::Display;


pub type InMemoryQueue = Arc<Mutex<HashMap<String, VecDeque<Job>>>>;

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

#[derive(Clone)]
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
    RetryJob,
    CancelJob,
    CompleteJob,
    ScheduledJob,
}
