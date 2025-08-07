use crate::db::postgres;
use chrono::{DateTime, Duration, Utc};
use deadpool_redis::Pool;
use serde::{self, Deserialize, Serialize};
use sqlx::{FromRow, Type};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
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
    pub redis_pool: RedisClient,
}

#[derive(Clone, Debug)]
pub struct JobNotification {
    pub job_type: String,
    pub notification_type: NotificationType,
    pub timestamp: Instant,
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

#[derive(Debug, Clone, Serialize, Deserialize,)]
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

pub const DEQUEUE_SCRIPT: &str = r#"
    local queue_key = KEYS[1]
    local running_key = KEYS[2]
    local lock_key = KEYS[3]
    local worker_id = ARGV[1]
    local now = ARGV[2]
    local processing_score = ARGV[3]
    local lock_ttl = ARGV[4]
    
    -- Try to acquire distributed lock for this queue
    local lock_acquired = redis.call('SET', lock_key, worker_id, 'PX', lock_ttl, 'NX')
    if not lock_acquired then
        return nil  -- Another worker is dequeuing from this queue
    end
    
    -- We have the lock, try atomic dequeue with ZPOPMIN
    local job_data = redis.call('ZPOPMIN', queue_key, 1)
    
    -- Release the lock immediately
    redis.call('DEL', lock_key)
    
    if #job_data > 0 then
        local job_json = job_data[1]  -- job_data contains [member, score]
        local score = job_data[2]
        
        -- Only process if the job's scheduled time has passed
        if tonumber(score) <= tonumber(now) then
            -- Parse job to get ID
            local job = cjson.decode(job_json)
            local running_entry = job.id .. ':' .. job_json
            
            -- Add to running set with processing timeout
            redis.call('ZADD', running_key, processing_score, running_entry)
            
            return job_json
        else
            -- Job not ready yet, put it back
            redis.call('ZADD', queue_key, score, job_json)
            return nil
        end
    end
    
    return nil
"#;

#[derive(Default, Debug)]
pub struct QueueStats {
    pub jobs_processed: u64,
    pub last_job_time: Option<Instant>,
    pub average_processing_time: Option<Duration>,
}

// Worker state tracking
#[derive(Debug)]
pub struct WorkerState {
    pub worker_id: String,
    pub job_types: Vec<String>,
    pub last_activity: Instant,
    pub jobs_processed: u64,
    pub consecutive_errors: usize,
    pub backoff_until: Option<Instant>,
    pub queue_rotation_index: usize,
}
