use crate::{
    db::postgres::DbPool,
    error::StackDuckError,
    types::{Job, JobManager, JobStatus},
};
use chrono::Utc;
use sqlx::{Postgres, pool::PoolConnection, prelude::*};
use std::{collections::VecDeque, time::Duration};
use std::{
    fmt::format,
    sync::{Arc, Mutex},
};
use tokio::time::sleep;
use uuid::Uuid;
// use deadpool_redis as redis;
use deadpool_redis::redis::{FromRedisValue, Value};
use deadpool_redis::redis::{RedisError, RedisResult};
use deadpool_redis::{Connection, redis::AsyncCommands};
// use std::str::FromStr;

impl Job {
    pub fn new(job_type: String, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type,
            payload,
            status: JobStatus::Queued,
            priority: Some(0),
            retry_count: Some(0),
            max_retries: Some(3),
            error_message: None,
            scheduled_at: None,
            started_at: None,
            completed_at: None,
            created_at: None,
            updated_at: None,
        }
    }
}

impl JobManager {
    pub async fn enqueue(&self, job: Job) -> Result<Job, StackDuckError> {
        let work = job.clone();

        // Save to Postgres for persistence
        let inserted_job = sqlx::query_as::<_, Job>(
            r#"
    INSERT INTO jobs (id, job_type, payload, status, priority, retry_count, max_retries, scheduled_at) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
    RETURNING *
    "#,
        )
        .bind(&work.id)
        .bind(&work.job_type)
        .bind(&work.payload)
        .bind(JobStatus::Queued.to_string())
        .bind(&work.priority)
        .bind(&work.retry_count)
        .bind(&work.error_message)
        bind(7)
        .bind(&work.max_retries)
        .bind(&work.scheduled_at)
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to insert job into Postgres: {}", e))
        })?;

        //  Enqueue to active queue systems only
        let queue_key = format!("Stackduck:queue:{}", inserted_job.job_type);
        let job_json = serde_json::to_string(&inserted_job)?;

        // Try Redis first
        if let Some(redis_client) = &self.redis_pool {
            if let Ok(mut conn) = redis_client.get_redis_client().await {
                let score = inserted_job
                    .scheduled_at
                    .unwrap_or_else(|| Utc::now())
                    .timestamp() as f64;

                if conn
                    .zadd::<&str, f64, &str, Value>(&queue_key, &job_json, score)
                    .await
                    .is_ok()
                {
                    let cache_key = format!("Stackduck:job:{}", inserted_job.id);

                    todo!("Job status update in Redis for corresponding apis");
                    let _: Result<(), _> = conn.set(&cache_key, &job_json).await;
                    return Ok(inserted_job);
                }
            }
        }

        // Fallback to in-memory
        self.fallback_to_memory(&queue_key, inserted_job.clone())?;

        // If both fail, the job is persisted but not actively queued
        // It will be picked up by the Postgres fallback in dequeue
        eprintln!(
            "⚠️ Job {} persisted but not actively queued - will be processed via Postgres fallback",
            inserted_job.id
        );
        Ok(inserted_job)
    }

    pub async fn dequeue(&self, queue_name: &str) -> Result<Option<Job>, StackDuckError> {
        let key = format!("Stackduck:queue:{}", queue_name);
        let started_at = Some(Utc::now());

        // Try Redis first
        if let Some(pool) = &self.redis_pool {
            match pool.get_redis_client().await {
                Ok(mut conn) => {
                    let now = Utc::now().timestamp() as f64;
                    let processing_score = (Utc::now().timestamp() + 1800) as f64;
                    let redis_result: Result<Option<String>, RedisError> = conn
                        .zrangebyscore_limit(&key, "-inf", &now.to_string(), 0, 1)
                        .await;
                    match redis_result {
                        Ok(Some(data)) => {
                            let job_json = &results[0];
                            let job: Job = serde_json::from_str(job_json)?;

                            //remove from queue
                            let _: () = conn.zrem(&key, &data).await?;
                            // Update status to running
                            let job_id = job.id.to_string();
                            let _: () = conn
                                .zadd(
                                    "Stackduck:running",
                                    &format!("{}", job_id),
                                    processing_score,
                                )
                                .await?;
                            let key = format!("Stackduck:job:{}", job_id);

                            // update redis status to running.
                            let _: () = conn.hset(&key, "status", "running").await?;

                            //update postgres
                            self.update_dequeue_job_status(&job_id, JobStatus::Running).await?;

                            return Ok(Some(job));
                        }
                        Ok(None) => {
                            // Redis queue is empty, continue to fallback
                        }
                        Err(e) => {
                            eprintln!("Redis dequeue failed, falling back to in-memory: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Redis unavailable: {}. Falling back to in-memory queue.", e);
                }
            }
        }

        // Try in-memory queue
        if let Ok(mut queues) = self.in_memory_queue.lock() {
            if let Some(queue) = queues.get_mut(&key) {
                if let Some(job) = queue.pop_front() {
                    // Update status to processing
                    self.update_dequeue_job_status(&job.id.to_string(), JobStatus::Running)
                        .await?;
                    return Ok(Some(job));
                }
            }
        } else {
            eprintln!("Failed to acquire lock on in-memory queue");
        }

        let job = self.update_postgres_dequeue_fallback(job, queue_name).await;

        Ok(job)
    }

    pub async fn ack_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        // Update in Postgres
        sqlx::query(
            "UPDATE jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW() WHERE id = $1"
        )
        .bind(job_id)
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to mark job as completed: {}", e)))?;

        // Update in Redis if available
        if let Some(client) = &self.redis_pool {
            if let Ok(conn) = client.pool.get().await {
                let running_jobs: Vec<String> = conn.zrange("Stackduck:running", 0, -1).await;
                let updates = vec![
                    ("status", "completed".to_string()),
                    ("completed_at", chrono::Utc::now().to_rfc3339()),
                    ("updated_at", chrono::Utc::now().to_rfc3339()),
                ];
                let _: () = conn.zrem("Stackduck:running", &job_id).await?;
                let _ = self.update_redis_job_property(conn, job_id, &updates).await;
            }
        }

        Ok(())
    }

    pub async fn nack_job(
        &self,
        job_id: &str,
        error_message: &str,
    ) -> Result<(), StackDuckError> {
        // Get job details to check retry eligibility
        let job = self.get_job_by_id(job_id).await?;
        job.error_message = Some(error_message.to_string());

        if let Some(mut job) = job {
            let current_retry_count = job.retry_count.unwrap_or(0);
            let max_retries = job.max_retries.unwrap_or(3);

            if current_retry_count < max_retries {
                // Job is eligible for retry
                job.retry_count = Some(current_retry_count + 1);
                self.retry_job(job).await?;
            } else {
                // Max retries exceeded - mark as permanently failed and move to dead letter queue
                self.nack_job(job_id, &job).await?;
            }
        } else {
            return Err(StackDuckError::JobError(format!(
                "Job {} not found",
                job_id
            )));
        }

        Ok(())
    }

    async fn handle_nack_and_retry(&self, job_id: &str, job: &Job) -> Result<(), StackDuckError> {
        // Update in Postgres with failed status and failed_at timestamp
        sqlx::query(
        "UPDATE jobs SET status = 'failed', failed_at = NOW(), updated_at = NOW() WHERE id = $1"
    )
    .bind(job_id)
    .execute(&self.db_pool)
    .await
    .map_err(|e| {
        StackDuckError::JobError(format!("Failed to mark job as permanently failed: {}", e))
    })?;

        // Remove from Redis running queue and move to dead letter queue
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                // Remove from running queue (if it exists there)
                let running_jobs: Vec<String> = conn
                    .zrange("Stackduck:running", 0, -1)
                    .await
                    .map_err(|e| StackDuckError::RedisJobError(e.to_string()))?;
                let _: () = conn.zrem("Stackduck:running", &job_id).await?;


                let reason = format!(
                    "Max retries ({}) exceeded with error: {}",
                    max_retries,
                    job.error_message.unwrap_or("No error message".to_string())
                );

                // Create dead letter queue entry with full context
                let dead_letter_entry = serde_json::json!({
                    "job_id": job_id,
                    "job_type": job.job_type,
                    "payload": job.payload,
                    "retry_count": job.retry_count.unwrap_or(0),
                    "max_retries": job.max_retries.unwrap_or(3),
                    "failed_at": chrono::Utc::now().to_rfc3339(),
                    "original_created_at": job.created_at.map(|dt| dt.to_rfc3339()),
                    "reason": reason
                });

                // Add to dead letter queue
                let dlq_key = format!("Stackduck:dlq:{}", job.job_type);
                let _: Result<i32, _> = conn.lpush(&dlq_key, dead_letter_entry.to_string()).await;

                // Increment dead letter queue counter for metrics
                let dlq_count_key = format!("Stackduck:dlq:{}:count", job.job_type);
                let _: Result<i32, _> = conn.incr(&dlq_count_key, 1).await;

                // Update job status in Redis cache if it exists
                let updates = vec![
                    ("status", "failed".to_string()),
                    ("failed_at", chrono::Utc::now().to_rfc3339()),
                    ("updated_at", chrono::Utc::now().to_rfc3339()),
                ];
                let _ = self
                    .update_redis_job_property(&mut conn, job_id, &updates)
                    .await;
            }
        }

        Ok(())
    }

    pub async fn update_dequeue_job_status(
        &self,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), StackDuckError> {
        // 1. Update Postgres
        sqlx::query("UPDATE jobs SET status = $1, started_at = NOW(), updated_at = NOW() WHERE id = $2")
            .bind(&new_status.to_string())
            .bind(job_id)
            .execute(&self.db_pool)
            .await
            .map_err(|e| {
                StackDuckError::JobError(format!("Failed to update job status in Postgres: {}", e))
            })?;

        // 2. Update in Redis cache if available
        if let Some(pool) = &self.redis_pool {
            if let Ok(conn) = pool.get_redis_client().await {
                let updated_time = Utc::now().to_rfc3339();
                self.update_redis_job_property(
                    conn,
                    job_id,
                    &[
                        ("status", new_status.to_string()),
                        ("updated_at", updated_time),
                    ],
                )
                .await?;
            }
        }

        Ok(())
    }


    pub async fn retry_job(&self, mut job: Job) -> Result<(), StackDuckError> {
        // Calculate exponential backoff delay (30 seconds * 2^retry_count)
        let retry_count = job.retry_count.unwrap_or(0);

        todo!("ADD dynamic delay in job struct!");
        let delay_seconds = 30 * (2_i64.pow(retry_count as u32));
        let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(delay_seconds);

        // Update job for retry
        job.status = "queued".to_string();
        job.scheduled_at = Some(scheduled_at);
        job.started_at = None; // Clear previous start time

        // Update in Postgres
        sqlx::query(
            r#"
        UPDATE jobs 
        SET status = 'queued', 
            retry_count = $2, 
            scheduled_at = $3,
            started_at = NULL,
            updated_at = NOW() 
        WHERE id = $1
        "#,
        )
        .bind(&job.id.to_string())
        .bind(retry_count)
        .bind(scheduled_at)
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to update job for retry: {}", e)))?;

        // Re-enqueue in Redis with scheduled time
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                // Remove from processing queue first
                let processing_jobs: Vec<String> = conn
                    .zrange("Stackduck:processing", 0, -1)
                    .await
                    .unwrap_or_default();

                for entry in processing_jobs {
                    if entry.starts_with(&format!("{}:", job.id)) {
                        let _: Result<i32, _> = conn.zrem("Stackduck:processing", &entry).await;
                        break;
                    }
                }

                // Add back to queue with new scheduled time
                let queue_key = format!("Stackduck:queue:{}", job.job_type);
                let job_json = serde_json::to_string(&job)?;
                let score = scheduled_at.timestamp() as f64;

                let _: Result<i32, _> = conn.zadd(&queue_key, score, &job_json).await;

                // Update job cache
                let cache_key = format!("Stackduck:job:{}", job.id);
                let _: Result<String, _> = conn.setex(&cache_key, 3600, &job_json).await;
            }
        }

        // Notify workers about the retried job (after delay)
        // This could be done via your existing notification system

        Ok(())
    }


    pub async fn update_redis_job_property(
        &self,
        mut conn: Connection,
        job_id: &str,
        values: &[(&str, String)],
    ) -> Result<(), StackDuckError> {
        let key = format!("Stackduck:job:{}", job_id);
        conn.hset_multiple::<_, _, _, ()>(&key, values)
            .await
            .map_err(|e| StackDuckError::RedisJobError((e)))
    }

    pub async fn get_redis_job_value(
        &self,
        conn: &mut Connection,
        job_id: Uuid,
        field: &str,
    ) -> Result<Option<String>, StackDuckError> {
        let key = format!("Stackduck:job:{}", job_id);
        let value: Option<String> = conn
            .hget(&key, field)
            .await
            .map_err(StackDuckError::RedisJobError)?;
        Ok(value)
    }

    pub async fn get_job_by_id(&self, job_id: &str) -> Result<Option<Job>, StackDuckError> {
        // Try Redis cache first if available
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                let cache_key = format!("Stackduck:job:{}", job_id);
                if let Ok(Some(job_json)) = conn.get::<String, Option<String>>(&cache_key).await {
                    if let Ok(job) = serde_json::from_str::<Job>(&job_json) {
                        return Ok(Some(job));
                    }
                }
            }
        }

        // Fallback to Postgres
        let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::JobError(format!("Failed to fetch job: {}", e)))?;

        // Cache the result in Redis for future lookups
        if let Some(ref job) = job {
            if let Some(client) = &self.redis_pool {
                if let Ok(mut conn) = client.get_redis_client().await {
                    let cache_key = format!("Stackduck:job:{}", job_id);
                    let job_json = serde_json::to_string(job).unwrap_or_default();
                    let _: Result<String, _> = conn.setex(&cache_key, 3600, &job_json).await; // Cache for 1 hour
                }
            }
        }

        Ok(job)
    }

    // pub async fn cache_job_as_hash(mut conn: Connection, job: &Job) -> Result<(), StackDuckError> {
    //     let key = format!("Stackduck:job:{}", job.id);

    //     let mut fields = vec![
    //         ("id", job.id.to_string()),
    //         ("job_type", job.job_type.clone()),
    //         ("status", job.status.clone()),
    //         ("payload", job.payload.to_string()),
    //     ];

    //     if let Some(priority) = job.priority {
    //         fields.push(("priority", priority.to_string()));
    //     }
    //     if let Some(retries) = job.retry_count {
    //         fields.push(("retry_count", retries.to_string()));
    //     }
    //     if let Some(max) = job.max_retries {
    //         fields.push(("max_retries", max.to_string()));
    //     }
    //     if let Some(t) = job.scheduled_at {
    //         fields.push(("scheduled_at", t.to_rfc3339()));
    //     }
    //     if let Some(t) = job.started_at {
    //         fields.push(("started_at", t.to_rfc3339()));
    //     }
    //     if let Some(t) = job.completed_at {
    //         fields.push(("completed_at", t.to_rfc3339()));
    //     }
    //     if let Some(t) = job.created_at {
    //         fields.push(("created_at", t.to_rfc3339()));
    //     }
    //     if let Some(t) = job.updated_at {
    //         fields.push(("updated_at", t.to_rfc3339()));
    //     }

    //     // Use HSET with tuple iterator
    //     conn.hset_multiple(&key, &fields)
    //         .await
    //         .map_err(|e| StackDuckError::RedisJobError((e)))
    // }

    pub async fn update_postgres_dequeue_fallback(
        &self,
        mut job: Job,
        queue_name: &str,
    ) -> Option<Job> {
        // Fallback to Postgres query with atomic status update
        let job = sqlx::query_as::<_, Job>(
            r#"
    UPDATE jobs 
    SET status = 'running', 
        started_at = NOW(),
        updated_at = NOW()
    WHERE id = (
        SELECT id FROM jobs
        WHERE (job_type = $1
          AND status = 'queued'
          AND (scheduled_at IS NULL OR scheduled_at <= NOW())
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
    "#,
        )
        .bind(queue_name)
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Postgres query failed: {}", e)))?;

        job
    }

    fn fallback_to_memory(&self, queue_key: &str, job: Job) -> Result<(), StackDuckError> {
        let mut queues = self
            .in_memory_queue
            .lock()
            .map_err(|e| StackDuckError::JobError(format!("In-memory queue lock error: {}", e)))?;

        queues
            .entry(queue_key.to_string())
            .or_insert_with(VecDeque::new)
            .push_back(job);

        Ok(())
    }
}
