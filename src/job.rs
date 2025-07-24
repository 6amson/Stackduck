use crate::{
    db::postgres::DbPool,
    error::StackDuckError,
    types::{Job, JobManager, JobStatus},
};
use chrono::Utc;
use sqlx::{Postgres, pool::PoolConnection, prelude::*};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
// use deadpool_redis as redis;
use deadpool_redis::redis::{FromRedisValue, Value};
use deadpool_redis::redis::{RedisError, RedisResult};
use deadpool_redis::{Connection, redis::AsyncCommands};
use std::str::FromStr;

impl Job {
    pub fn new(job_type: String, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type,
            payload,
            status: "queued".to_string(),
            // queue: None,
            priority: Some(0),
            retry_count: Some(0),
            max_retries: Some(3),
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

        // 1. Save to Postgres for persistence
        let inserted_job = sqlx::query_as::<_, Job>(
            r#"
    INSERT INTO jobs (id, job_type, payload, status, priority, retry_count, max_retries) 
    VALUES ($1, $2, $3, $4, $5, $6, $7) 
    RETURNING *
    "#,
        )
        .bind(&work.id)
        .bind(&work.job_type)
        .bind(&work.payload)
        .bind(&work.status)
        .bind(&work.priority)
        .bind(&work.retry_count)
        .bind(&work.max_retries)
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to insert job into Postgres: {}", e))
        })?;

        // 2. Enqueue to active queue systems only
        let queue_key = format!("Stackduck:queue:{}", inserted_job.job_type);
        let job_json = serde_json::to_string(&inserted_job)?;

        // Try Redis first
        if let Some(redis_client) = &self.redis_pool {
            if let Ok(mut conn) = redis_client.get_redis_client().await {
                if conn
                    .lpush::<&str, &str, i32>(&queue_key, &job_json)
                    .await
                    .is_ok()
                {
                    let cache_key = format!("Stackduck:job:{}", inserted_job.id);
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

        // 1. Try Redis first
        if let Some(pool) = &self.redis_pool {
            match pool.get_redis_client().await {
                Ok(mut conn) => {
                    let redis_result: Result<Option<String>, RedisError> =
                        conn.rpop(&key, None).await;
                    match redis_result {
                        Ok(Some(data)) => {
                            let job: Job = serde_json::from_str(&data)?;
                            // Update status to processing
                            let job_id = job.id.to_string();
                            self.update_job_status(&job_id, JobStatus::Running).await?;
                            let key = format!("Stackduck:job:{}", job_id);
                            let _: () = conn.hset(&key, "status", "running").await?;
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

        // 2. Try in-memory queue
        if let Ok(mut queues) = self.in_memory_queue.lock() {
            if let Some(queue) = queues.get_mut(&key) {
                if let Some(job) = queue.pop_front() {
                    // Update status to processing
                    self.update_job_status(&job.id.to_string(), JobStatus::Running)
                        .await?;
                    return Ok(Some(job));
                }
            }
        } else {
            eprintln!("Failed to acquire lock on in-memory queue");
        }

        // 3. Fallback to Postgres query with atomic status update
        let job = sqlx::query_as::<_, Job>(
            r#"
    UPDATE jobs 
    SET status = 'running', 
        started_at = NOW(),
        updated_at = NOW()
    WHERE id = (
        SELECT id FROM jobs
        WHERE (queue = $1 OR ($1 = job_type AND queue IS NULL))
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

        Ok(job)
    }

    pub async fn update_job_status(
        &self,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), StackDuckError> {
        // 1. Update in Postgres (source of truth)
        sqlx::query("UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(&new_status)
            .bind(job_id)
            .execute(&self.db_pool)
            .await
            .map_err(|e| {
                StackDuckError::JobError(format!("Failed to update job status in Postgres: {}", e))
            })?;

        // 2. Update in Redis cache if available
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get_redis_client().await {
                let cache_key = format!("Stackduck:job:{}", job_id);
                let updated_time = Utc::now().to_rfc3339().to_string();
                let _: () = conn
                    .hset_multiple::<_, _, _, ()>(
                        &cache_key,
                        &[
                            ("status", new_status.to_string()),
                            ("updated_at", updated_time),
                        ],
                    )
                    .await?;
            }
        }

        // 3. Update in in-memory structures if needed
        // Note: In-memory queues typically don't store job status since jobs are removed when dequeued
        // But you might have other in-memory structures tracking job status

        Ok(())
    }

    pub async fn update_redis_job_property(
        mut conn: Connection,
        job_id: Uuid,
        value: &str,
    ) -> Result<(), StackDuckError> {
        let key = format!("Stackduck:job:{}", job_id);
        conn.hset(&key, "status", value)
            .await
            .map_err(|e| StackDuckError::RedisJobError((e)))
    }

    pub async fn retry_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        // Get current job to check retry count
        let job = self.get_job_by_id(job_id).await?;
        let job = job.ok_or_else(|| StackDuckError::JobError("Job not found".to_string()))?;

        let current_retries = job.retry_count.unwrap_or(0);
        let max_retries = job.max_retries.unwrap_or(3);

        if current_retries >= max_retries {
            // Max retries reached, mark as failed
            sqlx::query("UPDATE jobs SET status = 'failed', updated_at = NOW() WHERE id = $1")
                .bind(job_id)
                .execute(&self.db_pool)
                .await
                .map_err(|e| {
                    StackDuckError::JobError(format!("Failed to mark job as failed: {}", e))
                })?;
        } else {
            // Reset status and increment retry count
            sqlx::query(
           "UPDATE jobs SET status = 'queued', retry_count = retry_count + 1, updated_at = NOW() WHERE id = $1"
       )
       .bind(job_id)
       .execute(&self.db_pool)
       .await
       .map_err(|e| StackDuckError::JobError(format!("Failed to retry job: {}", e)))?;

            // Re-enqueue the job
            let updated_job = self.get_job_by_id(job_id).await?;
            if let Some(job) = updated_job {
                self.enqueue(job).await?;
            }
        }

        Ok(())
    }

    pub async fn get_job_by_id(&self, job_id: &str) -> Result<Option<Job>, StackDuckError> {
        let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::JobError(format!("Failed to fetch job: {}", e)))?;

        Ok(job)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        sqlx::query("UPDATE jobs SET status = 'cancelled', updated_at = NOW() WHERE id = $1")
            .bind(job_id)
            .execute(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::JobError(format!("Failed to cancel job: {}", e)))?;

        Ok(())
    }

    pub async fn cache_job_as_hash(mut conn: Connection, job: &Job) -> Result<(), StackDuckError> {
        let key = format!("Stackduck:job:{}", job.id);

        let mut fields = vec![
            ("id", job.id.to_string()),
            ("job_type", job.job_type.clone()),
            ("status", job.status.clone()),
            ("payload", job.payload.to_string()),
        ];

        if let Some(priority) = job.priority {
            fields.push(("priority", priority.to_string()));
        }
        if let Some(retries) = job.retry_count {
            fields.push(("retry_count", retries.to_string()));
        }
        if let Some(max) = job.max_retries {
            fields.push(("max_retries", max.to_string()));
        }
        if let Some(t) = job.scheduled_at {
            fields.push(("scheduled_at", t.to_rfc3339()));
        }
        if let Some(t) = job.started_at {
            fields.push(("started_at", t.to_rfc3339()));
        }
        if let Some(t) = job.completed_at {
            fields.push(("completed_at", t.to_rfc3339()));
        }
        if let Some(t) = job.created_at {
            fields.push(("created_at", t.to_rfc3339()));
        }
        if let Some(t) = job.updated_at {
            fields.push(("updated_at", t.to_rfc3339()));
        }

        // Use HSET with tuple iterator
        conn.hset_multiple(&key, &fields)
            .await
            .map_err(|e| StackDuckError::RedisJobError((e)))
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
