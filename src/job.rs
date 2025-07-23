use crate::{
    db::postgres::DbPool,
    error::StackDuckError,
    types::{Job, JobManager, JobStatus},
};
use sqlx::{Postgres, pool::PoolConnection, prelude::*};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
// use deadpool_redis as redis;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::redis::RedisError;
use deadpool_redis::redis::Value;

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
    pub async fn enqueue_job(&self, job: Job) -> Result<Job, StackDuckError> {
        let work = job.clone();

        // 1. Save to Postgres for persistence
        let inserted_job = sqlx::query_as!(
            Job,
            r#"
        INSERT INTO jobs (id, job_type, payload, status, priority, retry_count, max_retries) 
        VALUES ($1, $2, $3, $4, $5, $6, $7) 
        RETURNING *
        "#,
            &work.id,
            &work.job_type,
            &work.payload,
            "queued", // Always start as queued
            // &work.queue,
            &work.priority,
            &work.retry_count,
            &work.max_retries
        )
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to persist job: {}", e)))?;

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
                    return Ok(inserted_job);
                }
            }
        }

        // Fallback to in-memory
        if let Ok(mut queues) = self.in_memory_queue.lock() {
            queues
                .entry(queue_key)
                .or_insert_with(VecDeque::new)
                .push_back(inserted_job.clone());
            return Ok(inserted_job);
        }

        // If both fail, the job is persisted but not actively queued
        // It will be picked up by the Postgres fallback in dequeue
        eprintln!(
            "⚠️ Job {} persisted but not actively queued - will be processed via Postgres fallback",
            inserted_job.id
        );
        Ok(inserted_job)
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

    async fn dequeue(&self, queue_name: &str) -> Result<Option<Job>, StackDuckError> {
        let key = format!("Stackduck:queue:{}", queue_name); // queue_name is job.job_type, always recall, to get the queue for that particular job type.

        // 1. Try Redis first
        if let Some(pool) = &self.redis_pool {
            match pool.get_redis_client().await {
                Ok(mut conn) => {
                    let redis_result: Result<Option<String>, RedisError> =
                        conn.rpop(&key, None).await;
                    match redis_result {
                        Ok(Some(data)) => {
                            let job: Job = serde_json::from_str(&data)?;
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
                    return Ok(Some(job));
                }
            }
        } else {
            eprintln!("Failed to acquire lock on in-memory queue");
        }

        // 3. Fallback to Postgres query
        let job = sqlx::query_as!(
            Job,
            r#"
        UPDATE jobs 
        SET status = 'processing'
        WHERE id = (
            SELECT id FROM jobs
            WHERE queue = $1 AND status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
            queue_name
        )
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Postgres query failed: {}", e)))?;

        Ok(job)
    }

    async fn update_job_status(
        &self,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), StackDuckError> {
        // 1. Update in Postgres (source of truth)
        sqlx::query!(
            "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2",
            new_status.to_string(),
            job_id
        )
        .execute(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to update job status in Postgres: {}", e))
        })?;

        // 2. Update in Redis cache if available
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get_redis_client().await {
                let cache_key = format!("Stackduck:job:{}", job_id);

                // Get the job from cache, update status, and put it back
                if let Ok(Some(job_data)) = conn.get::<String>(&cache_key).await {
                    if let Ok(mut job) = serde_json::from_str::<Job>(&job_data) {
                        job.status = new_status.clone();
                        job.updated_at = Some(chrono::Utc::now());

                        if let Ok(updated_data) = serde_json::to_string(&job) {
                            let _: Result<(), RedisError> =
                                conn.set(&cache_key, updated_data).await;
                        }
                    }
                }
            }
        }

        // 3. Update in in-memory structures if needed
        // Note: In-memory queues typically don't store job status since jobs are removed when dequeued
        // But you might have other in-memory structures tracking job status

        Ok(())
    }

    // Updated dequeue method that uses proper status management
    async fn dequeue(&self, queue_name: &str) -> Result<Option<Job>, StackDuckError> {
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
                            self.update_job_status(&job.id, JobStatus::Processing)
                                .await?;
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
                    self.update_job_status(&job.id, JobStatus::Processing)
                        .await?;
                    return Ok(Some(job));
                }
            }
        } else {
            eprintln!("Failed to acquire lock on in-memory queue");
        }

        // 3. Fallback to Postgres query with atomic status update
        let job = sqlx::query_as!(
            Job,
            r#"
        UPDATE jobs 
        SET status = 'processing', updated_at = NOW()
        WHERE id = (
            SELECT id FROM jobs
            WHERE queue = $1 AND status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
            queue_name
        )
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Postgres query failed: {}", e)))?;

        Ok(job)
    }

    // Additional helper methods you might want
    async fn mark_job_completed(&self, job_id: &str) -> Result<(), StackDuckError> {
        self.update_job_status(job_id, JobStatus::Completed).await
    }

    async fn mark_job_failed(
        &self,
        job_id: &str,
        error_message: Option<String>,
    ) -> Result<(), StackDuckError> {
        // Update status
        self.update_job_status(job_id, JobStatus::Failed).await?;

        // Optionally store error message
        if let Some(error) = error_message {
            sqlx::query!(
                "UPDATE jobs SET error_message = $1 WHERE id = $2",
                error,
                job_id
            )
            .execute(&self.db_pool)
            .await
            .map_err(|e| {
                StackDuckError::JobError(format!("Failed to update error message: {}", e))
            })?;
        }

        Ok(())
    }

    async fn retry_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        // Reset status and increment retry count
        sqlx::query!(
        "UPDATE jobs SET status = 'queued', retry_count = retry_count + 1, updated_at = NOW() WHERE id = $1",
        job_id
    )
    .execute(&self.db_pool)
    .await
    .map_err(|e| StackDuckError::JobError(format!("Failed to retry job: {}", e)))?;

        // Re-enqueue the job
        let job = self.get_job_by_id(job_id).await?;
        if let Some(job) = job {
            self.enqueue(job).await?;
        }

        Ok(())
    }
}
