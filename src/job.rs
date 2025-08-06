use crate::{
    error::StackDuckError,
    types::{Job, JobManager, JobStatus, DEQUEUE_SCRIPT},
};
use chrono::Utc;
use deadpool_redis::{redis::AsyncCommands, Connection};
use redis::Script;
use std::{collections::VecDeque, vec};
use uuid::Uuid;

impl Job {
    pub fn new(
        job_type: String,
        payload: serde_json::Value,
        priority: Option<i32>,
        delay: Option<i32>,
        max_retries: Option<i32>,
        scheduled_at: Option<chrono::DateTime<Utc>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type,
            payload,
            status: JobStatus::Queued.to_string(),
            priority,
            retry_count: Some(0),
            max_retries,
            error_message: None,
            delay,
            scheduled_at,
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
    INSERT INTO jobs (id, job_type, payload, status, priority, retry_count, error_message,  delay, max_retries, scheduled_at) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
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
        .bind(&work.delay)
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
                let priority = inserted_job.priority.unwrap_or(2);

                let base_timestamp = inserted_job
                    .scheduled_at
                    .unwrap_or_else(|| Utc::now())
                    .timestamp() as f64;

                // Reverse priority scoring so lower numbers = higher priority
                // Priority 1 (high) = 0, Priority 2 (medium) = 1, Priority 3 (low) = 2
                let priority_offset = (3 - priority) as f64;

                // Use a much smaller multiplier to avoid overwhelming the timestamp
                // This gives us priority differences of 1000 seconds (16 minutes)
                let score = base_timestamp + (priority_offset * 1000.0);

                println!(
                    "🔍 Enqueue: job_id={}, priority={}, base_timestamp={}, final_score={}",
                    inserted_job.id, priority, base_timestamp, score
                );

                if conn
                    .zadd::<&str, f64, &str, i32>(&queue_key, &job_json, score)
                    .await
                    .is_ok()
                {
                    let cache_key = format!("Stackduck:job:{}", inserted_job.id);
                    let _: Result<(), _> = conn.set_ex(&cache_key, &job_json, 3600).await;
                    return Ok(inserted_job);
                }
            }
        }

        // Fallback to in-memory
        let _ = self
            .fallback_to_memory(&queue_key, inserted_job.clone())
            .await;

        // If both fail, the job is persisted but not actively queued
        // It will be picked up by the Postgres fallback in dequeue
        eprintln!(
            "⚠️ Job {} persisted but not actively queued - will be processed via Postgres fallback",
            inserted_job.id
        );
        Ok(inserted_job)
    }

    // pub async fn dequeue(&self, queue_name: &str) -> Result<Option<Job>, StackDuckError> {
    //     let key = format!("Stackduck:queue:{}", queue_name);

    //     // Try Redis first
    //     if let Some(client) = &self.redis_pool {
    //         match client.get_redis_client().await {
    //             Ok(mut conn) => {
    //                 let now = Utc::now().timestamp() as f64;
    //                 let processing_score = (Utc::now().timestamp() + 1800) as f64;
    //                 let redis_result: Vec<String> = conn
    //                     .zrangebyscore_limit(&key, "-inf", &now.to_string(), 0, 1)
    //                     .await?;
    //                 if let Some(job_json) = redis_result.first() {
    //                     let job: Job = serde_json::from_str(job_json)?;

    //                     // remove from queue
    //                     let _: () = conn.zrem(&key, &job_json).await?;
    //                     // Update status to running
    //                     let job_id = job.id.to_string();
    //                     let _: () = conn
    //                         .zadd(
    //                             "Stackduck:running",
    //                             &format!("{}:{}", job_id, job_json),
    //                             processing_score,
    //                         )
    //                         .await?;

    //                     // update redis status to running.
    //                     let updates = vec![
    //                         ("status".to_string(), JobStatus::Running.to_string()),
    //                         ("started_at".to_string(), chrono::Utc::now().to_rfc3339()),
    //                     ];
    //                     let job_idd = job_id.clone();
    //                     let _ = self
    //                         .update_redis_job_property(&mut conn, job_idd, &updates)
    //                         .await;

    //                     //update postgres
    //                     self.update_dequeue_job_status(&job_id, JobStatus::Running)
    //                         .await?;

    //                     return Ok(Some(job));
    //                 };
    //             }
    //             Err(e) => {
    //                 eprintln!("Redis unavailable: {}. Falling back to in-memory queue.", e);
    //             }
    //         }
    //     }

    //     // Try in-memory queue
    //     let mut queues = self.in_memory_queue.lock().await;
    //     if let Some(queue) = queues.get_mut(&key) {
    //         if let Some(job) = queue.pop_front() {
    //             // Drop the lock before the async operation
    //             drop(queues);

    //             // Update status to processing
    //             self.update_dequeue_job_status(&job.id.to_string(), JobStatus::Running)
    //                 .await?;
    //             return Ok(Some(job));
    //         }
    //     }

    //     let job = self.update_postgres_dequeue_fallback(queue_name).await;

    //     Ok(job)
    // }

    pub async fn dequeue(&self, queue_name: &str) -> Result<Option<Job>, StackDuckError> {
        let key = format!("Stackduck:queue:{}", queue_name);

        // Try Redis first
        if let Some(client) = &self.redis_pool {
            match client.get_redis_client().await {
                Ok(mut conn) => {
                    let now = Utc::now().timestamp() as f64;
                    let processing_score = (Utc::now().timestamp() + 1800) as f64;
                    println!("🔍 Dequeued job logic running.");

                    // Use Lua script for atomic dequeue operation
                    let script_result: Option<String> = Script::new(DEQUEUE_SCRIPT)
                        .key(&key)
                        .key("Stackduck:running")
                        .arg(&now.to_string())
                        .arg(&processing_score.to_string())
                        .invoke_async(&mut *conn)
                        .await?;

                    if let Some(job_json) = script_result {
                        let job: Job = serde_json::from_str(&job_json)?;
                        let job_id = job.id.to_string();

                        // Update redis cache status to running
                        let updates = vec![
                            ("status".to_string(), JobStatus::Running.to_string()),
                            ("started_at".to_string(), chrono::Utc::now().to_rfc3339()),
                        ];
                        let _ = self
                            .update_redis_job_property(&mut conn, job_id.clone(), &updates)
                            .await;
                        // Update postgres
                        let _ = self
                            .update_dequeue_job_status(&job_id, JobStatus::Running)
                            .await;

                        println!("🔍 Dequeued job in Dequeue logic: {:?}", job);

                        return Ok(Some(job));
                    }
                }
                Err(e) => {
                    eprintln!("Redis unavailable: {}. Falling back to in-memory queue.", e);
                }
            }
        }

        // Try in-memory queue
        let mut queues = self.in_memory_queue.lock().await;
        if let Some(queue) = queues.get_mut(&key) {
            if let Some(job) = queue.pop_front() {
                // Drop the lock before the async operation
                drop(queues);

                // Update status to processing
                self.update_dequeue_job_status(&job.id.to_string(), JobStatus::Running)
                    .await?;
                return Ok(Some(job));
            }
        }

        let job = self.update_postgres_dequeue_fallback(queue_name).await;

        Ok(job)
    }

    pub async fn ack_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(&job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;
        // Update in Postgres
        sqlx::query(
            "UPDATE jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW() WHERE id = $1"
        )
        .bind(uuid)
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to mark job as completed: {}", e)))?;

        // Update in Redis if available
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                match conn
                    .zrange::<_, Vec<String>>("Stackduck:running", 0, -1)
                    .await
                {
                    Ok(running_jobs) => {
                        let job_prefix = format!("{}:", job_id);
                        for entry in running_jobs {
                            if entry.starts_with(&job_prefix) {
                                let _: () = conn.zrem("Stackduck:running", &entry).await?;
                                break;
                            }
                        }

                        // Update job cache
                        let updates = vec![
                            ("status".to_string(), "completed".to_string()),
                            ("completed_at".to_string(), chrono::Utc::now().to_rfc3339()),
                            ("updated_at".to_string(), chrono::Utc::now().to_rfc3339()),
                        ];
                        let _ = self
                            .update_redis_job_property(&mut conn, job_id.to_string(), &updates)
                            .await;
                    }
                    Err(e) => {
                        return Err(StackDuckError::RedisJobError(e));
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn nack_job(&self, job_id: &str, error_message: &str) -> Result<(), StackDuckError> {
        // Get job details to check retry eligibility
        let uuid = Uuid::parse_str(&job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;
        let job = self.get_job_by_id(job_id).await?;
        sqlx::query!(
            r#"
                UPDATE jobs
                SET error_message = $1, updated_at = now()
                WHERE id = $2
                "#,
            error_message,
            uuid
        )
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to persist error message: {}", e)))?;

        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                let updates = vec![("error_message".to_string(), error_message.to_string())];
                let _ = self
                    .update_redis_job_property(&mut conn, job_id.to_string(), &updates)
                    .await;
            }
        };

        if let Some(mut job) = job {
            job.error_message = Some(error_message.to_string());
            let current_retry_count = job.retry_count.unwrap_or(0);
            let max_retries = job.max_retries.unwrap_or_default();

            if current_retry_count < max_retries {
                // Job is eligible for retry
                job.retry_count = Some(current_retry_count + 1);
                self.retry_job(job).await?;
            } else {
                // Max retries exceeded - mark as permanently failed and move to dead letter queue
                self.handle_nack(job_id, &job).await?;
            }
        } else {
            return Err(StackDuckError::JobError(format!(
                "Job {} not found",
                job_id
            )));
        }

        Ok(())
    }

    pub async fn retry_job(&self, mut job: Job) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(&job.id.to_string())
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;

        let retry_count = job.retry_count.unwrap_or(0);
        let max_retries = job.max_retries.unwrap_or(0);

        if retry_count >= max_retries {
            // return Err(StackDuckError::JobError(format!(
            //     "Retry limit exceeded for job {} (retry_count: {}, max_retries: {})",
            //     job.id, retry_count, max_retries
            // )));
            self.handle_nack(&job.id.to_string(), &job).await?;
        }

        let new_retry_count = retry_count + 1;

        // Exponential backoff (30s * 2^retry_count)
        let delay = job.delay.unwrap_or(30);
        let delay_seconds = delay * (2_i32.pow(retry_count as u32));
        let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64);

        // Update job fields
        job.status = JobStatus::Queued.to_string();
        job.scheduled_at = Some(scheduled_at);
        job.started_at = None;

        // Update in Postgres
        sqlx::query(
            r#"
        UPDATE jobs 
        SET status = 'Queued', 
            retry_count = $2, 
            scheduled_at = $3,
            started_at = NULL,
            updated_at = NOW()
        WHERE id = $1
        "#,
        )
        .bind(uuid)
        .bind(new_retry_count)
        .bind(scheduled_at)
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to update job for retry: {}", e)))?;

        // Re-enqueue in Redis with scheduled time
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                // Remove from running queue first
                let running_jobs: Vec<String> = conn
                    .zrange("Stackduck:running", 0, -1)
                    .await
                    .unwrap_or_default();

                for entry in running_jobs {
                    if entry.starts_with(&format!("{}:", job.id.to_string())) {
                        let _: () = conn.zrem("Stackduck:running", &entry).await?;
                        break;
                    }
                }

                // Add back to queue with new scheduled time
                let queue_key = format!("Stackduck:queue:{}", job.job_type);
                let job_json = serde_json::to_string(&job)?;
                let score = scheduled_at.timestamp() as f64;

                let _: Result<i32, _> = conn.zadd(&queue_key, &job_json, score).await;

                // Update job cache
                let cache_key = format!("Stackduck:job:{}", job.id);
                let _: Result<String, _> = conn.set_ex(&cache_key, &job_json, 3600).await;
            }
        }

        Ok(())
    }

    // HELPER METHODS
    pub async fn update_dequeue_job_status(
        &self,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), StackDuckError> {
        // 1. Update Postgres
        sqlx::query(
            "UPDATE jobs SET status = $1, started_at = NOW(), updated_at = NOW() WHERE id = $2",
        )
        .bind(&new_status.to_string())
        .bind(job_id)
        .execute(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to update job status in Postgres: {}", e))
        })?;

        // 2. Update in Redis cache if available
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get_redis_client().await {
                let updated_time = Utc::now().to_rfc3339();
                self.update_redis_job_property(
                    &mut conn,
                    job_id.to_string(),
                    &[
                        ("status".to_string(), new_status.to_string()),
                        ("updated_at".to_string(), updated_time),
                    ],
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_nack(&self, job_id: &str, job: &Job) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(&job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;
        // Update in Postgres with failed status and failed_at timestamp
        sqlx::query(
        "UPDATE jobs SET status = 'failed', completed_at = NOW(), updated_at = NOW() WHERE id = $1"
    )
    .bind(uuid)
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
                    .map_err(|e| StackDuckError::RedisJobError(e))?;
                for entry in running_jobs {
                    if entry.starts_with(&format!("{}:", job_id)) {
                        let _: () = conn.zrem("Stackduck:running", &entry).await?;
                        break;
                    }
                }

                let job_clone = job.clone();
                let error_message = job_clone
                    .error_message
                    .unwrap_or("No error message".to_string());

                let reason = format!(
                    "Max retries ({}) exceeded with error: {}",
                    job.max_retries.unwrap_or(0),
                    error_message
                );

                // Create dead letter queue entry with full context
                let dead_letter_entry = serde_json::json!({
                    "job_id": job_id,
                    "job_type": job.job_type,
                    "payload": job.payload,
                    "retry_count": job.retry_count.unwrap_or(0),
                    "max_retries": job.max_retries.unwrap_or(3),
                    "delay": job.delay.unwrap_or(30),
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
                    ("status".to_string(), JobStatus::Failed.to_string()),
                    ("failed_at".to_string(), chrono::Utc::now().to_rfc3339()),
                    ("updated_at".to_string(), chrono::Utc::now().to_rfc3339()),
                ];
                let _ = self
                    .update_redis_job_property(&mut conn, job_id.to_string(), &updates)
                    .await;
            }
        }

        Ok(())
    }

    pub async fn update_redis_job_property(
        &self,
        conn: &mut Connection,
        job_id: String,
        values: &[(String, String)],
    ) -> Result<Job, StackDuckError> {
        let cache_key = format!("Stackduck:job:{}", job_id);

        println!(
            "🔍 Updating job {} in Redis cache with values: {:?}",
            job_id, values
        );

        // Fetch current job JSON from Redis
        let job_json: Option<String> = conn.get(&cache_key).await?;

        println!("🔍 Current job JSON in Redis: {:?}", job_json);

        let mut job: Job = match job_json {
            Some(json_str) => serde_json::from_str(&json_str).map_err(|e| {
                StackDuckError::JobError(format!("Failed to parse Job JSON: {}", e))
            })?,
            None => {
                return Err(StackDuckError::JobError(format!(
                    "Job {} not found",
                    job_id
                )));
            }
        };

        println!("🔍 Job before update: {:?}", job);

        // Convert Job to serde_json::Value to apply updates
        let mut job_value = serde_json::to_value(job).map_err(|e| {
            StackDuckError::JobError(format!("Failed to convert Job to Value: {}", e))
        })?;

        let obj = job_value.as_object_mut().ok_or_else(|| {
            StackDuckError::JobError("Failed to convert Job JSON to object".to_string())
        })?;

        // Apply field updates
        for (key, value) in values {
            obj.insert(key.to_string(), serde_json::Value::String(value.clone()));
        }

        // Deserialize back to Job
        job = serde_json::from_value(job_value).map_err(|e| {
            StackDuckError::JobError(format!("Failed to re-parse updated Job: {}", e))
        })?;

        // Store back in Redis with TTL (e.g., 1 hour = 3600 seconds)
        let new_job_json = serde_json::to_string(&job).map_err(|e| {
            StackDuckError::JobError(format!("Failed to serialize updated Job: {}", e))
        })?;

        println!("🔍 Job after update: {:?}", job);

        let ttl = 3600 + (job.retry_count.unwrap_or_default() * 1800) as u64;
        let _: () = conn
            .set_ex(&cache_key, new_job_json, ttl)
            .await
            .map_err(|e| StackDuckError::RedisJobError(e))?;
        Ok(job)
    }

    pub async fn get_job_by_id(&self, job_id: &str) -> Result<Option<Job>, StackDuckError> {
        let uuid = Uuid::parse_str(&job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;
        // Try Redis cache first if available
        if let Some(client) = &self.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                let cache_key = format!("Stackduck:job:{}", job_id);
                if let Ok(Some(job_json)) = conn.get::<String, Option<String>>(cache_key).await {
                    if let Ok(job) = serde_json::from_str::<Job>(&job_json) {
                        return Ok(Some(job));
                    }
                }
            }
        }

        // Fallback to Postgres
        let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
            .bind(uuid)
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::JobError(format!("Failed to fetch job: {}", e)))?;

        // Cache the result in Redis for future lookups
        if let Some(ref job) = job {
            if let Some(client) = &self.redis_pool {
                if let Ok(mut conn) = client.get_redis_client().await {
                    let cache_key = format!("Stackduck:job:{}", job_id);
                    let job_json = serde_json::to_string(job).unwrap_or_default();
                    let _: Result<String, _> = conn.set_ex(&cache_key, &job_json, 3600).await;
                    // Cache for 1 hour
                }
            }
        }

        Ok(job)
    }

    pub async fn update_postgres_dequeue_fallback(&self, queue_name: &str) -> Option<Job> {
        // Fallback to Postgres query with atomic status update
        let job = sqlx::query_as::<_, Job>(
            r#"
    UPDATE jobs 
    SET status = 'running', 
        started_at = NOW(),
        updated_at = NOW()
    WHERE id = (
        SELECT id FROM jobs
        WHERE job_type = $1
          AND status = 'Queued'
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
        .map_err(|e| StackDuckError::JobError(format!("Postgres query failed: {}", e)));

        match job {
            Ok(Some(job)) => Some(job),
            Ok(None) => None,
            Err(e) => {
                eprintln!("Failed to fetch job from Postgres: {}", e);
                None
            }
        }
    }

    async fn fallback_to_memory(&self, queue_key: &str, job: Job) -> Result<(), StackDuckError> {
        let mut queues = self.in_memory_queue.lock().await;

        queues
            .entry(queue_key.to_string())
            .or_insert_with(VecDeque::new)
            .push_back(job);

        Ok(())
    }
}
