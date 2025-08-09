use crate::stackduck::GrpcJob;
use crate::{
    error::StackDuckError,
    types::{Job, JobManager, JobStatus, DEQUEUE_SCRIPT},
};
use chrono::Utc;
use deadpool_redis::{redis::AsyncCommands, Connection};
use redis::Script;
use std::vec;
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

impl From<Job> for GrpcJob {
    fn from(job: Job) -> Self {
        Self {
            id: job.id.to_string(),
            job_type: job.job_type,
            payload: job.payload.to_string(),
            status: job.status,
            priority: job.priority.unwrap_or_default(),
            retry_count: job.retry_count.unwrap_or_default(),
            max_retries: job.max_retries.unwrap_or_default(),
            error_message: job.error_message.unwrap_or_default(),
            delay: job.delay.unwrap_or_default(),
            scheduled_at: job
                .scheduled_at
                .map(|dt| dt.timestamp())
                .unwrap_or_default(),
            started_at: job.started_at.map(|dt| dt.timestamp()).unwrap_or_default(),
            completed_at: job
                .completed_at
                .map(|dt| dt.timestamp())
                .unwrap_or_default(),
            created_at: job.created_at.map(|dt| dt.timestamp()).unwrap_or_default(),
            updated_at: job.updated_at.map(|dt| dt.timestamp()).unwrap_or_default(),
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
        .bind(work.id)
        .bind(work.job_type)
        .bind(work.payload)
        .bind(JobStatus::Queued.to_string())
        .bind(work.priority)
        .bind(work.retry_count)
        .bind(work.error_message)
        .bind(work.delay)
        .bind(work.max_retries)
        .bind(work.scheduled_at)
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to insert job into Postgres: {}", e))
        })?;

        //  Enqueue to active queue systems only
        let queue_key = format!("Stackduck:queue:{}", inserted_job.job_type);
        let job_json = serde_json::to_string(&inserted_job)?;

        // Try Redis first
        let mut conn = self.redis_pool.get_redis_client().await?;
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
        //     }
        // }

        eprintln!(
            "⚠️ Job {} persisted but not actively queued - will be processed via Postgres fallback",
            inserted_job.id
        );
        Ok(inserted_job)
    }

    pub async fn dequeue(
        &self,
        queue_name: &str,
        worker_id: Option<&str>,
    ) -> Result<Option<Job>, StackDuckError> {
        let worker_id = worker_id.expect("Should have worker id");

        // Only Redis - no fallbacks, no race conditions
        let mut conn = self.redis_pool.get_redis_client().await?;
        let script_result: Option<String> = Script::new(DEQUEUE_SCRIPT)
            .key(format!("Stackduck:queue:{}", queue_name))
            .key("Stackduck:running")
            .key(format!("lock:dequeue:{}", queue_name))
            .arg(worker_id)
            .arg(Utc::now().timestamp().to_string())
            .arg((Utc::now().timestamp() + 1800).to_string())
            .arg("1000")
            .invoke_async(&mut *conn)
            .await?;

        if let Some(job_json) = script_result {
            let job: Job = serde_json::from_str(&job_json)?;
            let job_id = job.id.to_string();

            println!("✅ Worker {} got job from REDIS: {}", worker_id, job.id);

            let updates = vec![
                ("status".to_string(), JobStatus::Running.to_string()),
                ("started_at".to_string(), chrono::Utc::now().to_rfc3339()),
            ];
            // Update Postgres for persistence (async, non-blocking)
            let _ = self
                .update_postgres_dequeue_fallback(queue_name, worker_id)
                .await;
            // let _ = self
            //     .update_dequeue_job_status(&job.id.to_string(), JobStatus::Running)
            //     .await;
            let _ = self
                .update_redis_job_property(&mut conn, job_id.clone(), &updates)
                .await;
            return Ok(Some(job));
        }

        Ok(None) // No job available
    }

    pub async fn ack_job(&self, job_id: &str) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;

        // Redis is required - get connection first
        let mut conn = self.redis_pool.get_redis_client().await?;

        // Remove from Redis running set
        let running_jobs: Vec<String> = conn
            .zrange("Stackduck:running", 0, -1)
            .await
            .map_err(StackDuckError::RedisJobError)?;

        let job_prefix = format!("{}:", job_id);
        for entry in running_jobs {
            if entry.starts_with(&job_prefix) {
                let _: () = conn.zrem("Stackduck:running", &[&entry]).await?;
                //
                break;
            }
        }

        // Update Redis job cache
        let updates = vec![
            ("status".to_string(), "completed".to_string()),
            ("completed_at".to_string(), chrono::Utc::now().to_rfc3339()),
            ("updated_at".to_string(), chrono::Utc::now().to_rfc3339()),
        ];
        self.update_redis_job_property(&mut conn, job_id.to_string(), &updates)
            .await?;

        // Update Postgres for persistence
        sqlx::query(
        "UPDATE jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW() WHERE id = $1"
    )
    .bind(uuid)
    .execute(&self.db_pool)
    .await
    .map_err(|e| StackDuckError::JobError(format!("Failed to mark job as completed: {}", e)))?;

        Ok(())
    }

    pub async fn retry_job(&self, mut job: Job, error_message: &str) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(&job.id.to_string())
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;

        let retry_count = job.retry_count.unwrap_or_default();
        // let max_retries = job.max_retries.unwrap_or_default();

        let new_retry_count = retry_count + 1;

        // Exponential backoff (30s * 2^retry_count)

        let delay = job.delay.unwrap_or_default();
        let max_delay = 3600; // 1 hour
        let raw_delay = delay * (2_i32.pow(retry_count as u32));
        let delay_seconds = raw_delay.min(max_delay);
        let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64);

        // Update job fields
        job.status = JobStatus::Queued.to_string();
        job.scheduled_at = Some(scheduled_at);
        job.started_at = None;
        job.retry_count = Some(new_retry_count);
        job.error_message = Some(error_message.to_string());

        // Update in Postgres
        sqlx::query(
            r#"
        UPDATE jobs 
        SET status = 'Queued', 
            retry_count = $2, 
            scheduled_at = $3,
            error_message = $4,
            started_at = NULL,
            updated_at = NOW()
        WHERE id = $1
        "#,
        )
        .bind(uuid)
        .bind(new_retry_count)
        .bind(scheduled_at)
        .bind(error_message)
        .execute(&self.db_pool)
        .await
        .map_err(|e| StackDuckError::JobError(format!("Failed to update job for retry: {}", e)))?;

        // Re-enqueue in Redis with scheduled time
        let mut conn = self.redis_pool.get_redis_client().await?;
        // Remove from running queue first
        let running_jobs: Vec<String> = conn
            .zrange("Stackduck:running", 0, -1)
            .await
            .unwrap_or_default();

        for entry in running_jobs {
            if entry.starts_with(&format!("{}:", job.id)) {
                let _: () = conn.zrem("Stackduck:running", &entry).await?;
                break;
            }
        }

        // Add back to queue with new scheduled time
        let queue_key = format!("Stackduck:queue:{}", job.job_type);
        let job_json = serde_json::to_string(&job)?;

        let priority = job.priority.unwrap_or(2);

        let base_timestamp = job.scheduled_at.unwrap_or_else(chrono::Utc::now).timestamp() as f64;

        // Reverse priority scoring so lower numbers = higher priority
        // Priority 1 (high) = 0, Priority 2 (medium) = 1, Priority 3 (low) = 2
        let priority_offset = (3 - priority) as f64;

        // Use a much smaller multiplier to avoid overwhelming the timestamp
        // This gives us priority differences of 1000 seconds (16 minutes)
        let score = base_timestamp + (priority_offset * 1000.0);

        let _: Result<i32, _> = conn.zadd(&queue_key, &job_json, score).await;

        // Update job cache
        let cache_key = format!("Stackduck:job:{}", job.id);
        let _: Result<String, _> = conn.set_ex(&cache_key, &job_json, 3600).await;
        println!(
            "Job from queue {} and with ID {} re-enqueued for retry",
            job.job_type, job.id
        );

        Ok(())
    }

    // HELPER METHODS
    pub async fn get_due_jobs_sequentially(
        &self,
        job_types: &[&str],
    ) -> Result<Option<Job>, StackDuckError> {
        let mut conn = self.redis_pool.get_redis_client().await?;

        for job_type in job_types {
            let queue_key = format!("Stackduck:queue:{}", job_type);

            // Get jobs whose score (scheduled_at) <= now
            let due_jobs: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&queue_key)
                .arg("-inf") // min score
                .arg(chrono::Utc::now().timestamp()) // max score = now
                .arg("LIMIT")
                .arg(0) // offset
                .arg(10) // count
                .query_async(&mut conn)
                .await?;

            if let Some(job_json) = due_jobs.into_iter().next() {
                let job: Job = serde_json::from_str(&job_json)?;
                return Ok(Some(job));
            }
        }

        Ok(None)
    }

    pub async fn update_dequeue_job_status(
        &self,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), StackDuckError> {
        // 1. Update Postgres
        sqlx::query(
            "UPDATE jobs SET status = $1, started_at = NOW(), updated_at = NOW() WHERE id = $2",
        )
        .bind(new_status.to_string())
        .bind(job_id)
        .execute(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to update job status in Postgres: {}", e))
        })?;

        // 2. Update in Redis cache if available
        let mut conn = self.redis_pool.get_redis_client().await?;
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

        Ok(())
    }

    pub async fn nack_job(&self, job_id: &str, error_message: &str) -> Result<(), StackDuckError> {
        let uuid = Uuid::parse_str(job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;

        let job: Job = sqlx::query_as::<_, Job>(
            "UPDATE jobs 
     SET error_message = $1, 
         status = 'failed', 
         completed_at = NOW(), 
         updated_at = NOW() 
     WHERE id = $2 
     RETURNING *",
        )
        .bind(error_message)
        .bind(uuid)
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| {
            StackDuckError::JobError(format!("Failed to mark job as permanently failed: {}", e))
        })?;

        // Remove from Redis running queue and move to dead letter queue
        let mut conn = self.redis_pool.get_redis_client().await?;
        // Remove from running queue (if it exists there)
        let running_jobs: Vec<String> = conn
            .zrange("Stackduck:running", 0, -1)
            .await
            .map_err(StackDuckError::RedisJobError)?;
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
        println!("Nacked this job: {:?}", job);

        Ok(())
    }

    pub async fn update_redis_job_property(
        &self,
        conn: &mut Connection,
        job_id: String,
        values: &[(String, String)],
    ) -> Result<Job, StackDuckError> {
        let cache_key = format!("Stackduck:job:{}", job_id);

        // println!(
        //     "🔍 Updating job {} in Redis cache with values: {:?}",
        //     job_id, values
        // );

        // Fetch current job JSON from Redis
        let job_json: Option<String> = conn.get(&cache_key).await?;

        // println!("🔍 Current job JSON in Redis: {:?}", job_json);

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

        // println!("🔍 Job before update: {:?}", job);

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

        // println!("🔍 Job after update: {:?}", job);

        let ttl = 3600 + (job.retry_count.unwrap_or_default() * 1800) as u64;
        let _: () = conn
            .set_ex(&cache_key, new_job_json, ttl)
            .await
            .map_err(StackDuckError::RedisJobError)?;
        Ok(job)
    }

    pub async fn get_job_by_id(&self, job_id: &str) -> Result<Option<Job>, StackDuckError> {
        let uuid = Uuid::parse_str(job_id)
            .map_err(|e| StackDuckError::JobError(format!("Invalid job ID: {}", e)))?;
        // Try Redis cache first if available
        let mut conn = self.redis_pool.get_redis_client().await?;
        let cache_key = format!("Stackduck:job:{}", job_id);
        if let Ok(Some(job_json)) = conn.get::<String, Option<String>>(cache_key).await {
            if let Ok(job) = serde_json::from_str::<Job>(&job_json) {
                return Ok(Some(job));
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
            let mut conn = self.redis_pool.get_redis_client().await?;
            let cache_key = format!("Stackduck:job:{}", job_id);
            let job_json = serde_json::to_string(job).unwrap_or_default();
            let _: Result<String, _> = conn.set_ex(&cache_key, &job_json, 3600).await;
            // Cache for 1 hour
        }

        println!("Got this job man job: {:?}", &job);

        Ok(job)
    }

    pub async fn update_postgres_dequeue_fallback(
        &self,
        queue_name: &str,
        worker_id: &str,
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
            Ok(Some(job)) => {
                println!(
                    "✅ Worker {} updated job on POSTGRES: {}",
                    worker_id, job.id
                );
                Some(job)
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!("Failed to fetch job from Postgres: {}", e);
                None
            }
        }
    }
}
