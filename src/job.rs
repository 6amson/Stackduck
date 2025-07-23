use crate::{
    db::postgres::DbPool,
    types::{Job, JobStatus,JobManager},
    error::StackDuckError,
};
use sqlx::{pool::PoolConnection, prelude::*, Postgres};
use uuid::Uuid;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
// use deadpool_redis as redis;
use deadpool_redis::redis::AsyncCommands;


impl JobManager{

    async fn get_postgres_pool (&self) -> Result<PoolConnection<Postgres>, StackDuckError>{
    let mut conn = self.db_pool.acquire().await
    .map_err(|e| StackDuckError::DbConnectionError(e.to_string()));
conn
    }

    pub async fn enqueue_job(&self, job: Job) -> Result<Job, StackDuckError>{
    //save to postgres
    let postgres_pool = self.get_postgres_pool().await?;
    let payload_json = serde_json::to_string(&job.payload)?; 
      let insert_query = sqlx::query(
            "INSERT INTO jobs (id, job_type, payload, status) VALUES ($1, $2, $3, $4)",
        )
        .bind(&job.id)
        .bind(&job.job_type)
        .bind(payload_json)
        .bind(&job.status);

        let conn = postgres_pool.acquire().await?;
        insert_query.execute(&conn).await
        .map_err(|e| StackDuckError::JobError(e.to_string()))?;

        if let Some(redis_client) = &self.redis_pool{
            let mut conn = redis_client.get_redis_client().await?;
            let key = format!("Stackduck:queue:{}", job.job_type);
            let job_json = serde_json::to_string(&job)?;
            conn.lpush(&key, job_json).await?;
        } else {
            let mut queue = self.in_memory_queue.lock().unwrap();
            queue.push_back(job);
            println!("Redis unavailable. Job pushed to in-memeory.")
        }

    Ok(())
};

pub async fn fetch_next (&self) -> Result<Option<Job>, StackDuckError>{
    // let row = sqlx::query_as!()
} 

}