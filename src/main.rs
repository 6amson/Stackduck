pub mod db;
pub mod error;
pub mod job;
pub mod types;
pub mod stackduck {
    tonic::include_proto!("stackduck");
}
pub mod queue;

use std::collections::{HashMap,};
use tokio::sync::Mutex;
use crate::db::postgres::{connect_to_db, DbPool};
use crate::db::redis::connect_to_redis;
use crate::error::StackDuckError;
use crate::types::RedisClient;
use crate::types::JobManager;
use deadpool_redis;
use sqlx::{pool::PoolConnection, Postgres};
use stackduck::stack_duck_service_server::StackDuckServiceServer;
use crate::queue::StackduckGrpcService;
use std::sync::Arc;
use tonic::transport::Server;

pub struct StackDuck {
    pub db_pool: DbPool,
    pub redis_client: Option<RedisClient>,
}

#[tokio::main]
async fn main() -> Result<(), StackDuckError> {
    // Load environment variables
    dotenvy::dotenv().ok();

    println!("🦆 Starting StackDuck Job Queue System...");

    // Get configuration from environment
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable is required");
    let redis_url = std::env::var("REDIS_URL").ok();
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "[::1]:50051".to_string())
        .parse()
        .expect("Invalid SERVER_ADDR format");

    // Initialize StackDuck
    let stackduck = match redis_url {
        Some(redis_url) => {
            println!("📡 Initializing with Redis support...");
            StackDuck::new_with_redis(&database_url, &redis_url).await?
        }
        None => {
            println!("⚠️  Redis URL not provided, using Postgres + in-memory fallback");
            StackDuck::new(&database_url).await?
        }
    };

    // Run database migrations
    println!("🔄 Running database migrations...");
    stackduck.run_migrations().await?;

    // Create gRPC service
    let job_manager = Arc::new(JobManager {
        db_pool: stackduck.db_pool.clone(),
        redis_pool: stackduck.redis_client.clone(),
        in_memory_queue: Mutex::new(HashMap::new()),
    });
    let grpc_service = StackduckGrpcService::new(job_manager);

    // Start gRPC server
    println!("🚀 Starting gRPC server on {}", server_addr);

    Server::builder()
        .add_service(StackDuckServiceServer::new(grpc_service))
        .serve(server_addr)
        .await
        .map_err(|e| StackDuckError::ServerError(format!("gRPC server failed: {}", e)))?;

    Ok(())
}

impl StackDuck {
    pub async fn new(database_url: &str) -> Result<Self, StackDuckError> {
        let db_pool = connect_to_db(database_url).await?;
        Ok(Self {
            db_pool,
            redis_client: None,
        })
    }

    pub async fn new_with_redis(
        database_url: &str,
        redis_url: &str,
    ) -> Result<Self, StackDuckError> {
        let db_pool = connect_to_db(database_url).await?;
        let redis_client = connect_to_redis(redis_url)
            .await
            .map_err(|err| StackDuckError::JobError(err.to_string()))?;
        Ok(Self {
            db_pool,
            redis_client: Some(redis_client),
        })
    }

    pub async fn run_migrations(&self) -> Result<(), StackDuckError> {
        sqlx::migrate!("./migrations")
            .run(&self.db_pool)
            .await
            .map_err(|e| StackDuckError::DbConnectionError(format!("Migration failed: {}", e)))?;
        Ok(())
    }

    pub fn has_redis(&self) -> bool {
        self.redis_client.is_some()
    }

    pub async fn get_postgres_conn(&self) -> Result<PoolConnection<Postgres>, StackDuckError> {
        self.db_pool
            .acquire()
            .await
            .map_err(|e| StackDuckError::DbConnectionError(e.to_string()))
    }

    pub async fn get_redis_conn(&self) -> Result<deadpool_redis::Connection, StackDuckError> {
        match &self.redis_client {
            Some(client) => client.get_redis_client().await,
            None => Err(StackDuckError::RedisConnectionError(
                "Redis not configured".into(),
            )),
        }
    }
}
