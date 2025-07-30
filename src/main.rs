pub mod db;
pub mod error;
pub mod job;
pub mod types;
pub mod stackduck {
    tonic::include_proto!("stackduck");
}
pub mod queue;

use crate::db::postgres::{DbPool, connect_to_db};
use crate::db::redis::connect_to_redis;
use crate::error::StackDuckError;
use crate::types::RedisClient;
use sqlx::{Postgres, pool::PoolConnection};
use deadpool_redis;

pub struct StackDuck {
    pub db_pool: DbPool,
    pub redis_client: Option<RedisClient>,
}

#[tokio::main]
async fn main() -> Result<(), StackDuckError> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Initialize logging
    // env_logger::init();

    println!("🦆 Starting StackDuck Job Queue System...");

    // Get configuration from environment
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable is required");
    let redis_url = std::env::var("REDIS_URL").ok(); // Optional Redis

    // Initialize StackDuck with or without Redis
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

    // // Test the connection
    // println!("🔍 Testing database connection...");
    // test_database_connection(&stackduck).await?;

    // // Test Redis connection if available
    // if stackduck.has_redis() {
    //     println!("🔍 Testing Redis connection...");
    //     test_redis_connection(&stackduck).await?;
    // }

    // // Test database connection
    // async fn test_database_connection(stackduck: &StackDuck) -> Result<(), StackDuckError> {
    //     let mut conn = stackduck.get_postgres_conn().await?;

    //     let result = sqlx::query("SELECT 1 as test")
    //         .fetch_one(&mut *conn)
    //         .await
    //         .map_err(|e| StackDuckError::DbConnectionError(e.to_string()))?;

    //     println!("✅ Database connection successful");
    //     Ok(())
    // }

    // // Test Redis connection
    // async fn test_redis_connection(stackduck: &StackDuck) -> Result<(), StackDuckError> {
    //     let mut conn = stackduck.get_redis_conn().await?;

    //     let _: String = deadpool_redis:: cmd("PING")
    //         .query_async(&mut conn)
    //         .await
    //         .map_err(|e| StackDuckError::RedisConnectionError(e.to_string()))?;

    //     println!("✅ Redis connection successful");
    //     Ok(())
    // }

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
            .map_err(|err| StackDuckError::JobError((err.to_string())))?;
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
