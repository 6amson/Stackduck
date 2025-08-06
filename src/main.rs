use stackduck::error::StackDuckError;
use stackduck::grpc::StackduckGrpcService;
use stackduck::types::JobManager;
use stackduck::{stackduck::stack_duck_service_server::StackDuckServiceServer, StackDuck};
use tokio::signal;
use tokio::sync::Mutex;
use tonic::transport::Server;

use dotenvy;
use std::collections::HashMap;
use std::sync::Arc;

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
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
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
        .serve_with_shutdown(server_addr, shutdown_signal())
        .await
        .map_err(|e| StackDuckError::ServerError(format!("gRPC server failed: {}", e)))?;

    Ok(())
}

async fn shutdown_signal() {
    if cfg!(unix) {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");

        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("🛑 Ctrl+C received. Initiating shutdown...");
            }
            _ = sigterm.recv() => {
                println!("🛑 SIGTERM received. Initiating shutdown...");
            }
        }
    } else {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
        println!("🛑 Ctrl+C received. Initiating shutdown...");
    }
}


