use stackduck::error::StackDuckError;
use stackduck::grpc::StackduckGrpcService;
use stackduck::types::JobManager;
use stackduck::{stackduck::stack_duck_service_server::StackDuckServiceServer, StackDuck};
use tokio::signal;
use tokio::sync::RwLock;
use tonic::transport::Server;

use std::collections::HashSet;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), StackDuckError> {
    // Load environment variables
    dotenvy::dotenv().ok();

    println!("🦆 Starting StackDuck Job Queue System...");

    // Get configuration from environment
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable is required");
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL environment variable is required");
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()
        .expect("Invalid SERVER_ADDR format");

    let stackduck = StackDuck::new(&database_url, &redis_url).await?;

    // Run database migrations
    println!("🔄 Running database migrations...");
    stackduck.run_migrations().await?;

    // Create gRPC service
    let job_manager = Arc::new(JobManager {
        db_pool: stackduck.db_pool.clone(),
        redis_pool: stackduck.redis_client.clone(),
        subscribed_job_types: Arc::new(RwLock::new(HashSet::new())),
    });
    let grpc_service = StackduckGrpcService::new(job_manager.clone());

    // Spawn the poll loop in the background
    let poll_service = grpc_service.clone();
    tokio::spawn(async move {
        poll_service.start_scheduled_job_poller().await;
    });

    // Start gRPC server
    println!("🚀 Starting gRPC server on {}", server_addr);

    {
    let mut subscribed = grpc_service.job_manager.subscribed_job_types.write().await;
    subscribed.insert("test_job".to_string());
    subscribed.insert("send_email".to_string());
}

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
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        println!("🛑 Ctrl+C received. Initiating shutdown...");
    }
}
