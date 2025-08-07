use futures_util::StreamExt;
use serde_json::json;
use serial_test::serial;
use stackduck::grpc::StackduckGrpcService;
use stackduck::stackduck::stack_duck_service_server::StackDuckService;
use stackduck::stackduck::{
    CompleteJobRequest, ConsumeJobsRequest, EnqueueJobRequest, FailJobRequest,
};
use stackduck::types::{JobManager, NotificationType};
use stackduck::StackDuck;

use deadpool_redis::redis::AsyncCommands;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tonic::Request;

mod consume_test_utils {
    use stackduck::error::StackDuckError;

    use super::*;

    pub async fn setup_grpc_service() -> StackduckGrpcService {
        dotenvy::dotenv().ok();

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://stackduck_test:test_password@localhost:5433/stackduck_test".to_string()
        });
        println!("🧪 Using DB URL: {}", db_url);
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6380".to_string());

        let stackduck = StackDuck::new(&db_url, &redis_url)
            .await
            .expect("Failed to setup test database");

        stackduck
            .run_migrations()
            .await
            .expect("Failed to run migrations");

        let job_manager = Arc::new(JobManager {
            db_pool: stackduck.db_pool.clone(),
            redis_pool: stackduck.redis_client.clone(),
        });

        StackduckGrpcService::new(job_manager)
    }

    pub async fn cleanup_service_data(
        service: &StackduckGrpcService,
    ) -> Result<(), StackDuckError> {
        sqlx::query("TRUNCATE TABLE jobs CASCADE")
            .execute(&service.job_manager.db_pool)
            .await
            .unwrap();

        let mut conn = service.job_manager.redis_pool.get_redis_client().await?;
        let _: () = conn
            .flushdb()
            .await
            .map_err(|e| StackDuckError::RedisJobError(e))?;
        Ok(())
    }

    pub async fn enqueue_test_job(service: &StackduckGrpcService, job_type: &str) -> String {
        let request = Request::new(EnqueueJobRequest {
            job_type: job_type.to_string(),
            payload: json!({"test": "data"}).to_string(),
            priority: 2,
            scheduled_at: "2025-08-04T21:00:00Z".to_string(),
            max_retries: 2,
            delay: 30,
        });

        let response = service.enqueue_job(request).await.unwrap();
        response.into_inner().job_id
    }
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_existing_jobs() {
    let service = consume_test_utils::setup_grpc_service().await;
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Pre-enqueue some jobs BEFORE starting consume stream
    let job_id1 = consume_test_utils::enqueue_test_job(&service, "test_queue").await;
    let job_id2 = consume_test_utils::enqueue_test_job(&service, "test_queue").await;
    let _job_id3 = consume_test_utils::enqueue_test_job(&service, "other_queue").await; // Different queue

    // Start consuming only "test_queue"
    let request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response = service.consume_jobs(request).await.unwrap();
    let mut stream = response.into_inner();

    println!("🧪 Starting consume stream...");
    // println!("Response: {:?}", response);

    // Should immediately get the 2 existing jobs
    let mut received_jobs = Vec::new();

    // Use timeout to avoid hanging if no jobs come
    for _ in 0..2 {
        match timeout(Duration::from_secs(25), stream.next()).await {
            Ok(Some(Ok(job_message))) => {
                let job = job_message.job.unwrap();
                println!("🧪 Received job: {:?}", job);
                received_jobs.push(job.id);
                assert_eq!(job.job_type, "test_queue");
                assert_eq!(
                    job_message.notification_type,
                    NotificationType::ExistingJob.to_string()
                );
            }
            Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
            Ok(None) => break, // Stream ended
            Err(_) => panic!("Timeout waiting for job"),
        }
    }

    assert_eq!(received_jobs.len(), 2);
    assert!(received_jobs.contains(&job_id1));
    assert!(received_jobs.contains(&job_id2));
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_multiple_job_types() {
    let service = consume_test_utils::setup_grpc_service().await;
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Pre-enqueue jobs of different types
    let _job_id1 = consume_test_utils::enqueue_test_job(&service, "type_a").await;
    let _job_id2 = consume_test_utils::enqueue_test_job(&service, "type_b").await;
    let _job_id3 = consume_test_utils::enqueue_test_job(&service, "type_c").await; // Not subscribed

    // Subscribe to multiple job types
    let request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["type_a".to_string(), "type_b".to_string()],
    });

    let response = service.consume_jobs(request).await.unwrap();
    let mut stream = response.into_inner();

    let mut received_types = Vec::new();

    // Should get jobs from both subscribed types
    for _ in 0..2 {
        match timeout(Duration::from_secs(25), stream.next()).await {
            Ok(Some(Ok(job_message))) => {
                let job = job_message.job.unwrap();
                received_types.push(job.job_type);
            }
            Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
            Ok(None) => break,
            Err(_) => panic!("Timeout waiting for job"),
        }
    }

    println!("Received types: {:?}", received_types);

    assert_eq!(received_types.len(), 2);
    assert!(received_types.contains(&"type_a".to_string()));
    assert!(received_types.contains(&"type_b".to_string()));
    // Should NOT contain type_c
    assert!(!received_types.contains(&"type_c".to_string()));
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_multiple_workers_compete() {
    let service = Arc::new(consume_test_utils::setup_grpc_service().await);
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Pre-enqueue one job
    let _job_id = consume_test_utils::enqueue_test_job(&service, "test_queue").await;

    // Start two competing workers
    let request1 = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let request2 = Request::new(ConsumeJobsRequest {
        worker_id: "worker2".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response1 = service.consume_jobs(request1).await.unwrap();
    let response2 = service.consume_jobs(request2).await.unwrap();

    let mut stream1 = response1.into_inner();
    let mut stream2 = response2.into_inner();

    // Race to get the job
    let (result1, result2) = tokio::join!(
        timeout(Duration::from_secs(2), stream1.next()),
        timeout(Duration::from_secs(2), stream2.next())
    );

    // Only one worker should get the job
    let mut successful_workers = 0;

    if let Ok(Some(Ok(_))) = result1 {
        successful_workers += 1;
    }
    if let Ok(Some(Ok(_))) = result2 {
        successful_workers += 1;
    }

    assert_eq!(successful_workers, 1, "Only one worker should get the job");
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_empty_initially() {
    let service = consume_test_utils::setup_grpc_service().await;
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Start consuming with no existing jobs
    let request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["empty_queue".to_string()],
    });

    let response = service.consume_jobs(request).await.unwrap();
    let mut stream = response.into_inner();

    // Should not receive anything initially
    match timeout(Duration::from_millis(500), stream.next()).await {
        Ok(Some(Ok(_))) => panic!("Should not receive any jobs from empty queue"),
        Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
        Ok(None) => {} // Stream ended - OK
        Err(_) => {}   // Timeout - expected for empty queue
    }

    // Now enqueue a job - should get notification
    let service_clone = Arc::new(service);
    tokio::spawn({
        let service = service_clone.clone();
        async move {
            sleep(Duration::from_millis(100)).await;
            consume_test_utils::enqueue_test_job(&service, "empty_queue").await;
        }
    });

    // Should now receive the job
    match timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(Ok(job_message))) => {
            let job = job_message.job.unwrap();
            assert_eq!(job.job_type, "empty_queue");
            assert_eq!(
                job_message.notification_type,
                NotificationType::NewJob.to_string()
            );
        }
        Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
        Ok(None) => panic!("Stream ended unexpectedly"),
        Err(_) => panic!("Timeout waiting for new job"),
    }
}

// Helper test to verify the stream behavior with job already processed scenario
#[tokio::test]
#[serial]
async fn test_consume_jobs_job_already_processed() {
    let service = Arc::new(consume_test_utils::setup_grpc_service().await);
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Enqueue a job
    let _job_id = consume_test_utils::enqueue_test_job(&service, "test_queue").await;

    // Start first worker - should get the job
    let request1 = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response1 = service.consume_jobs(request1).await.unwrap();
    let mut stream1 = response1.into_inner();

    // Worker1 gets the job
    match timeout(Duration::from_secs(2), stream1.next()).await {
        Ok(Some(Ok(job_message))) => {
            assert!(job_message.job.is_some());
        }
        _ => panic!("Worker1 should have received the job"),
    }

    // Now start second worker and trigger a notification (even though no jobs left)
    let service_clone = service.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        // This will trigger notifications but no actual jobs to dequeue
        service_clone
            .notify_workers("test_queue", NotificationType::NewJob)
            .await;
    });

    let request2 = Request::new(ConsumeJobsRequest {
        worker_id: "worker2".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response2 = service.consume_jobs(request2).await.unwrap();
    let mut stream2 = response2.into_inner();

    // Worker2 should not get anything (job already processed)
    // The stream should handle this gracefully and continue listening
    match timeout(Duration::from_millis(500), stream2.next()).await {
        Ok(Some(Ok(_))) => panic!("Worker2 should not receive already processed job"),
        Ok(Some(Err(_))) => {} // Error is acceptable in this scenario
        Ok(None) => {}         // Stream ended
        Err(_) => {}           // Timeout - expected
    }
}

// INTEGRATION TEST - Full workflow with multiple failures and retries
#[tokio::test]
#[serial]
async fn test_consume_jobs_full_retry_workflow() {
    let service = Arc::new(consume_test_utils::setup_grpc_service().await);
    let _ = consume_test_utils::cleanup_service_data(&service).await;

    // Start worker FIRST (before creating jobs)
    let consume_request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });
    let consume_response = service.consume_jobs(consume_request).await.unwrap();
    let mut stream = consume_response.into_inner();

    // Create job in parallel and wait for proper timing
    let service_clone = service.clone();
    let job_creation = tokio::spawn(async move {
        // Wait for worker to be listening for notifications
        sleep(Duration::from_millis(500)).await;
        
        let request = Request::new(EnqueueJobRequest {
            job_type: "test_queue".to_string(),
            payload: json!({"test": "data"}).to_string(),
            priority: 2,
            scheduled_at: "2025-08-04T21:00:00Z".to_string(),
            max_retries: 2, // Can retry twice
            delay: 1, // 1 second delay for faster testing
        });
        let response = service_clone.enqueue_job(request).await.unwrap();
        // println!("🧪 Created job: {}", response.into_inner().job_id);
        response.into_inner().job_id
    });

    let mut job_attempts = Vec::new();

    // Attempt 1: Initial job (should come via NewJob notification)
    println!("🧪 Waiting for initial job...");
    let job1 = timeout(Duration::from_secs(5), stream.next()).await
        .expect("Should receive initial job")
        .expect("Stream ended")
        .expect("Stream error");
        
    let job_id = job_creation.await.unwrap();
    
    job_attempts.push((job1.notification_type.clone(), job1.job.as_ref().unwrap().id.clone()));
    println!("🧪 Got initial job: {} ({})", job1.job.as_ref().unwrap().id, job1.notification_type);
    
    // Fail attempt 1
    service.fail_job(Request::new(FailJobRequest {
        job_id: job_id.clone(),
        error_message: "Attempt 1 failed".to_string(),
    })).await.unwrap();

    // Attempt 2: First retry
    let job2 = timeout(Duration::from_secs(30), stream.next()).await
        .expect("Should receive retry 1")
        .expect("Stream ended")
        .expect("Stream error");
    job_attempts.push((job2.notification_type.clone(), job2.job.as_ref().unwrap().id.clone()));
    
    // Fail attempt 2
    service.fail_job(Request::new(FailJobRequest {
        job_id: job_id.clone(),
        error_message: "Attempt 2 failed".to_string(),
    })).await.unwrap();

    // Attempt 3: Second retry
    let job3 = timeout(Duration::from_secs(30), stream.next()).await
        .expect("Should receive retry 2")
        .expect("Stream ended")
        .expect("Stream error");
    job_attempts.push((job3.notification_type.clone(), job3.job.as_ref().unwrap().id.clone()));
    
    // Complete attempt 3 successfully
    service.complete_job(Request::new(CompleteJobRequest {
        job_id: job_id.clone(),
    })).await.unwrap();

    // Verify the workflow
    assert_eq!(job_attempts.len(), 3);
    assert_eq!(job_attempts[0].0, NotificationType::NewJob.to_string());
    assert_eq!(job_attempts[1].0, NotificationType::RetryJob.to_string());
    assert_eq!(job_attempts[2].0, NotificationType::RetryJob.to_string());
    
    // All should be the same job
    assert!(job_attempts.iter().all(|(_, id)| id == &job_id));
    
    // Should not get any more jobs (job is completed)
    match timeout(Duration::from_secs(1), stream.next()).await {
        Ok(Some(Ok(job_message))) => {
            assert_ne!(job_message.job.as_ref().unwrap().id, job_id, 
                      "Should not retry completed job");
        }
        Ok(Some(Err(_))) => panic!("Stream error"),
        Ok(None) => {} // Stream ended - acceptable
        Err(_) => {} // Timeout - expected
    }

    println!("✅ Full retry workflow: New -> Retry -> Retry -> Complete");
}