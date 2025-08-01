// tests/consume_jobs_tests.rs
use futures_util::StreamExt;
use serde_json::json;
use serial_test::serial;
use stackduck::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep, timeout};
use tonic::{Request, Response, Status};

mod consume_test_utils {
    use super::*;

    pub async fn setup_grpc_service() -> StackduckGrpcService {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://stackduck_test:test_password@localhost:5433/stackduck_test".to_string()
        });
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6380".to_string());

        let stackduck = StackDuck::new_with_redis(&db_url, &redis_url)
            .await
            .unwrap();
        stackduck.run_migrations().await.unwrap();

        let job_manager = Arc::new(JobManager {
            db_pool: stackduck.db_pool.clone(),
            redis_pool: stackduck.redis_client.clone(),
            in_memory_queue: Mutex::new(HashMap::new()),
        });

        StackduckGrpcService {
            job_manager,
            job_notifiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn cleanup_service_data(service: &StackduckGrpcService) {
        sqlx::query("TRUNCATE TABLE jobs CASCADE")
            .execute(&service.job_manager.db_pool)
            .await
            .unwrap();

        if let Some(client) = &service.job_manager.redis_pool {
            if let Ok(mut conn) = client.get_redis_client().await {
                use redis::AsyncCommands;
                let _: () = conn.flushdb().await.unwrap();
            }
        }
    }

    pub async fn enqueue_test_job(service: &StackduckGrpcService, job_type: &str) -> String {
        let request = Request::new(EnqueueJobRequest {
            job_type: job_type.to_string(),
            payload: json!({"test": "data"}).to_string(),
            priority: 2,
            retry_count: 0,
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
    consume_test_utils::cleanup_service_data(&service).await;

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

    // Should immediately get the 2 existing jobs
    let mut received_jobs = Vec::new();

    // Use timeout to avoid hanging if no jobs come
    for _ in 0..2 {
        match timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(job_message))) => {
                let job = job_message.job.unwrap();
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
async fn test_consume_jobs_new_job_notifications() {
    let service = consume_test_utils::setup_grpc_service().await;
    consume_test_utils::cleanup_service_data(&service).await;

    // Start consuming BEFORE enqueueing jobs
    let request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response = service.consume_jobs(request).await.unwrap();
    let mut stream = response.into_inner();

    // Spawn a task to enqueue a job after a short delay
    let service_clone = Arc::new(service);
    let enqueue_task = {
        let service = service_clone.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            consume_test_utils::enqueue_test_job(&service, "test_queue").await
        })
    };

    // Wait for the job to arrive via stream
    match timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(Ok(job_message))) => {
            let job = job_message.job.unwrap();
            assert_eq!(job.job_type, "test_queue");
            assert_eq!(
                job_message.notification_type,
                NotificationType::NewJob.to_string()
            );

            // Verify it's the job we enqueued
            let enqueued_job_id = enqueue_task.await.unwrap();
            assert_eq!(job.id, enqueued_job_id);
        }
        Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
        Ok(None) => panic!("Stream ended unexpectedly"),
        Err(_) => panic!("Timeout waiting for new job notification"),
    }
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_multiple_job_types() {
    let service = consume_test_utils::setup_grpc_service().await;
    consume_test_utils::cleanup_service_data(&service).await;

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
        match timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(job_message))) => {
                let job = job_message.job.unwrap();
                received_types.push(job.job_type);
            }
            Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
            Ok(None) => break,
            Err(_) => panic!("Timeout waiting for job"),
        }
    }

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
    consume_test_utils::cleanup_service_data(&service).await;

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
async fn test_consume_jobs_retry_notifications() {
    let service = consume_test_utils::setup_grpc_service().await;
    consume_test_utils::cleanup_service_data(&service).await;

    // Start consuming
    let request = Request::new(ConsumeJobsRequest {
        worker_id: "worker1".to_string(),
        job_types: vec!["test_queue".to_string()],
    });

    let response = service.consume_jobs(request).await.unwrap();
    let mut stream = response.into_inner();

    // Enqueue, dequeue, and retry a job
    let service_clone = Arc::new(service);
    let retry_task = {
        let service = service_clone.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;

            // Enqueue job
            let job_id = consume_test_utils::enqueue_test_job(&service, "test_queue").await;

            // Dequeue it
            let dequeue_request = Request::new(DequeueJobRequest {
                queue_name: "test_queue".to_string(),
            });
            service.dequeue_job(dequeue_request).await.unwrap();

            // Retry it
            let retry_request = Request::new(RetryJobRequest {
                job_id: job_id.clone(),
            });
            service.retry_job(retry_request).await.unwrap();

            job_id
        })
    };

    let mut notification_types = Vec::new();
    let mut job_ids = Vec::new();

    // Should get both NewJob and RetryJob notifications
    for _ in 0..2 {
        match timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(job_message))) => {
                notification_types.push(job_message.notification_type.clone());
                if let Some(job) = job_message.job {
                    job_ids.push(job.id);
                }
            }
            Ok(Some(Err(e))) => panic!("Stream error: {:?}", e),
            Ok(None) => break,
            Err(_) => {
                println!("Timeout - received notifications: {:?}", notification_types);
                break;
            }
        }
    }

    let retried_job_id = retry_task.await.unwrap();

    // Should have received both notification types
    assert!(notification_types.contains(&NotificationType::NewJob.to_string()));
    assert!(notification_types.contains(&NotificationType::RetryJob.to_string()));

    // Both should be for the same job
    assert!(job_ids.iter().all(|id| id == &retried_job_id));
}

#[tokio::test]
#[serial]
async fn test_consume_jobs_empty_initially() {
    let service = consume_test_utils::setup_grpc_service().await;
    consume_test_utils::cleanup_service_data(&service).await;

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
    consume_test_utils::cleanup_service_data(&service).await;

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
