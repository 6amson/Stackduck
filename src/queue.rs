use crate::types::Job;
use crate::types::JobManager;
use crate::types::JobNotification;
use crate::types::NotificationType;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
// use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

pub struct JobQueueGrpcService {
    job_manager: Arc<JobManager>,
    job_notifiers: Arc<Mutex<HashMap<String, broadcast::Sender<JobNotification>>>>,
}

impl JobQueueGrpcService {
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self {
            job_manager,
            job_notifiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Notify workers when jobs are available for a specific job_type
    async fn notify_workers(&self, job_type: &str, notification_type: NotificationType) {
        let notifiers = self.job_notifiers.lock().await;
        if let Some(sender) = notifiers.get(job_type) {
            let notification = JobNotification {
                job_type: job_type.to_string(),
                notification_type: notification_type.to_string(),
            };
            let _ = sender.send(notification);
        }
    }

    // Convert your Job struct to gRPC Job message
    fn job_to_grpc(job: Job) -> Job {
        Job {
            id: job.id.to_string(),
            job_type: job.job_type.clone(),
            payload: job.payload.to_string(),
            status: job.status.clone(),
            priority: job.priority,
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            error_message: job.error_message,
            delay: job.delay,
            scheduled_at: job.scheduled_at.map(|dt| dt.timestamp()),
            started_at: job.started_at.map(|dt| dt.timestamp()),
            completed_at: job.completed_at.map(|dt| dt.timestamp()),
            created_at: job.created_at.map(|dt| dt.timestamp()),
            updated_at: job.updated_at.map(|dt| dt.timestamp()),
        }
    }
}

#[tonic::async_trait]
impl JobQueueService for JobQueueGrpcService {
    async fn enqueue_job(
        &self,
        request: Request<job_queue::EnqueueJobRequest>,
    ) -> Result<Response<job_queue::EnqueueJobResponse>, Status> {
        let req = request.into_inner();

        // Parse payload JSON
        let payload: serde_json::Value = serde_json::from_str(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON payload: {}", e)))?;

        // Create job using your existing constructor
        let mut job = crate::Job::new(req.job_type.clone(), payload);

        // Set optional fields
        if let Some(priority) = req.priority {
            job.priority = Some(priority);
        }
        if let Some(max_retries) = req.max_retries {
            job.max_retries = Some(max_retries);
        }
        if let Some(scheduled_timestamp) = req.scheduled_at {
            job.scheduled_at = Some(DateTime::from_timestamp(scheduled_timestamp, 0).unwrap());
        }

        // USE YOUR EXISTING ENQUEUE METHOD
        match self.job_manager.enqueue(job).await {
            Ok(enqueued_job) => {
                // After successful enqueue, notify workers
                self.notify_workers(&req.job_type, "NEW_JOB").await;

                Ok(Response::new(job_queue::EnqueueJobResponse {
                    job_id: enqueued_job.id.to_string(),
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(job_queue::EnqueueJobResponse {
                job_id: String::new(),
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    async fn dequeue_job(
        &self,
        request: Request<job_queue::DequeueJobRequest>,
    ) -> Result<Response<job_queue::DequeueJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING DEQUEUE METHOD
        // Your dequeue checks Redis -> InMemory -> Postgres in that order
        match self.job_manager.dequeue(&req.queue_name).await {
            Ok(Some(job)) => Ok(Response::new(job_queue::DequeueJobResponse {
                job: Some(Self::job_to_grpc(&job)),
                success: true,
            })),
            Ok(None) => Ok(Response::new(job_queue::DequeueJobResponse {
                job: None,
                success: false,
            })),
            Err(e) => Err(Status::internal(format!("Dequeue failed: {}", e))),
        }
    }

    type ConsumeJobsStream = tokio_stream::wrappers::BroadcastStream<JobNotification>;

    async fn consume_jobs(
        &self,
        request: Request<job_queue::ConsumeJobsRequest>,
    ) -> Result<Response<Self::ConsumeJobsStream>, Status> {
        let req = request.into_inner();

        // Create or get broadcast channels for each job type
        let mut all_receivers = Vec::new();
        {
            let mut notifiers = self.job_notifiers.lock().await;

            for job_type in &req.job_types {
                let sender = notifiers
                    .entry(job_type.clone())
                    .or_insert_with(|| broadcast::channel(1000).0)
                    .clone();

                all_receivers.push(sender.subscribe());
            }
        }

        // Create a stream that listens for notifications and then attempts dequeue
        let job_manager = self.job_manager.clone();
        let worker_id = req.worker_id.clone();
        let job_types = req.job_types.clone();

        let stream = async_stream::stream! {
            // First, check for existing jobs in all requested queues
            for job_type in &job_types {
                while let Ok(Some(job)) = job_manager.dequeue(job_type).await {
                    yield Ok(job_queue::JobMessage {
                        job: Some(Self::job_to_grpc(&job)),
                        notification_type: "EXISTING_JOB".to_string(),
                    });
                }
            }

            // Then listen for new job notifications
            let mut merged_receivers = Self::merge_notification_receivers(all_receivers);

            while let Some(notification_result) = merged_receivers.recv().await {
                if let Ok(notification) = notification_result {
                    // When notified about a job, try to dequeue from that specific queue
                    if let Ok(Some(job)) = job_manager.dequeue(&notification.job_type).await {
                        yield Ok(job_queue::JobMessage {
                            job: Some(Self::job_to_grpc(&job)),
                            notification_type: notification.notification_type,
                        });
                    }
                }
            }
        };

        println!(
            "Worker {} subscribed to job types: {:?}",
            worker_id, job_types
        );
        Ok(Response::new(Box::pin(stream)))
    }

    async fn complete_job(
        &self,
        request: Request<job_queue::CompleteJobRequest>,
    ) -> Result<Response<job_queue::CompleteJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING METHOD
        match self.job_manager.mark_job_completed(&req.job_id).await {
            Ok(_) => Ok(Response::new(job_queue::CompleteJobResponse {
                success: true,
            })),
            Err(e) => {
                println!("Failed to complete job {}: {}", req.job_id, e);
                Ok(Response::new(job_queue::CompleteJobResponse {
                    success: false,
                }))
            }
        }
    }

    async fn fail_job(
        &self,
        request: Request<job_queue::FailJobRequest>,
    ) -> Result<Response<job_queue::FailJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING METHOD
        match self.job_manager.mark_job_failed(&req.job_id, &req.error_message).await {
            Ok(_) => Ok(Response::new(job_queue::FailJobResponse { success: true })),
            Err(e) => {
                println!("Failed to mark job {} as failed: {}", req.job_id, e);
                Ok(Response::new(job_queue::FailJobResponse { success: false }))
            }
        }
    }

    async fn retry_job(
        &self,
        request: Request<job_queue::RetryJobRequest>,
    ) -> Result<Response<job_queue::RetryJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING RETRY METHOD
        match self.job_manager.retry_job(&req.job_id).await {
            Ok(_) => {
                // After retry, notify workers (job is re-enqueued)
                if let Ok(Some(job)) = self.job_manager.get_job_by_id(&req.job_id).await {
                    self.notify_workers(&job.job_type, "RETRY_JOB").await;
                }
                Ok(Response::new(job_queue::RetryJobResponse { success: true }))
            }
            Err(e) => {
                println!("Failed to retry job {}: {}", req.job_id, e);
                Ok(Response::new(job_queue::RetryJobResponse {
                    success: false,
                }))
            }
        }
    }
}

impl JobQueueGrpcService {
    // Helper to merge multiple broadcast receivers
    fn merge_notification_receivers(
        receivers: Vec<broadcast::Receiver<JobNotification>>,
    ) -> broadcast::Receiver<JobNotification> {
        // For simplicity, just return the first receiver
        // In production, you'd want to properly merge all receivers
        receivers.into_iter().next().unwrap()
    }
}
