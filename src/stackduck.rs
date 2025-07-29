use crate::types::Job;
use crate::types::JobManager;
use crate::types::JobNotification;
use crate::types::NotificationType;
use async_stream::stream;
use chrono::{DateTime, Utc};
use futures::stream::{StreamExt, select_all};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status, transport::Server};

pub struct StackduckGrpcService {
    job_manager: Arc<JobManager>,
    job_notifiers: Arc<Mutex<HashMap<String, broadcast::Sender<JobNotification>>>>,
}

impl StackduckGrpcService {
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

    // Convert Job struct to gRPC Job message
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

    fn merge_notification_receivers(
        receivers: Vec<broadcast::Receiver<JobNotification>>,
    ) -> impl Stream<Item = Result<JobNotification, broadcast::error::RecvError>> {
        let streams = receivers
            .into_iter()
            .map(|rx| {
                BroadcastStream::new(rx).map(|result| {
                    match result {
                        Ok(notification) => Ok(notification),
                        Err(e) => {
                            // Log lagged messages or other errors
                            eprintln!("Receiver error: {:?}", e);
                            Err(e)
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        select_all(streams)
    }
}

#[tonic::async_trait]
impl StackduckService for StackduckGrpcService {
    async fn enqueue_job(
        &self,
        request: Request<job_queue::EnqueueJobRequest>,
    ) -> Result<Response<job_queue::EnqueueJobResponse>, Status> {
        let req = request.into_inner();

        // Parse payload JSON
        let payload: serde_json::Value = serde_json::from_str(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON payload: {}", e)))?;

        // Validate and default priority in enqueue
        let priority = match req.priority {
            Some(p) if (1..=3).contains(&p) => Some(p),
            Some(invalid) => {
                eprintln!("Invalid priority {}, defaulting to 2", invalid);
                Some(2) // Default to normal priority
            }
            None => Some(2), // Default to normal priority
        };

        let retry_count = match req.retry_count {
            Some(p) if (1..=4).contains(&p) => Some(p),
            Some(_) => {
                eprintln!("Invalid retry count {}, defaulting to 0", req.retry_count);
                Some(0)
            }
            None => Some(0),
        };

        let max_retries = match req.max_retries {
            Some(p) if (1..=4).contains(&p) => Some(p),
            Some(_) => {
                eprintln!("Invalid max retries {}, defaulting to 2", req.max_retries);
                Some(2)
            }
            None => Some(0),
        };

        let delay = match req.delay {
            Some(p) if (1..=3600).contains(&p) => Some(p),
            Some(_) => {
                eprintln!("Invalid delay {}, defaulting to 30 seconds", req.delay);
                Some(30)
            }
            None => Some(0),
        };

        // Create job using existing constructor
        let mut job = Job::new(
            req.job_type.clone(),
            payload,
            priority,
            delay,
            max_retries,
            retry_count,
        );

        // USE EXISTING ENQUEUE METHOD
        match self.job_manager.enqueue(job).await {
            Ok(enqueued_job) => {
                // After successful enqueue, notify workers
                self.notify_workers(&req.job_type, NotificationType::NewJob)
                    .await;

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

        // USE EXISTING DEQUEUE METHOD
        // dequeue checks Redis -> InMemory -> Postgres in that order
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

    type ConsumeJobsStream =
        Pin<Box<dyn Stream<Item = Result<job_queue::JobMessage, Status>> + Send>>;

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
                                notification_type: NotificationType::ExistingJob.to_string(),
                            });
                        }
                    }

                    // Then listen for new job notifications
                    let mut merged_receivers = Self::merge_notification_receivers(all_receivers);

        while let Some(notification_result) = merged_receivers.next().await {
            let notification = match notification_result {
                Ok(notification) => notification,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!("Missed {} notifications due to lag", count);
                    continue; // Skip this iteration but keep processing
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("A notification receiver closed");
                    continue; // Other receivers might still be active
                }
            };

            // Process the notification
            match job_manager.dequeue(&notification.job_type).await {
                Ok(Some(job)) => {
                    yield Ok(job_queue::JobMessage {
                        job: Some(Self::job_to_grpc(&job)),
                        notification_type: notification.notification_type,
                    });
                }
                Ok(None) => {
                    // Job was already taken by another worker - normal in distributed systems
                    tracing::trace!("Job already processed for type: {}", notification.job_type);
                }
                Err(e) => {
                    tracing::error!("Dequeue failed for {}: {:?}", notification.job_type, e);
                    // Decide: yield error or continue
                    yield Err(e.into());
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
        match self.job_manager.ack_job(&req.job_id).await {
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
        match self
            .job_manager
            .nack_job(&req.job_id, &req.error_message)
            .await
        {
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
                    self.notify_workers(&job.job_type, NotificationType::RetryJob)
                        .await;
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
