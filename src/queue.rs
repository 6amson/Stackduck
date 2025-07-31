use crate::stackduck::stack_duck_service_server::StackDuckService;
use crate::stackduck::{
    CompleteJobRequest, CompleteJobResponse, ConsumeJobsRequest, DequeueJobRequest,
    DequeueJobResponse, EnqueueJobRequest, EnqueueJobResponse, FailJobRequest, FailJobResponse,
    GrpcJob, JobMessage, RetryJobRequest, RetryJobResponse,
};
use crate::types::{Job, JobManager, JobNotification, NotificationType};
use futures::stream::{select_all};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tonic::{Request, Response, Status};

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
                notification_type,
            };
            let _ = sender.send(notification);
        }
    }

    // Convert Job struct to gRPC Job message
    fn job_to_grpc(job: Job) -> GrpcJob {
        GrpcJob {
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

    fn merge_notification_receivers(
        receivers: Vec<broadcast::Receiver<JobNotification>>,
    ) -> impl Stream<Item = JobNotification> {
        let streams = receivers
            .into_iter()
            .map(|rx| {
                BroadcastStream::new(rx).filter_map(|result| {
                    // Sync closure, no async needed
                    match result {
                        Ok(notification) => Some(notification),
                        Err(BroadcastStreamRecvError::Lagged(n)) => {
                            eprintln!("Receiver lagged by {} messages, skipping", n);
                            None
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        select_all(streams)
    }
}

#[tonic::async_trait]
impl StackDuckService for StackduckGrpcService {
    async fn enqueue_job(
        &self,
        request: Request<EnqueueJobRequest>,
    ) -> Result<Response<EnqueueJobResponse>, Status> {
        let req = request.into_inner();

        // Parse payload JSON
        let payload: serde_json::Value = serde_json::from_str(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON payload: {}", e)))?;

        // Validate and default priority in enqueue
        let priority = if (1..=3).contains(&req.priority) {
            req.priority
        } else {
            eprintln!("Invalid priority {}, defaulting to 2", req.priority);
            2
        };

        let retry_count = if (0..=4).contains(&req.retry_count) {
            req.retry_count
        } else {
            eprintln!("Invalid retry_count {}, defaulting to 0", req.retry_count);
            0
        };

        let max_retries = if (1..=4).contains(&req.max_retries) {
            req.max_retries
        } else {
            eprintln!("Invalid max_retries {}, defaulting to 2", req.max_retries);
            2
        };

        let delay = if (1..=3600).contains(&req.delay) {
            req.delay
        } else {
            eprintln!("Invalid delay {}, defaulting to 30 seconds", req.delay);
            30
        };

        // Create job using existing constructor
        let job = Job::new(
            req.job_type.clone(),
            payload,
            Some(priority),
            Some(delay),
            Some(max_retries),
            Some(retry_count),
        );

        // USE EXISTING ENQUEUE METHOD
        match self.job_manager.enqueue(job).await {
            Ok(enqueued_job) => {
                // After successful enqueue, notify workers
                self.notify_workers(&req.job_type, NotificationType::NewJob)
                    .await;

                Ok(Response::new(EnqueueJobResponse {
                    job_id: enqueued_job.id.to_string(),
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(EnqueueJobResponse {
                job_id: String::new(),
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    async fn dequeue_job(
        &self,
        request: Request<DequeueJobRequest>,
    ) -> Result<Response<DequeueJobResponse>, Status> {
        let req = request.into_inner();

        // USE EXISTING DEQUEUE METHOD
        // dequeue checks Redis -> InMemory -> Postgres in that order
        match self.job_manager.dequeue(&req.queue_name).await {
            Ok(Some(job)) => {
                let grpc_job: GrpcJob = StackduckGrpcService::job_to_grpc(job);
                let response = DequeueJobResponse {
                    job: Some(grpc_job),
                    success: true,
                    error_message: String::new(),
                };
                Ok(Response::new(response))
            }
            Ok(None) => {
                let response = DequeueJobResponse {
                    job: None,
                    success: true,
                    error_message: String::new(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                let response = DequeueJobResponse {
                    job: None,
                    success: false,
                    error_message: e.to_string(),
                };
                Ok(Response::new(response))
            }
        }
    }

    type ConsumeJobsStream = Pin<Box<dyn Stream<Item = Result<JobMessage, Status>> + Send>>;

    async fn consume_jobs(
        &self,
        request: Request<ConsumeJobsRequest>,
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
        let job_types2 = req.job_types.clone();


        let stream = async_stream::stream! {
            // First, check for existing jobs in all requested queues
            for job_type in job_types {
                while let Ok(Some(job)) = job_manager.dequeue(&job_type).await {
                    yield Ok(JobMessage {
                        job: Some(StackduckGrpcService::job_to_grpc(job)),
                        notification_type: NotificationType::ExistingJob.to_string(), // Add notification_type
                    });
                }
            }

            // Then listen for new job notifications
            let mut merged_receivers = Self::merge_notification_receivers(all_receivers);

            while let Some(notification_result) = merged_receivers.next().await {
                let notification = notification_result;

                // Process the notification
                match job_manager.dequeue(&notification.job_type).await {
                    Ok(Some(job)) => {
                        yield Ok(JobMessage {
                            job: Some(StackduckGrpcService::job_to_grpc(job)),
                            notification_type: notification.notification_type.to_string(), // Use the actual notification type
                        });
                    }
                    Ok(None) => {
                        // Job was already taken by another worker - normal in distributed systems
                        eprintln!("Job already processed for type: {}", notification.job_type);
                        // Don't yield anything, just continue
                    }
                    Err(e) => {
                        eprintln!("Dequeue failed for {}: {:?}", notification.job_type, e);
                        // Yield error instead of using .into() which might not be implemented
                        yield Err(Status::internal(format!("Dequeue failed: {}", e)));
                    }
                }
            }
        };

        let job_tt = job_types2.clone();

        println!(
            "Worker {} subscribed to job types: {:?}",
            worker_id, job_tt
        );

        Ok(Response::new(Box::pin(stream)))
    }

    async fn complete_job(
        &self,
        request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING METHOD
        match self.job_manager.ack_job(&req.job_id).await {
            Ok(_) => Ok(Response::new(CompleteJobResponse { success: true })),
            Err(e) => {
                println!("Failed to complete job {}: {}", req.job_id, e);
                Ok(Response::new(CompleteJobResponse { success: false }))
            }
        }
    }

    async fn fail_job(
        &self,
        request: Request<FailJobRequest>,
    ) -> Result<Response<FailJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING METHOD
        match self
            .job_manager
            .nack_job(&req.job_id, &req.error_message)
            .await
        {
            Ok(_) => Ok(Response::new(FailJobResponse { success: true })),
            Err(e) => {
                println!("Failed to mark job {} as failed: {}", req.job_id, e);
                Ok(Response::new(FailJobResponse { success: false }))
            }
        }
    }

    async fn retry_job(
        &self,
        request: Request<RetryJobRequest>,
    ) -> Result<Response<RetryJobResponse>, Status> {
        let req = request.into_inner();
        let job_result = self.job_manager.get_job_by_id(&req.job_id).await;
        let job_option = job_result.unwrap_or_default();
        let job = job_option.unwrap();

        // USE YOUR EXISTING RETRY METHOD
        match self.job_manager.retry_job(job).await {
            Ok(_) => {
                // After retry, notify workers (job is re-enqueued)
                if let Ok(Some(job)) = self.job_manager.get_job_by_id(&req.job_id).await {
                    self.notify_workers(&job.job_type, NotificationType::RetryJob)
                        .await;
                }
                Ok(Response::new(RetryJobResponse { success: true }))
            }
            Err(e) => {
                println!("Failed to retry job {}: {}", req.job_id, e);
                Ok(Response::new(RetryJobResponse { success: false }))
            }
        }
    }
}
