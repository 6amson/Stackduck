use crate::stackduck::stack_duck_service_server::StackDuckService;
use crate::stackduck::{
    CompleteJobRequest, CompleteJobResponse, ConsumeJobsRequest, DequeueJobRequest,
    DequeueJobResponse, DequeueRetriedJobRequest, DequeueRetriedJobResponse, EnqueueJobRequest,
    EnqueueJobResponse, FailJobRequest, FailJobResponse, GrpcJob, JobMessage, RetryJobRequest,
    RetryJobResponse,
};
use crate::types::{Job, JobManager, JobNotification, NotificationType};
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, SelectAll};
use futures::StreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, span, warn, Level};

#[derive(Clone)]
pub struct StackduckGrpcService {
    pub job_manager: Arc<JobManager>,
    pub job_notifiers: Arc<Mutex<HashMap<String, broadcast::Sender<JobNotification>>>>,
}

impl StackduckGrpcService {
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self {
            job_manager,
            job_notifiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Notify workers when jobs are available for a specific job_type
    pub async fn notify_workers(&self, job_type: &str, notification_type: NotificationType) {
        let notifiers = self.job_notifiers.lock().await;
        if let Some(sender) = notifiers.get(job_type) {
            let notification = JobNotification {
                job_type: job_type.to_string(),
                notification_type,
                timestamp: Instant::now(),
            };
            let _ = sender.send(notification);
        }
    }


    fn merge_notification_receivers(
        mut receivers: Vec<tokio::sync::broadcast::Receiver<JobNotification>>,
    ) -> SelectAll<BoxStream<'static, JobNotification>> {
        let streams = receivers
            .drain(..)
            .map(|rx| {
                let stream = BroadcastStream::new(rx).filter_map(|result| async move {
                    match result {
                        Ok(notification) => Some(notification),
                        Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                            warn!("Worker lagged, skipped {} notifications", skipped);
                            None
                        }
                    }
                });
                Box::pin(stream) as BoxStream<'static, JobNotification>
            })
            .collect::<Vec<_>>();

        SelectAll::from_iter(streams)
    }

    pub async fn start_scheduled_job_poller(&self) {
        loop {
            // Get a snapshot of job types at the beginning of each iteration
            let job_types = {
            let all_job_types = self.job_manager.subscribed_job_types.read().await;
            info!("🔍 Poller sees job_types: {:?} (count: {})", 
                    all_job_types, all_job_types.len()); 
            all_job_types.iter().cloned().collect::<Vec<String>>()
        };

            // Skip iteration if no job types are subscribed
            if job_types.is_empty() {
                // println!("No job types subscribed, skipping poll");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }

            match self
                .job_manager
                .get_due_jobs_sequentially(
                    &job_types.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                )
                .await
            {
                Ok(job) => match job {
                    Some(job) => {
                        let grpc_job: GrpcJob = job.into();
                        let req = Request::new(DequeueRetriedJobRequest {
                            job: Some(grpc_job),
                        });
                        match self.dequeue_retried_job(req).await {
                            Ok(_) => println!("Successfully processed job"),
                            Err(e) => println!("Error processing job: {:?}", e),
                        }
                    }
                    None => warn!("No due jobs found"),
                },
                Err(e) => warn!("Error polling for due jobs: {:?}", e),
            }

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
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

        let scheduled_at = if !req.scheduled_at.trim().is_empty() {
            Some(
                DateTime::parse_from_rfc3339(&req.scheduled_at)
                    .map_err(|e| {
                        Status::invalid_argument(format!("Invalid scheduled_at format: {}", e))
                    })?
                    .with_timezone(&Utc),
            )
        } else {
            None
        };

        let max_retries = if (0..=3).contains(&req.max_retries) {
            req.max_retries
        } else {
            eprintln!("Invalid max retries {}, defaulting to 2", req.max_retries);
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
            scheduled_at,
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

    async fn dequeue_retried_job(
        &self,
        request: Request<DequeueRetriedJobRequest>,
    ) -> Result<Response<DequeueRetriedJobResponse>, Status> {
        let req = request.into_inner();

        if let Some(job) = req.job {
            self.notify_workers(&job.job_type, NotificationType::RetryJob)
                .await;
            Ok(Response::new(DequeueRetriedJobResponse { success: true }))
        } else {
            Ok(Response::new(DequeueRetriedJobResponse { success: false }))
        }
    }

    async fn dequeue_job(
        &self,
        request: Request<DequeueJobRequest>,
    ) -> Result<Response<DequeueJobResponse>, Status> {
        let req = request.into_inner();

        // USE EXISTING DEQUEUE METHOD
        // dequeue checks Redis -> Postgres in that order
        match self
            .job_manager
            .dequeue(&req.queue_name, Some(&req.worker_id))
            .await
        {
            Ok(Some(job)) => {
                // let grpc_job: GrpcJob = StackduckGrpcService::job_to_grpc(job);
                let grpc_job: GrpcJob = job.into();
                let jobb = grpc_job.clone();
                let response = DequeueJobResponse {
                    job: Some(grpc_job),
                    success: true,
                    error_message: String::new(),
                };
                self.notify_workers(&jobb.job_type, NotificationType::NewJob)
                    .await;
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

    async fn complete_job(
        &self,
        request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        let req = request.into_inner();

        // USE YOUR EXISTING METHOD
        match self.job_manager.ack_job(&req.job_id).await {
            Ok(_) => Ok(Response::new(CompleteJobResponse { success: true })),
            Err(e) => {
                warn!("Failed to complete job {}: {}", req.job_id, e);
                Ok(Response::new(CompleteJobResponse { success: false }))
            }
        }
    }

    async fn fail_job(
        &self,
        request: Request<FailJobRequest>,
    ) -> Result<Response<FailJobResponse>, Status> {
        let req = request.into_inner();
        let job_result = match self.job_manager.get_job_by_id(&req.job_id).await {
            Ok(Some(job)) => {
                let retry_count = job.retry_count.unwrap_or_default();
                let max_retries = job.max_retries.unwrap_or_default();
                let job_type = job.job_type.clone();

                if retry_count < max_retries {
                    match self.job_manager.retry_job(job, &req.error_message).await {
                        Ok(_) => {
                            self.notify_workers(&job_type, NotificationType::RetryJob)
                                .await;
                            Ok(Response::new(FailJobResponse { success: true }))
                        }
                        Err(e) => {
                            warn!("Failed to mark job {} as failed: {}", req.job_id, e);
                            Ok(Response::new(FailJobResponse { success: false }))
                        }
                    }
                } else {
                    match self
                        .job_manager
                        .nack_job(&req.job_id, &req.error_message)
                        .await
                    {
                        Ok(_) => Ok(Response::new(FailJobResponse { success: true })),
                        Err(e) => {
                            warn!("Failed to mark job {} as failed: {}", req.job_id, e);
                            Ok(Response::new(FailJobResponse { success: false }))
                        }
                    }
                }
            }
            Ok(None) => {
                info!("Failed to nack job {} as failed.", req.job_id);
                Ok(Response::new(FailJobResponse { success: false }))
            }
            Err(e) => {
                warn!("Failed to nack job {} as failed: {}", req.job_id, e);
                Ok(Response::new(FailJobResponse { success: false }))
            }
        };

        job_result
    }

    async fn retry_job(
        &self,
        request: Request<RetryJobRequest>,
    ) -> Result<Response<RetryJobResponse>, Status> {
        let req = request.into_inner();
        let job_result = self.job_manager.get_job_by_id(&req.job_id).await;
        let job_option = job_result.unwrap_or_default();
        let job = job_option.unwrap();

        let job_types = self.job_manager.subscribed_job_types.read().await;
        info!("Job types: {:?}", job_types);

        // USE YOUR EXISTING RETRY METHOD
        match self.job_manager.retry_job(job, &req.error_message).await {
            Ok(_) => {
                // After retry, notify workers (job is re-enqueued)
                Ok(Response::new(RetryJobResponse { success: true }))
            }
            Err(e) => {
                warn!("Failed to retry job {}: {}", req.job_id, e);
                Ok(Response::new(RetryJobResponse { success: false }))
            }
        }
    }

    async fn consume_jobs(
        &self,
        request: Request<ConsumeJobsRequest>,
    ) -> Result<Response<Self::ConsumeJobsStream>, Status> {
        let req = request.into_inner();

        // Validate input
        if req.job_types.is_empty() {
            return Err(Status::invalid_argument("job_types cannot be empty"));
        }

        let worker_span = span!(Level::INFO, "worker", id = %req.worker_id);
        let _enter = worker_span.enter();

        let job_types = req.job_types.clone();

        {
            let mut subscribed_job_types = self.job_manager.subscribed_job_types.write().await;
            for job_type in job_types.iter() {
                subscribed_job_types.insert(job_type.clone());
            }
            
            info!("🔍 Just inserted job_types: {:?}", subscribed_job_types);
        } 

        // Setup broadcast receivers - scope the lock properly
        let receivers = {
            let mut notifiers = self.job_notifiers.lock().await;
            req.job_types
                .iter()
                .map(|job_type| {
                    notifiers
                        .entry(job_type.clone())
                        .or_insert_with(|| broadcast::channel(1000).0)
                        .subscribe()
                })
                .collect::<Vec<_>>()
        };

        let job_manager = Arc::clone(&self.job_manager);
        let worker_id = req.worker_id.clone();
        let job_types = req.job_types.clone();

        info!("Starting job consumption for types: {:?}", job_types);

        let stream = async_stream::stream! {
            // Phase 1: Check existing jobs with round-robin fairness.
            let mut queue_index = 0;
            let mut jobs_found = true;
            let mut existing_job_count = 0;
            const MAX_EXISTING_JOBS: usize = 40; // Safety limit

            debug!("Checking for existing jobs in types: {:?}", job_types);

            // Round-robin through types until no more jobs found
            while jobs_found && existing_job_count < MAX_EXISTING_JOBS {
                jobs_found = false;

                // Check each type once per round
                for _ in 0..job_types.len() {
                    let job_type = &job_types[queue_index % job_types.len()];
                    queue_index += 1;
                   debug!("🔴 Worker {} attempting dequeue for {}", worker_id, job_type);

                    match job_manager.dequeue(job_type, Some(&worker_id)).await {
                        Ok(Some(job)) => {
                            debug!("Found existing job: id={}, type={}", job.id, job.job_type);
                            existing_job_count += 1;
                            jobs_found = true;

                            yield Ok(JobMessage {
                                job: Some(job.into()),
                                notification_type: NotificationType::ExistingJob.to_string(),
                            });
                        }
                        Ok(None) => {
                            // No job in this queue, continue to next
                        }
                        Err(e) => {
                            error!("Error checking existing jobs for type {}: {:?}", job_type, e);
                            yield Err(Status::internal("Failed to check existing jobs"));
                            return;
                        }
                    }
                }
            }

            info!("Found {} existing jobs, now listening for notifications...", existing_job_count);

            // Phase 2: Listen for notifications (your original event-driven approach)
            let mut notification_stream = Self::merge_notification_receivers(receivers);

            while let Some(notification) = notification_stream.next().await {
                let job_type = &notification.job_type;
                debug!("Received notification of type: {} for job_type: {}", notification.notification_type, job_type);

                match job_manager.dequeue(job_type, Some(&worker_id)).await {
                    Ok(Some(job)) => {
                        info!("Successfully dequeued job: id={}, type={}", job.id, job.job_type);
                        yield Ok(JobMessage {
                            job: Some(job.into()),
                            notification_type: notification.notification_type.to_string(),
                        });
                    }
                    Ok(None) => {
                        debug!("Job already processed for type: {}", job_type);
                    }
                    Err(e) => {
                        warn!("Dequeue failed for {}: {:?}", job_type, e);
                    }
                }
            }

            info!("Worker stream ended");
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

// impl StackduckGrpcService {
//     /// Drain existing jobs with true round-robin fairness
//     async fn drain_existing_jobs(
//         job_manager: &JobManager,
//         job_types: &[String],
//         worker_id: &str,
//     ) -> Vec<Result<Job, StackDuckError>> {
//         const MAX_TOTAL_EXISTING: usize = 50; // Circuit breaker

//         let mut results = Vec::new();
//         let mut queue_index = 0;
//         let mut consecutive_empty = 0;
//         let job_type_count = job_types.len();

//         debug!("Draining existing jobs for worker: {}", worker_id);

//         // True round-robin: one job per type per iteration
//         loop {
//             // Circuit breaker
//             if results.len() >= MAX_TOTAL_EXISTING {
//                 warn!(
//                     "Hit maximum existing jobs limit ({}), stopping drain",
//                     MAX_TOTAL_EXISTING
//                 );
//                 break;
//             }

//             // Stop if we've cycled through all types without finding jobs
//             if consecutive_empty >= job_type_count {
//                 debug!("No more jobs found after checking all types");
//                 break;
//             }

//             let job_type = &job_types[queue_index % job_type_count];
//             queue_index += 1;

//             match job_manager.dequeue(job_type).await {
//                 Ok(Some(job)) => {
//                     debug!("Drained existing job: id={}, type={}", job.id, job.job_type);
//                     results.push(Ok(job));
//                     consecutive_empty = 0; // Reset counter on successful dequeue
//                 }
//                 Ok(None) => {
//                     consecutive_empty += 1; // No job in this queue
//                 }
//                 Err(e) => {
//                     error!("Error draining jobs for type {}: {:?}", job_type, e);
//                     results.push(Err(e));
//                     break;
//                 }
//             }
//         }

//         info!(
//             "Drained {} existing jobs for worker: {}",
//             results.len(),
//             worker_id
//         );
//         results
//     }

// }
