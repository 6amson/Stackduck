# Stackduck

A high-performance, distributed job queue system built in Rust with gRPC support for language-agnostic job processing.

## Overview

Stackduck provides a robust foundation for background job processing in distributed systems. It combines the speed of Redis for job queuing with PostgreSQL's reliability for metadata persistence, offering automatic failover and graceful degradation.

## Features

- **Multi-tier Storage**: Redis (fast queuing) → In-memory (fallback) → PostgreSQL (persistence)
- **gRPC API**: Language-agnostic job management and worker coordination
- **Job Priorities**: 3-level priority system for job ordering
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Real-time Workers**: Streaming job consumption with instant notifications
- **Fault Tolerance**: Automatic failover between storage layers
- **Job Scheduling**: Delay jobs for future execution
- **Worker Coordination**: Multi-worker support with race condition handling

## 🛠️ Architecture Overview

```text
Producers (Framework Agnostic)
    NestJS | Flask | Ruby | CLI | etc
         |
         | gRPC Job Submission.
         v
   +---------------------------+
   | StackDuck gRPC API (Rust) |
   | - async-stream support    |
   | - Job validation          |
   +---------------------------+
         |
         | Enqueue + Persist
         v
+---------------------------+    +---------------------------+
| Redis Queue               |    | PostgreSQL                |
| - Job queue               |    | - Job metadata            |
| - Priority queues         |    | - Execution state         |
| - Rate limiting           |    | - Worker assignments      |
+---------------------------+    +---------------------------+
         |                              
         | Notification Emit            
         v                              
   +---------------------------+        
   | StackDuck gRPC API (Rust) |        
   | - Job ready notifications |        
   | - Worker selection        |        
   +---------------------------+        
         |                             
         | async-stream job distribution 
         v                              
+------------------------------------------------------------+
|          Workers/Consumers (Multi-Framework)               |
|                      Job Execution                         |
|                Job Ack and Job Nack calls                  |
|  +------------+ +------------+ +------------+ +----------+ |
|  | Ruby       | | NestJS     | | Flask      | | Rust     | |
|  | Worker     | | Worker     | | Worker     | | Worker   | |
|  | (gRPC)     | | (gRPC)     | | (gRPC)     | | (Native) | |
|  +------------+ +------------+ +------------+ +----------+ |
+-------------------------------------------------------------+
         |
         | Job Ack or Job Nack
         v
+-----------------------------------------+
| StackDuck gRPC API (Rust)               |
| - On Nack (Handles retries)             |
| - On Ack (Marks job as complete)        |
| - On Error (Marks job as failed)        |
| - On failure (Moves job to dead-letter) |
+-----------------------------------------+
         |
         | Stream Results
         v
   +--------------------------------------+
   |  Consumers/Workers                   |
   | - Polls result for dead-letter queue |
   | - Handle dead letter jobs            |
   | - Log failed jobs                    |
   +--------------------------------------+
```

## Quick Start

### Prerequisites

- Rust 1.70+
- PostgreSQL 12+
- Redis 6+

### Installation

```bash
cargo install stackduck

export DATABASE_URL="postgres://user:pass@localhost/mydb"
export REDIS_URL="redis://127.0.0.1:6379"
export SERVER_ADDR="127.0.0.1:50051" (optional)

stackduck
```


```bash
git clone https://github.com/6amson/Stackduck
cd Stackduck
cargo build --release
```

### Configuration

Create a `.env` file:

```env
DATABASE_URL=postgresql://user:password@localhost/stackduck
REDIS_URL=redis://localhost:6379
GRPC_PORT=50051
```

### Running the Server

```bash
cargo run --release
```

## Usage

### Enqueuing Jobs

```rust
// Rust client example
let mut client = StackDuckServiceClient::connect("http://127.0.0.1:50051").await?;

let request = EnqueueJobRequest {
    job_type: "email_send".to_string(),
    payload: r#"{"to": "user@example.com", "subject": "Welcome!"}"#.to_string(),
    priority: 1,        // High priority (1-3)
    delay: 0,           // Execute retry after delay * 2^retry_count, capped at 1hr.
    max_retries: 3,     // Max retries up to 3 times
};

let response = client.enqueue_job(request).await?;
println!("Job enqueued with ID: {}", response.into_inner().job_id);
```

### Worker Implementation

```rust
// Worker consuming jobs
let request = ConsumeJobsRequest {
    worker_id: "worker-001".to_string(),
    job_types: vec!["email_send".to_string(), "image_process".to_string()],
};

let mut stream = client.consume_jobs(request).await?.into_inner();

while let Some(job_message) = stream.message().await? {
    if let Some(job) = job_message.job {
        println!("Processing job: {}", job.id);
        
        // Process the job
        match process_job(&job).await {
            Ok(_) => {
                // Mark job as completed
                client.complete_job(CompleteJobRequest {
                    job_id: job.id.clone(),
                }).await?;
            }
            Err(e) => {
                // Mark job as failed
                client.fail_job(FailJobRequest {
                    job_id: job.id.clone(),
                    error_message: e.to_string(),
                }).await?;
            }
        }
    }
}
```

## API Reference

### Job Structure

```protobuf
message Job {
    string id = 1;
    string job_type = 2;
    string payload = 3;       // JSON string
    string status = 4;        // "queued", "running", "completed"
    int32 priority = 5;       // 1 (high) to 3 (low)
    int32 retry_count = 6;
    int32 max_retries = 7;
    string error_message = 8;
    int32 delay = 9;          // Delay in seconds
    int64 scheduled_at = 10;  // Unix timestamp
    int64 started_at = 11;
    int64 completed_at = 12;
    int64 created_at = 13;
    int64 updated_at = 14;
}
```

### gRPC Methods

#### `EnqueueJob`
Adds a new job to the queue.

**Request**: `EnqueueJobRequest`
- `job_type`: String identifier for the job type
- `payload`: JSON string with job data
- `priority`: Job priority (1-3, defaults to 2)
- `delay`: Time basis for calculating exponential backoff in seconds, defaults to 30
- `max_retries`: Maximum retry attempts if job failed, defaults to 2

#### `DequeueJob`
Pulls a single job from a specific queue. [This is an internally called endpoint].

**Request**: `DequeueJobRequest`
- `queue_name`: Name of the queue to pull from

#### `ConsumeJobs`
Opens a streaming connection for continuous job processing.

**Request**: `ConsumeJobsRequest`
- `worker_id`: Unique identifier for the worker
- `job_types`: Array of job types this worker can handle

#### `CompleteJob`
Marks a job as successfully completed.

**Request**: `CompleteJobRequest`
- `job_id`: ID of the completed job

#### `FailJob`
Marks a job as failed with an error message.

**Request**: `FailJobRequest`
- `job_id`: ID of the failed job
- `error_message`: Description of the failure

#### `RetryJob`
Retries a failed job if within max retry limits. [This is an internally called endpoint]

**Request**: `RetryJobRequest`
- `job_id`: ID of the job to retry

## Storage Layers

### Redis (Primary)
- Fast job queuing and dequeuing
- Used for active job processing
- Caching jobs for faster retrieval

### PostgreSQL (Persistence)
- Stores job metadata and history
- Provides durability and job tracking
- Final fallback for job operations

## Job Lifecycle

1. **Enqueued**: Job added to queue with "pending" status
2. **Dequeued**: Worker pulls job, status becomes "running"
3. **Processing**: Worker executes job logic
4. **Completion**: 
   - Success: `complete_job()` → status "completed"
   - Failure: `fail_job()` → retry if attempts remain, else "failed"
   - Retry: `retry_job()` → back to "Queued" with incremented retry count and exponential backoff

## Configuration

### Environment Variables

- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string (optional)
- `SERVER_ADDR`: gRPC server port (default: 50051)

### Job Defaults

- **Priority**: 2 (medium)
- **Max Retries**: 2
- **Delay**: 30 seconds

## Language Support

Stackduck uses gRPC for cross-language compatibility. Generate clients for:

- **Python**: `pip install grpcio-tools`
- **Node.js**: `npm install @grpc/grpc-js`
- **Go**: `go install google.golang.org/protobuf/cmd/protoc-gen-go`
- **Java**: Maven/Gradle gRPC plugins
- **C#**: `dotnet add package Grpc.Tools`

## Performance

- **Throughput**: 10K+ jobs/second (Redis-backed)
- **Latency**: Sub-millisecond job enqueue/dequeue
- **Concurrency**: Thousands of concurrent workers
- **Scalability**: Horizontal scaling via multiple server instances

## Monitoring

Built-in metrics and logging for:
- Job processing rates
- Queue depths
- Worker activity
- Error rates
- Storage layer health

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run tests: `cargo test`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.

