# 🐣 StackDuck

A fast, lightweight distributed task queue system built in **Rust** using **gRPC**, **Redis**, and **PostgreSQL**. StackDuck lets you enqueue tasks from any gRPC client (like NestJS) and run background jobs using dynamic, scalable workers.

---

## 🚀 Features

- 🦀 Written in Rust
- ⚡️ Supports gRPC API for enqueueing jobs
- 🧠 Pluggable worker architecture (Rust or Node/NestJS)
- 💾 PostgreSQL for persistence
- 🧰 Redis or in-memory fallback for job queues
- 🛡️ Designed for horizontal scalability

---

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
