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
Client (NestJS / Flask / CLI / etc)
         |
         v
   +-------------+
   | StackDuck gRPC API (Rust)
   +-------------+
         |
         v
+------------------------+
| Redis Queue / In-Mem   |  ---> Optional fallback queue
+------------------------+
         |
         v
   +-------------+
   |  Worker(s)  |  <-- Can be Rust or Node-based workers
   +-------------+
         |
         v
  Job execution + Result persistence (Postgres)
