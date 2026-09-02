# Distributed gRPC Job Scheduler

A fault-tolerant distributed job scheduler, built with **TypeScript + gRPC**. Added features such as worker heartbeats, failure detection, at-least-once execution, and performance benchmarking.

---

## The Architecture:

### Controller (Master)

- gRPC server
- Maintains worker registry with heartbeats
- Tracks job state machine (QUEUED → ASSIGNED → SUCCEEDED/FAILED)
- Automatically detects failed workers
- Requeues in-flight jobs from dead workers

### Workers

- Register with controller
- Send periodic heartbeats
- Pull jobs when below capacity
- Execute payload (`sleep:N` or `fib:N`)
- Report results back via gRPC

### How communication flows:

- Pull-based scheduling (workers request jobs)
- Batched submit, request, and result APIs for lower per-job RPC overhead
- At-least-once execution semantics
- Idempotency keys prevent duplicate completion

---

## Some features (more to be added soon):

- gRPC-based distributed control plane
- Worker heartbeats with configurable timeout
- Automatic failure detection
- Automatic requeue of in-flight jobs
- Capacity-aware scheduling per worker
- Retry logic for reported failures, expired leases, and dead-worker recovery
- Idempotent job submission and completion
- Attempt leases reject stale worker results after requeue/reassignment
- Worker lease renewal for long-running attempts
- Dead-letter listing and replay for failed jobs
- Exponential retry backoff to avoid hot-looping failing jobs
- Real-time metrics endpoint
- Benchmark script for throughput measurement
- Chaos testing (induced worker failure)

---

## Useful Commands

```bash
npm test
TEST_DATABASE_URL=postgres://scheduler:scheduler@localhost:5432/scheduler npm run test:postgres
npm run typecheck
docker compose up
N=1000 MODE=sleep SUBMIT_BATCH_SIZE=100 npx ts-node scripts/benchmark.ts
```

## Configuration

- `HEARTBEAT_INTERVAL_MS`: interval returned to workers for heartbeat cadence
- `HEARTBEAT_TIMEOUT_MS`: controller timeout before marking a worker dead
- `MAX_ATTEMPTS`: max attempts before a job becomes a terminal failure
- `JOB_LEASE_MS`: max time an assigned attempt can run before being requeued
- `RETRY_BACKOFF_BASE_MS`: first retry delay; each later retry doubles from this base
- `RETRY_BACKOFF_MAX_MS`: cap for retry backoff delays
- `METRICS_PORT`: HTTP metrics server port
- `GRPC_ADDR`: controller bind address
- `SCHEDULER_BACKEND`: `memory` or `postgres`
- `DATABASE_URL`: optional Postgres snapshot persistence connection string
- `STATE_FILE`: optional controller snapshot path for restart recovery
- `CAPACITY`: worker-local concurrency
- `IDLE_POLL_MS`: worker delay when no jobs are available
- `FULL_POLL_MS`: worker delay while at capacity
- `LEASE_RENEW_INTERVAL_MS`: how often workers check running attempts for renewal
- `LEASE_RENEW_THRESHOLD_MS`: renew attempts when this close to lease expiry

## Storage Notes

The controller runs with `SCHEDULER_BACKEND=memory` by default. In memory mode, set `STATE_FILE` to enable JSON snapshot recovery, or set `DATABASE_URL` to store the same snapshot in Postgres.

Use `SCHEDULER_BACKEND=postgres DATABASE_URL=postgres://...` to run jobs directly from normalized Postgres rows. In that mode, Postgres owns submit, claim, report, idempotency, attempt leases, lease renewal, expired lease requeue, and job metrics.

Retries are delayed by exponential backoff. Fresh jobs can still run while failed retries are waiting for their next available time.

The metrics endpoint keeps `queueDepth` for compatibility and also exposes `queue.total`, `queue.ready`, and `queue.delayed` so delayed retries are visible separately from work that can run immediately.

Failed jobs stay queryable through the scheduler API and can be replayed back into the queue. By default, replay resets the attempt counter so the job gets a fresh retry budget; set `preserveAttempts` to keep the old count.

[db/postgres/schema.sql](db/postgres/schema.sql) contains the Postgres schema. Snapshot persistence uses `scheduler_snapshots`; the live Postgres backend uses `jobs` and `job_attempts`.

---

## Performance Results

### Environment

- 3 workers
- Worker capacity = 2 jobs each
- Heartbeat interval = 1s
- Heartbeat timeout = 3s
