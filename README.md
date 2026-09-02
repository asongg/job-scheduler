# Distributed gRPC Job Scheduler

A fault-tolerant distributed job scheduler built with **TypeScript + gRPC**, renewable attempt leases, retry backoff, dead-letter replay, worker heartbeats, and optional Postgres-backed durability.

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
- Benchmark script for throughput and latency measurement
- Chaos testing (induced worker failure)

---

## Useful Commands

```bash
npm test
TEST_DATABASE_URL=postgres://scheduler:scheduler@localhost:5432/scheduler npm run test:postgres
npm run typecheck
docker compose up
N=1000 MODE=sleep SUBMIT_BATCH_SIZE=100 npx ts-node scripts/benchmark.ts
N=1000 MODE=sleep SUBMIT_BATCH_SIZE=100 npm run benchmark
RESULT_BATCH_SIZE=1 N=1000 MODE=sleep SUBMIT_BATCH_SIZE=100 JSON_OUTPUT=1 npm run --silent benchmark > benchmark-baseline.json
MATRIX_REPEATS=5 MATRIX_JOBS=5000,10000 MATRIX_RESULT_BATCHES=1,50 npm run benchmark:matrix
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
- `REGISTER_RETRY_MS`: worker delay before retrying registration if the controller is not ready
- `LEASE_RENEW_INTERVAL_MS`: how often workers check running attempts for renewal
- `LEASE_RENEW_THRESHOLD_MS`: renew attempts when this close to lease expiry
- `RESULT_BATCH_SIZE`: worker result buffer size before flushing `ReportResults`
- `RESULT_FLUSH_INTERVAL_MS`: max worker result buffering delay before flushing
- `POLL_INTERVAL_MS`: benchmark metrics polling interval
- `JSON_OUTPUT`: set to `1` for machine-readable benchmark output

## Storage Notes

The controller runs with `SCHEDULER_BACKEND=memory` by default when launched directly. In memory mode, set `STATE_FILE` to enable JSON snapshot recovery, or set `DATABASE_URL` to store the same snapshot in Postgres.

Use `SCHEDULER_BACKEND=postgres DATABASE_URL=postgres://...` to run jobs directly from normalized Postgres rows. In that mode, Postgres owns submit, claim, report, idempotency, attempt leases, lease renewal, expired lease requeue, and job metrics.

`docker compose up` starts Postgres, the controller with `SCHEDULER_BACKEND=postgres`, three workers, gRPC on `localhost:50051`, and metrics on `localhost:8080/metrics`.

Retries are delayed by exponential backoff. Fresh jobs can still run while failed retries are waiting for their next available time.

The metrics endpoint keeps `queueDepth` for compatibility and also exposes `queue.total`, `queue.ready`, and `queue.delayed` so delayed retries are visible separately from work that can run immediately.

Failed jobs stay queryable through the scheduler API and can be replayed back into the queue. By default, replay resets the attempt counter so the job gets a fresh retry budget; set `preserveAttempts` to keep the old count.

[db/postgres/schema.sql](db/postgres/schema.sql) contains the Postgres schema. Snapshot persistence uses `scheduler_snapshots`; the live Postgres backend uses `jobs` and `job_attempts`.

---

## Performance Results

The benchmark reports submit latency, estimated end-to-end completion latency, throughput, submit RPC count, and metrics poll count. Completion latency is estimated from metrics deltas, so it is best used for relative comparisons across benchmark runs rather than exact per-job tracing.

### Environment

- MacBook local Docker Desktop
- Postgres backend via `docker compose`
- 3 workers
- Worker capacity = 50 jobs each
- Heartbeat interval = 1s
- Heartbeat timeout = 3s
- Workload = `sleep:10`
- Submit batch size = 100
- Metrics poll interval = 25ms

### Current Results

Each configuration below was run five times against a fresh `jobs`/`job_attempts` table.

| Jobs | Result batch size | Median throughput | Min throughput | Max throughput | Std dev | Median p95 completion latency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 1 | 1,573.53 jobs/sec | 1,126.03 | 1,743.90 | 207.47 | 3,058.81ms |
| 5,000 | 50 | 1,983.75 jobs/sec | 1,688.05 | 2,307.43 | 230.21 | 2,391.05ms |
| 10,000 | 1 | 1,353.22 jobs/sec | 1,244.48 | 1,424.25 | 60.76 | 7,107.95ms |
| 10,000 | 50 | 1,959.07 jobs/sec | 1,806.73 | 2,318.41 | 186.41 | 4,901.31ms |

Worker-side result batching improved median throughput by about 26% in the 5,000-job run and 45% in the 10,000-job run.

### Baseline Template

Run:

```bash
docker compose up
RESULT_BATCH_SIZE=1 docker compose up
N=10000 MODE=sleep SUBMIT_BATCH_SIZE=100 JSON_OUTPUT=1 npm run --silent benchmark > benchmark-baseline.json
```

Recommended comparisons:

| Experiment | What to compare |
| --- | --- |
| start 1, 2, or 3 worker services, or add more worker service entries | throughput and p95 completion latency |
| `WORKER_CAPACITY=1/2/4/8 docker compose up` | capacity scaling |
| `SUBMIT_BATCH_SIZE=1/10/50/100` | submit batching impact |
| `RESULT_BATCH_SIZE=1/10/50 RESULT_FLUSH_INTERVAL_MS=25` | result batching impact |
| `SCHEDULER_BACKEND=memory` vs `postgres` | backend throughput and latency |
| kill one worker during a run | recovery time and recovered jobs |
