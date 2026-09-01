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
- Retry logic (max 3 attempts per job)
- Idempotent job submission and completion
- Real-time metrics endpoint
- Benchmark script for throughput measurement
- Chaos testing (induced worker failure)

---

## Useful Commands

```bash
npm test
npm run typecheck
N=1000 MODE=sleep SUBMIT_BATCH_SIZE=100 npx ts-node scripts/benchmark.ts
```

## Configuration

- `HEARTBEAT_INTERVAL_MS`: interval returned to workers for heartbeat cadence
- `HEARTBEAT_TIMEOUT_MS`: controller timeout before marking a worker dead
- `MAX_ATTEMPTS`: max attempts before a job becomes a terminal failure
- `METRICS_PORT`: HTTP metrics server port
- `GRPC_ADDR`: controller bind address
- `CAPACITY`: worker-local concurrency
- `IDLE_POLL_MS`: worker delay when no jobs are available
- `FULL_POLL_MS`: worker delay while at capacity

---

## Performance Results

### Environment

- 3 workers
- Worker capacity = 2 jobs each
- Heartbeat interval = 1s
- Heartbeat timeout = 3s
