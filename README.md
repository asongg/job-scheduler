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
- Idempotent job completion
- Chaos testing (induced worker failure)

---

## Results

### Environment
- 3 workers
- Worker capacity = 2 jobs each
- Heartbeat interval = 1s
- Heartbeat timeout = 3s
