import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJobStore } from "../controller/src/jobStore";
import { metricsSnapshot } from "../controller/src/metrics";
import { WorkerRegistry } from "../controller/src/registry";
import { JobScheduler } from "../controller/src/scheduler";

test("metricsSnapshot includes total, ready, and delayed queue counts", async () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({
    maxAttempts: 3,
    leaseMs: 30000,
    retryBackoffBaseMs: 100,
    retryBackoffMaxMs: 1000,
    now: () => timestamp,
  });
  const store = new MemoryJobStore(scheduler);
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => timestamp });
  const delayed = scheduler.submitJob("delayed", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");

  scheduler.reportResult("worker-1", delayed.jobId, "delayed", assigned.job.attemptId, false);
  timestamp = 1050;
  scheduler.submitJob("ready", "sleep:2");

  const snapshot = await metricsSnapshot(store, registry);

  assert.equal(snapshot.queueDepth, 2);
  assert.deepEqual(snapshot.queue, { total: 2, ready: 1, delayed: 1 });
});
