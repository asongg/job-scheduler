import assert from "node:assert/strict";
import test from "node:test";
import { JobScheduler } from "../controller/src/scheduler";

test("submitJob deduplicates active jobs by idempotency key", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3 });

  const first = scheduler.submitJob("same-key", "sleep:10");
  const second = scheduler.submitJob("same-key", "sleep:20");

  assert.equal(first.ok, true);
  assert.equal(second.ok, true);
  assert.equal(second.jobId, first.jobId);
  assert.equal(scheduler.queueDepth(), 1);
  assert.equal(scheduler.metrics().submitted, 1);
  assert.equal(scheduler.getJob(first.jobId)?.payload, "sleep:10");
});

test("requestJob assigns queued work and reportResult records success", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({ maxAttempts: 3, now: () => timestamp });
  const submitted = scheduler.submitJob("idem-1", "fib:5");

  timestamp = 1250;
  const assigned = scheduler.requestJob("worker-1");

  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected job assignment");
  assert.equal(assigned.job.jobId, submitted.jobId);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "ASSIGNED");
  assert.equal(scheduler.getJob(submitted.jobId)?.assignedTo, "worker-1");
  assert.equal(scheduler.getJob(submitted.jobId)?.assignedAtMs, 1250);
  assert.equal(scheduler.metrics().assigned, 1);

  const report = scheduler.reportResult(submitted.jobId, "idem-1", true);

  assert.equal(report.ok, true);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "SUCCEEDED");
  assert.equal(scheduler.metrics().succeeded, 1);
  assert.equal(scheduler.metrics().succeededCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
});

test("requestJobs assigns up to the requested batch size", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3 });
  const first = scheduler.submitJob("idem-1", "sleep:1");
  const second = scheduler.submitJob("idem-2", "sleep:1");
  scheduler.submitJob("idem-3", "sleep:1");

  const jobs = scheduler.requestJobs("worker-1", 2);

  assert.deepEqual(jobs.map((job) => job.jobId), [first.jobId, second.jobId]);
  assert.equal(scheduler.metrics().assigned, 2);
  assert.equal(scheduler.metrics().queued, 1);
  assert.equal(scheduler.queueDepth(), 1);
});

test("failed jobs retry until max attempts, then become terminal failures", () => {
  const scheduler = new JobScheduler({ maxAttempts: 2 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");

  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  scheduler.reportResult(submitted.jobId, "idem-1", false);

  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 1);
  assert.equal(scheduler.metrics().retriedCount, 1);
  assert.equal(scheduler.metrics().failedCount, 1);

  const second = scheduler.requestJob("worker-1");
  assert.equal(second.hasJob, true);
  scheduler.reportResult(submitted.jobId, "idem-1", false);

  assert.equal(scheduler.getJob(submitted.jobId)?.state, "FAILED");
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 2);
  assert.equal(scheduler.metrics().failed, 1);
  assert.equal(scheduler.metrics().failedCount, 2);
  assert.equal(scheduler.metrics().retriedCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
});

test("requeueAssignedJobs returns assigned jobs to the queue", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);

  const requeued = scheduler.requeueAssignedJobs([submitted.jobId, "missing-job"]);

  assert.equal(requeued, 1);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
  assert.equal(scheduler.metrics().queued, 1);
  assert.equal(scheduler.faultMetrics().jobsRequeuedFromDeadWorkers, 1);

  const reassigned = scheduler.requestJob("worker-2");
  assert.equal(reassigned.hasJob, true);
  if (!reassigned.hasJob) throw new Error("expected reassignment");
  assert.equal(reassigned.job.jobId, submitted.jobId);
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 2);
});

test("duplicate result reports do not increment success counters twice", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  scheduler.requestJob("worker-1");

  scheduler.reportResult(submitted.jobId, "idem-1", true);
  scheduler.reportResult(submitted.jobId, "idem-1", true);

  assert.equal(scheduler.metrics().succeeded, 1);
  assert.equal(scheduler.metrics().succeededCount, 1);
});
