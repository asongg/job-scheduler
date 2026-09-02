import assert from "node:assert/strict";
import test from "node:test";
import { JobScheduler } from "../controller/src/scheduler";

test("submitJob deduplicates active jobs by idempotency key", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });

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
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000, now: () => timestamp });
  const submitted = scheduler.submitJob("idem-1", "fib:5");

  timestamp = 1250;
  const assigned = scheduler.requestJob("worker-1");

  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected job assignment");
  assert.equal(assigned.job.jobId, submitted.jobId);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "ASSIGNED");
  assert.equal(scheduler.getJob(submitted.jobId)?.assignedTo, "worker-1");
  assert.equal(scheduler.getJob(submitted.jobId)?.assignedAtMs, 1250);
  assert.equal(scheduler.getJob(submitted.jobId)?.leaseExpiresAtMs, 31250);
  assert.equal(typeof assigned.job.attemptId, "string");
  assert.equal(assigned.job.leaseExpiresAtMs, 31250);
  assert.equal(scheduler.metrics().assigned, 1);

  const report = scheduler.reportResult("worker-1", submitted.jobId, "idem-1", assigned.job.attemptId, true);

  assert.equal(report.ok, true);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "SUCCEEDED");
  assert.equal(scheduler.metrics().succeeded, 1);
  assert.equal(scheduler.metrics().succeededCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
});

test("requestJobs assigns up to the requested batch size", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
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
  const scheduler = new JobScheduler({ maxAttempts: 2, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");

  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  if (!first.hasJob) throw new Error("expected first assignment");
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", first.job.attemptId, false);

  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 1);
  assert.equal(scheduler.metrics().retriedCount, 1);
  assert.equal(scheduler.metrics().failedCount, 1);

  const second = scheduler.requestJob("worker-1");
  assert.equal(second.hasJob, true);
  if (!second.hasJob) throw new Error("expected second assignment");
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", second.job.attemptId, false);

  assert.equal(scheduler.getJob(submitted.jobId)?.state, "FAILED");
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 2);
  assert.equal(scheduler.metrics().failed, 1);
  assert.equal(scheduler.metrics().failedCount, 2);
  assert.equal(scheduler.metrics().retriedCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
});

test("failed retries wait for backoff without blocking fresh ready jobs", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({
    maxAttempts: 3,
    leaseMs: 30000,
    retryBackoffBaseMs: 100,
    retryBackoffMaxMs: 1000,
    now: () => timestamp,
  });
  const failed = scheduler.submitJob("failed", "sleep:1");
  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  if (!first.hasJob) throw new Error("expected first assignment");

  scheduler.reportResult("worker-1", failed.jobId, "failed", first.job.attemptId, false);
  assert.equal(scheduler.getJob(failed.jobId)?.availableAtMs, 1100);
  assert.deepEqual(scheduler.queueMetrics(), { total: 1, ready: 0, delayed: 1 });

  timestamp = 1050;
  const fresh = scheduler.submitJob("fresh", "sleep:2");
  assert.deepEqual(scheduler.queueMetrics(), { total: 2, ready: 1, delayed: 1 });
  const ready = scheduler.requestJob("worker-2");
  assert.equal(ready.hasJob, true);
  if (!ready.hasJob) throw new Error("expected fresh assignment");
  assert.equal(ready.job.jobId, fresh.jobId);
  assert.deepEqual(scheduler.queueMetrics(), { total: 1, ready: 0, delayed: 1 });

  assert.deepEqual(scheduler.requestJob("worker-3"), { hasJob: false });

  timestamp = 1100;
  assert.deepEqual(scheduler.queueMetrics(), { total: 1, ready: 1, delayed: 0 });
  const retried = scheduler.requestJob("worker-3");
  assert.equal(retried.hasJob, true);
  if (!retried.hasJob) throw new Error("expected retry assignment");
  assert.equal(retried.job.jobId, failed.jobId);
  assert.equal(scheduler.getJob(failed.jobId)?.attempts, 2);
});

test("retry backoff grows exponentially and caps at the configured max", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({
    maxAttempts: 4,
    leaseMs: 30000,
    retryBackoffBaseMs: 100,
    retryBackoffMaxMs: 150,
    now: () => timestamp,
  });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");

  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  if (!first.hasJob) throw new Error("expected first assignment");
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", first.job.attemptId, false);
  assert.equal(scheduler.getJob(submitted.jobId)?.availableAtMs, 1100);

  timestamp = 1100;
  const second = scheduler.requestJob("worker-1");
  assert.equal(second.hasJob, true);
  if (!second.hasJob) throw new Error("expected second assignment");
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", second.job.attemptId, false);
  assert.equal(scheduler.getJob(submitted.jobId)?.availableAtMs, 1250);
});

test("requeueAssignedJobs returns assigned jobs to the queue", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);

  const requeued = scheduler.requeueAssignedJobs([submitted.jobId, "missing-job"]);

  assert.deepEqual(requeued, { requeued: 1, failed: 0 });
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
  assert.equal(scheduler.metrics().queued, 1);
  assert.equal(scheduler.faultMetrics().jobsRequeuedFromDeadWorkers, 1);

  const reassigned = scheduler.requestJob("worker-2");
  assert.equal(reassigned.hasJob, true);
  if (!reassigned.hasJob) throw new Error("expected reassignment");
  assert.equal(reassigned.job.jobId, submitted.jobId);
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 2);
});

test("dead-worker recovery marks exhausted jobs failed instead of requeueing forever", () => {
  const scheduler = new JobScheduler({ maxAttempts: 1, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);

  const recovery = scheduler.requeueAssignedJobs([submitted.jobId]);

  assert.deepEqual(recovery, { requeued: 0, failed: 1 });
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "FAILED");
  assert.equal(scheduler.getJob(submitted.jobId)?.lastFailureReason, "worker dead");
  assert.equal(scheduler.metrics().failed, 1);
  assert.equal(scheduler.metrics().failedCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
  assert.equal(scheduler.faultMetrics().jobsFailedFromDeadWorkers, 1);
});

test("failed jobs can be listed and replayed back to the queue", () => {
  const scheduler = new JobScheduler({ maxAttempts: 1, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", assigned.job.attemptId, false);

  assert.deepEqual(scheduler.listFailedJobs(), [{
    jobId: submitted.jobId,
    idempotencyKey: "idem-1",
    payload: "sleep:1",
    attempts: 1,
    lastFailureReason: "reported failure",
  }]);

  assert.deepEqual(scheduler.replayFailedJob(submitted.jobId), { ok: true, jobId: submitted.jobId });
  assert.deepEqual(scheduler.listFailedJobs(), []);
  assert.equal(scheduler.queueDepth(), 1);
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 0);
  assert.equal(scheduler.getJob(submitted.jobId)?.lastFailureReason, undefined);

  const replayed = scheduler.requestJob("worker-2");
  assert.equal(replayed.hasJob, true);
  if (!replayed.hasJob) throw new Error("expected replay assignment");
  assert.equal(replayed.job.jobId, submitted.jobId);
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 1);
});

test("replayFailedJob rejects active jobs and can preserve attempts", () => {
  const activeScheduler = new JobScheduler({ maxAttempts: 2, leaseMs: 30000 });
  const active = activeScheduler.submitJob("active", "sleep:1");
  assert.deepEqual(activeScheduler.replayFailedJob(active.jobId), { ok: false, jobId: "" });

  const scheduler = new JobScheduler({ maxAttempts: 2, leaseMs: 30000 });
  const failed = scheduler.submitJob("failed", "sleep:2");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");
  scheduler.reportResult("worker-1", failed.jobId, "failed", assigned.job.attemptId, false);
  const assignedAgain = scheduler.requestJob("worker-1");
  assert.equal(assignedAgain.hasJob, true);
  if (!assignedAgain.hasJob) throw new Error("expected reassignment");
  scheduler.reportResult("worker-1", failed.jobId, "failed", assignedAgain.job.attemptId, false);

  assert.equal(scheduler.getJob(failed.jobId)?.attempts, 2);
  assert.deepEqual(scheduler.replayFailedJob(failed.jobId, true), { ok: true, jobId: failed.jobId });
  assert.equal(scheduler.getJob(failed.jobId)?.attempts, 2);
});

test("duplicate result reports do not increment success counters twice", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");

  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", assigned.job.attemptId, true);
  scheduler.reportResult("worker-1", submitted.jobId, "idem-1", assigned.job.attemptId, true);

  assert.equal(scheduler.metrics().succeeded, 1);
  assert.equal(scheduler.metrics().succeededCount, 1);
});

test("wrong-worker result reports are rejected even with a matching attempt token", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");

  const report = scheduler.reportResult("worker-2", submitted.jobId, "idem-1", assigned.job.attemptId, true);

  assert.deepEqual(report, { ok: false });
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "ASSIGNED");
  assert.equal(scheduler.metrics().succeededCount, 0);
});

test("stale attempt results are rejected after a job is requeued and reassigned", () => {
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  if (!first.hasJob) throw new Error("expected first assignment");

  scheduler.requeueAssignedJobs([submitted.jobId]);
  const second = scheduler.requestJob("worker-2");
  assert.equal(second.hasJob, true);
  if (!second.hasJob) throw new Error("expected reassignment");

  const staleReport = scheduler.reportResult("worker-1", submitted.jobId, "idem-1", first.job.attemptId, true);
  assert.equal(staleReport.ok, false);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "ASSIGNED");
  assert.equal(scheduler.getJob(submitted.jobId)?.assignedTo, "worker-2");
  assert.equal(scheduler.metrics().succeededCount, 0);

  const freshReport = scheduler.reportResult("worker-2", submitted.jobId, "idem-1", second.job.attemptId, true);
  assert.equal(freshReport.ok, true);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "SUCCEEDED");
});

test("expired leases are requeued and invalidate the old attempt", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 500, now: () => timestamp });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const first = scheduler.requestJob("worker-1");
  assert.equal(first.hasJob, true);
  if (!first.hasJob) throw new Error("expected first assignment");

  timestamp = 1500;
  assert.deepEqual(scheduler.requeueExpiredLeases(), []);

  timestamp = 1501;
  assert.deepEqual(scheduler.requeueExpiredLeases(), [{ jobId: submitted.jobId, workerId: "worker-1", requeued: true }]);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
  assert.equal(scheduler.faultMetrics().jobsRequeuedFromExpiredLeases, 1);

  const staleReport = scheduler.reportResult("worker-1", submitted.jobId, "idem-1", first.job.attemptId, true);
  assert.equal(staleReport.ok, false);

  const second = scheduler.requestJob("worker-2");
  assert.equal(second.hasJob, true);
  if (!second.hasJob) throw new Error("expected reassignment");
  assert.notEqual(second.job.attemptId, first.job.attemptId);
  assert.equal(scheduler.getJob(submitted.jobId)?.attempts, 2);
});

test("expired leases mark exhausted jobs failed instead of requeueing forever", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({ maxAttempts: 1, leaseMs: 500, now: () => timestamp });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);

  timestamp = 1501;
  const expired = scheduler.requeueExpiredLeases();

  assert.deepEqual(expired, [{ jobId: submitted.jobId, workerId: "worker-1", requeued: false }]);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "FAILED");
  assert.equal(scheduler.getJob(submitted.jobId)?.lastFailureReason, "lease expired");
  assert.equal(scheduler.metrics().failed, 1);
  assert.equal(scheduler.metrics().failedCount, 1);
  assert.equal(scheduler.queueDepth(), 0);
  assert.equal(scheduler.faultMetrics().jobsFailedFromExpiredLeases, 1);
});

test("lease renewal extends assigned work and skips stale expiration markers", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 500, now: () => timestamp });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");
  assert.equal(assigned.job.leaseExpiresAtMs, 1500);

  timestamp = 1400;
  const renewed = scheduler.extendLease("worker-1", submitted.jobId, "idem-1", assigned.job.attemptId);

  assert.deepEqual(renewed, { ok: true, leaseExpiresAtMs: 1900 });
  assert.equal(scheduler.getJob(submitted.jobId)?.leaseExpiresAtMs, 1900);

  timestamp = 1501;
  assert.deepEqual(scheduler.requeueExpiredLeases(), []);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "ASSIGNED");

  assert.deepEqual(
    scheduler.extendLease("worker-2", submitted.jobId, "idem-1", assigned.job.attemptId),
    { ok: false, leaseExpiresAtMs: 0 }
  );

  timestamp = 1901;
  assert.deepEqual(scheduler.requeueExpiredLeases(), [{ jobId: submitted.jobId, workerId: "worker-1", requeued: true }]);
  assert.equal(scheduler.getJob(submitted.jobId)?.state, "QUEUED");
});

test("snapshots restore queued order, idempotency, and assigned leases", () => {
  let timestamp = 1000;
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 500, now: () => timestamp });
  const assignedSubmission = scheduler.submitJob("assigned-key", "sleep:1");
  const queuedSubmission = scheduler.submitJob("queued-key", "sleep:2");
  const assigned = scheduler.requestJob("worker-1");
  assert.equal(assigned.hasJob, true);
  if (!assigned.hasJob) throw new Error("expected assignment");

  const restored = JobScheduler.fromSnapshot(
    { maxAttempts: 3, leaseMs: 500, now: () => timestamp },
    scheduler.toSnapshot()
  );

  assert.equal(restored.submitJob("assigned-key", "sleep:ignored").jobId, assignedSubmission.jobId);
  assert.equal(restored.submitJob("queued-key", "sleep:ignored").jobId, queuedSubmission.jobId);

  timestamp = 1200;
  const queued = restored.requestJob("worker-2");
  assert.equal(queued.hasJob, true);
  if (!queued.hasJob) throw new Error("expected queued job");
  assert.equal(queued.job.jobId, queuedSubmission.jobId);

  timestamp = 1501;
  assert.deepEqual(restored.requeueExpiredLeases(), [{ jobId: assignedSubmission.jobId, workerId: "worker-1", requeued: true }]);
  assert.equal(restored.reportResult("worker-1", assignedSubmission.jobId, "assigned-key", assigned.job.attemptId, true).ok, false);
});
