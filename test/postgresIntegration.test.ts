import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { Pool } from "pg";
import { PostgresJobStore } from "../controller/src/postgresJobStore";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;

function quoteIdent(value: string): string {
  return `"${value.replace(/"/g, "\"\"")}"`;
}

test("PostgresJobStore runs submit, claim, report, and metrics against a live database", {
  skip: TEST_DATABASE_URL ? false : "set TEST_DATABASE_URL to run live Postgres integration",
}, async () => {
  const schema = `test_scheduler_${randomUUID().replace(/-/g, "")}`;
  const adminPool = new Pool({ connectionString: TEST_DATABASE_URL });
  const schemaPool = new Pool({
    connectionString: TEST_DATABASE_URL,
    options: `-c search_path=${schema},public`,
  });

  try {
    await adminPool.query(`CREATE SCHEMA ${quoteIdent(schema)}`);

    const store = new PostgresJobStore(schemaPool, {
      maxAttempts: 2,
      leaseMs: 150,
      now: Date.now,
    });
    await store.initialize();

    const first = await store.submitJob("idem-1", "sleep:1");
    const duplicate = await store.submitJob("idem-1", "sleep:ignored");
    const second = await store.submitJob("idem-2", "sleep:2");

    assert.equal(first.ok, true);
    assert.equal(duplicate.jobId, first.jobId);
    assert.equal(second.ok, true);

    const claimed = await store.requestJobs("worker-1", 2);
    assert.equal(claimed.length, 2);
    assert.deepEqual(claimed.map((job) => job.jobId), [first.jobId, second.jobId]);

    const staleReport = await store.reportResult(
      "worker-2",
      claimed[0]!.jobId,
      claimed[0]!.idempotencyKey,
      claimed[0]!.attemptId,
      true,
      "wrong worker",
      1
    );
    assert.equal(staleReport.ok, false);

    const success = await store.reportResult(
      "worker-1",
      claimed[0]!.jobId,
      claimed[0]!.idempotencyKey,
      claimed[0]!.attemptId,
      true,
      "ok",
      1
    );
    assert.equal(success.ok, true);

    await new Promise((resolve) => setTimeout(resolve, 75));
    const renewed = await store.extendLease(
      "worker-1",
      claimed[1]!.jobId,
      claimed[1]!.idempotencyKey,
      claimed[1]!.attemptId
    );
    assert.equal(renewed.ok, true);

    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual(await store.requeueExpiredLeases(), []);

    await new Promise((resolve) => setTimeout(resolve, 75));
    const expired = await store.requeueExpiredLeases();
    assert.deepEqual(expired, [{ jobId: claimed[1]!.jobId, workerId: "worker-1", requeued: true }]);

    const retried = await store.requestJob("worker-2");
    assert.equal(retried.hasJob, true);
    if (!retried.hasJob) throw new Error("expected retried job");
    const finalFailure = await store.reportResult(
      "worker-2",
      retried.job.jobId,
      retried.job.idempotencyKey,
      retried.job.attemptId,
      false,
      "still broken",
      1
    );
    assert.equal(finalFailure.ok, true);

    const failedJobs = await store.listFailedJobs();
    assert.deepEqual(failedJobs.map((job) => job.jobId), [claimed[1]!.jobId]);

    const replayed = await store.replayFailedJob(claimed[1]!.jobId);
    assert.deepEqual(replayed, { ok: true, jobId: claimed[1]!.jobId });

    const metrics = await store.metrics();
    assert.equal(metrics.totalTracked, 2);
    assert.equal(metrics.succeeded, 1);
    assert.equal(metrics.queued, 1);
    assert.equal(metrics.retriedCount, 1);
  } finally {
    await schemaPool.end();
    await adminPool.query(`DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`);
    await adminPool.end();
  }
});
