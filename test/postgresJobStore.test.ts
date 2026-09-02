import assert from "node:assert/strict";
import test from "node:test";
import { PostgresJobStore, type QueryResultLike, type TransactionClient } from "../controller/src/postgresJobStore";

type QueryCall = {
  text: string;
  values?: unknown[] | undefined;
};

class MockClient implements TransactionClient {
  readonly calls: QueryCall[] = [];
  released = false;

  constructor(private readonly handler: (text: string, values?: unknown[]) => QueryResultLike<any>) {}

  async query<T = any>(text: string, values?: unknown[]): Promise<QueryResultLike<T>> {
    this.calls.push({ text, values });
    return this.handler(text, values) as QueryResultLike<T>;
  }

  release(): void {
    this.released = true;
  }
}

class MockPool {
  readonly client: MockClient;
  readonly calls: QueryCall[] = [];

  constructor(private readonly handler: (text: string, values?: unknown[]) => QueryResultLike<any>) {
    this.client = new MockClient(handler);
  }

  async query<T = any>(text: string, values?: unknown[]): Promise<QueryResultLike<T>> {
    this.calls.push({ text, values });
    return this.handler(text, values) as QueryResultLike<T>;
  }

  async connect(): Promise<TransactionClient> {
    return this.client;
  }
}

function compact(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

function last<T>(values: T[]): T | undefined {
  return values[values.length - 1];
}

test("PostgresJobStore initializes normalized job tables", async () => {
  const pool = new MockPool(() => ({ rows: [] }));
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  await store.initialize();

  const sql = compact(pool.calls[0]?.text || "");
  assert.match(sql, /CREATE TABLE IF NOT EXISTS jobs/);
  assert.match(sql, /CREATE TABLE IF NOT EXISTS job_attempts/);
  assert.match(sql, /CREATE INDEX IF NOT EXISTS jobs_ready_available_idx/);
  assert.match(sql, /available_at timestamptz/);
});

test("PostgresJobStore submitJob inserts by idempotency key and returns existing ids", async () => {
  const pool = new MockPool((text, values) => {
    assert.match(compact(text), /ON CONFLICT \(idempotency_key\) DO NOTHING/);
    assert.deepEqual(values, ["job-1", "idem-1", "sleep:1"]);
    return { rows: [{ job_id: "job-1" }] };
  });
  const store = new PostgresJobStore(pool, {
    maxAttempts: 3,
    leaseMs: 30000,
    newJobId: () => "job-1",
  });

  const result = await store.submitJob("idem-1", "sleep:1");

  assert.deepEqual(result, { ok: true, jobId: "job-1" });
});

test("PostgresJobStore requestJobs claims ready rows inside one transaction", async () => {
  let attempt = 0;
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("FOR UPDATE SKIP LOCKED") && sql.includes("WHERE state = 'QUEUED'")) {
      assert.deepEqual(values, [2]);
      assert.match(sql, /available_at IS NULL OR available_at <= now\(\)/);
      return {
        rows: [
          { job_id: "job-1", idempotency_key: "idem-1", payload: "sleep:1" },
          { job_id: "job-2", idempotency_key: "idem-2", payload: "sleep:2" },
        ],
      };
    }
    if (sql.startsWith("UPDATE jobs") && sql.includes("RETURNING job_id")) {
      const payload = values?.[0] === "job-1" ? "sleep:1" : "sleep:2";
      return {
        rows: [{
          job_id: values?.[0],
          idempotency_key: values?.[0] === "job-1" ? "idem-1" : "idem-2",
          payload,
          current_attempt_id: values?.[2],
          lease_expires_at: values?.[3],
        }],
      };
    }
    if (sql.startsWith("INSERT INTO job_attempts")) return { rows: [] };
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, {
    maxAttempts: 3,
    leaseMs: 500,
    now: () => 1000,
    newAttemptId: () => `attempt-${++attempt}`,
  });

  const jobs = await store.requestJobs("worker-1", 2);

  assert.deepEqual(jobs, [
    {
      jobId: "job-1",
      idempotencyKey: "idem-1",
      payload: "sleep:1",
      attemptId: "attempt-1",
      leaseExpiresAtMs: 1500,
    },
    {
      jobId: "job-2",
      idempotencyKey: "idem-2",
      payload: "sleep:2",
      attemptId: "attempt-2",
      leaseExpiresAtMs: 1500,
    },
  ]);
  assert.equal(pool.client.calls[0]?.text, "BEGIN");
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
  assert.equal(pool.client.released, true);
});

test("PostgresJobStore reportResult rejects stale attempts without mutating rows", async () => {
  const pool = new MockPool((text) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("SELECT job_id, state, attempts")) {
      return { rows: [{ job_id: "job-1", state: "ASSIGNED", attempts: 1 }] };
    }
    if (sql.includes("AND current_attempt_id = $4")) return { rows: [] };
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const result = await store.reportResult("worker-1", "job-1", "idem-1", "old-attempt", true, "ok", 10);

  assert.deepEqual(result, { ok: false });
  assert.equal(pool.client.calls.some((call) => compact(call.text).startsWith("UPDATE jobs")), false);
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
});

test("PostgresJobStore reportResult completes the matching attempt", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("SELECT job_id, state, attempts")) {
      return { rows: [{ job_id: "job-1", state: "ASSIGNED", attempts: 1 }] };
    }
    if (sql.includes("AND current_attempt_id = $4")) return { rows: [{ job_id: "job-1" }] };
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", true, "ok", 10, false]);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs") && sql.includes("state = 'SUCCEEDED'")) return { rows: [] };
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const result = await store.reportResult("worker-1", "job-1", "idem-1", "attempt-1", true, "ok", 10);

  assert.deepEqual(result, { ok: true });
  assert.equal(pool.client.calls.some((call) => compact(call.text).includes("state = 'SUCCEEDED'")), true);
});

test("PostgresJobStore reportResult schedules failed retries after backoff", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("SELECT job_id, state, attempts")) {
      return { rows: [{ job_id: "job-1", state: "ASSIGNED", attempts: 1 }] };
    }
    if (sql.includes("AND current_attempt_id = $4")) return { rows: [{ job_id: "job-1" }] };
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", false, "bad", 10, true]);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs")) {
      assert.deepEqual(values, ["job-1", "QUEUED", new Date(1500)]);
      assert.match(sql, /available_at = \$3/);
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, {
    maxAttempts: 3,
    leaseMs: 30000,
    retryBackoffBaseMs: 500,
    retryBackoffMaxMs: 5000,
    now: () => 1000,
  });

  const result = await store.reportResult("worker-1", "job-1", "idem-1", "attempt-1", false, "bad", 10);

  assert.deepEqual(result, { ok: true });
});

test("PostgresJobStore extendLease refreshes the matching assigned attempt", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.startsWith("UPDATE jobs") && sql.includes("RETURNING lease_expires_at")) {
      assert.deepEqual(values, ["job-1", "idem-1", "worker-1", "attempt-1", new Date(1500)]);
      assert.match(sql, /AND idempotency_key = \$2/);
      assert.match(sql, /AND assigned_to = \$3/);
      assert.match(sql, /AND current_attempt_id = \$4/);
      assert.match(sql, /AND state = 'ASSIGNED'/);
      return { rows: [{ lease_expires_at: values?.[4] }] };
    }
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", new Date(1500)]);
      assert.match(sql, /finished_at IS NULL/);
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 500, now: () => 1000 });

  const result = await store.extendLease("worker-1", "job-1", "idem-1", "attempt-1");

  assert.deepEqual(result, { ok: true, leaseExpiresAtMs: 1500 });
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
});

test("PostgresJobStore extendLease rejects stale attempts without touching attempt history", async () => {
  const pool = new MockPool((text) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.startsWith("UPDATE jobs") && sql.includes("RETURNING lease_expires_at")) {
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 500, now: () => 1000 });

  const result = await store.extendLease("worker-1", "job-1", "idem-1", "old-attempt");

  assert.deepEqual(result, { ok: false, leaseExpiresAtMs: 0 });
  assert.equal(pool.client.calls.some((call) => compact(call.text).startsWith("UPDATE job_attempts")), false);
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
});

test("PostgresJobStore requeueExpiredLeases releases expired attempts transactionally", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("lease_expires_at < now()")) {
      assert.deepEqual(values, [1000]);
      return { rows: [{ job_id: "job-1", worker_id: "worker-1", attempt_id: "attempt-1", attempts: 1 }] };
    }
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", true]);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs")) {
      assert.deepEqual(values, ["job-1", "attempt-1", "QUEUED", new Date(1200)]);
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, {
    maxAttempts: 3,
    leaseMs: 30000,
    retryBackoffBaseMs: 200,
    retryBackoffMaxMs: 1000,
    now: () => 1000,
  });

  const expired = await store.requeueExpiredLeases();

  assert.deepEqual(expired, [{ jobId: "job-1", workerId: "worker-1", requeued: true }]);
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
});

test("PostgresJobStore requeueExpiredLeases fails exhausted attempts", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("lease_expires_at < now()")) {
      return { rows: [{ job_id: "job-1", worker_id: "worker-1", attempt_id: "attempt-1", attempts: 1 }] };
    }
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", false]);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs")) {
      assert.deepEqual(values, ["job-1", "attempt-1", "FAILED", null]);
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 1, leaseMs: 30000 });

  const expired = await store.requeueExpiredLeases();

  assert.deepEqual(expired, [{ jobId: "job-1", workerId: "worker-1", requeued: false }]);
});

test("PostgresJobStore requeueAssignedJobs releases worker-owned attempts", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("WHERE job_id = ANY($1::text[]) AND state = 'ASSIGNED'")) {
      assert.deepEqual(values, [["job-1", "job-2"]]);
      return {
        rows: [
          { job_id: "job-1", worker_id: "worker-1", attempt_id: "attempt-1", attempts: 1 },
          { job_id: "job-2", worker_id: "worker-1", attempt_id: "attempt-2", attempts: 1 },
        ],
      };
    }
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.match(sql, /output = 'worker dead'/);
      assert.equal(values?.[1], true);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs")) {
      assert.equal(values?.[2], "QUEUED");
      assert.deepEqual(values?.[3], new Date(1200));
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, {
    maxAttempts: 3,
    leaseMs: 30000,
    retryBackoffBaseMs: 200,
    retryBackoffMaxMs: 1000,
    now: () => 1000,
  });

  const requeued = await store.requeueAssignedJobs(["job-1", "job-1", "job-2"]);

  assert.deepEqual(requeued, { requeued: 2, failed: 0 });
  assert.equal(last(pool.client.calls)?.text, "COMMIT");
});

test("PostgresJobStore requeueAssignedJobs fails exhausted attempts", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    if (sql === "BEGIN" || sql === "COMMIT") return { rows: [] };
    if (sql.includes("WHERE job_id = ANY($1::text[]) AND state = 'ASSIGNED'")) {
      return { rows: [{ job_id: "job-1", worker_id: "worker-1", attempt_id: "attempt-1", attempts: 1 }] };
    }
    if (sql.startsWith("UPDATE job_attempts")) {
      assert.deepEqual(values, ["attempt-1", false]);
      return { rows: [] };
    }
    if (sql.startsWith("UPDATE jobs")) {
      assert.deepEqual(values, ["job-1", "attempt-1", "FAILED", null]);
      return { rows: [] };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 1, leaseMs: 30000 });

  const recovery = await store.requeueAssignedJobs(["job-1"]);

  assert.deepEqual(recovery, { requeued: 0, failed: 1 });
});

test("PostgresJobStore listFailedJobs returns dead-letter rows", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    assert.match(sql, /WHERE state = 'FAILED'/);
    assert.match(sql, /ORDER BY updated_at DESC, job_id/);
    assert.deepEqual(values, [50]);
    return {
      rows: [{
        job_id: "job-1",
        idempotency_key: "idem-1",
        payload: "sleep:1",
        attempts: "3",
        last_failure_reason: "lease expired",
      }],
    };
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const failedJobs = await store.listFailedJobs(50);

  assert.deepEqual(failedJobs, [{
    jobId: "job-1",
    idempotencyKey: "idem-1",
    payload: "sleep:1",
    attempts: 3,
    lastFailureReason: "lease expired",
  }]);
});

test("PostgresJobStore replayFailedJob moves only failed jobs back to queued", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    assert.match(sql, /WHERE job_id = \$1 AND state = 'FAILED'/);
    assert.match(sql, /attempts = CASE WHEN \$2 THEN attempts ELSE 0 END/);
    assert.deepEqual(values, ["job-1", false]);
    return { rows: [{ job_id: "job-1" }] };
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const replayed = await store.replayFailedJob("job-1");

  assert.deepEqual(replayed, { ok: true, jobId: "job-1" });
});

test("PostgresJobStore replayFailedJob can preserve attempts and reports misses", async () => {
  const pool = new MockPool((text, values) => {
    const sql = compact(text);
    assert.match(sql, /WHERE job_id = \$1 AND state = 'FAILED'/);
    assert.deepEqual(values, ["job-1", true]);
    return { rows: [] };
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const replayed = await store.replayFailedJob("job-1", true);

  assert.deepEqual(replayed, { ok: false, jobId: "" });
});

test("PostgresJobStore queueMetrics splits ready and delayed queued jobs", async () => {
  const pool = new MockPool((text) => {
    const sql = compact(text);
    assert.match(sql, /COUNT\(\*\) AS total/);
    assert.match(sql, /available_at IS NULL OR available_at <= now\(\)/);
    assert.match(sql, /available_at > now\(\)/);
    assert.match(sql, /WHERE state = 'QUEUED'/);
    return { rows: [{ total: "7", ready: "5", delayed: "2" }] };
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const queue = await store.queueMetrics();

  assert.deepEqual(queue, { total: 7, ready: 5, delayed: 2 });
});

test("PostgresJobStore metrics combines job state and attempt history", async () => {
  const pool = new MockPool((text) => {
    const sql = compact(text);
    if (sql.includes("FROM jobs") && !sql.includes("last_failure_reason")) {
      return {
        rows: [{
          total_tracked: "10",
          queued: "4",
          assigned: "2",
          succeeded: "3",
          failed: "1",
          submitted: "10",
          succeeded_count: "3",
        }],
      };
    }
    if (sql.includes("FROM job_attempts")) {
      return {
        rows: [{
          failed_count: "6",
          retried_count: "5",
          jobs_requeued_from_dead_workers: "2",
          jobs_requeued_from_expired_leases: "1",
          jobs_failed_from_dead_workers: "0",
          jobs_failed_from_expired_leases: "0",
        }],
      };
    }
    if (sql.includes("last_failure_reason")) {
      return {
        rows: [{
          failed_count: "0",
          retried_count: "0",
          jobs_requeued_from_dead_workers: "0",
          jobs_requeued_from_expired_leases: "0",
          jobs_failed_from_dead_workers: "1",
          jobs_failed_from_expired_leases: "2",
        }],
      };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const store = new PostgresJobStore(pool, { maxAttempts: 3, leaseMs: 30000 });

  const metrics = await store.metrics();
  const faults = await store.faultMetrics();

  assert.deepEqual(metrics, {
    totalTracked: 10,
    queued: 4,
    assigned: 2,
    succeeded: 3,
    failed: 1,
    submitted: 10,
    succeededCount: 3,
    failedCount: 6,
    retriedCount: 5,
  });
  assert.deepEqual(faults, {
    jobsRequeuedFromDeadWorkers: 2,
    jobsRequeuedFromExpiredLeases: 1,
    jobsFailedFromDeadWorkers: 1,
    jobsFailedFromExpiredLeases: 2,
  });
});
