import { randomUUID } from "crypto";
import { Pool } from "pg";
import type {
  AssignedJob,
  FailedJob,
  JobMetrics,
  JobQueueMetrics,
  JobRecoveryOutcome,
  JobRecoverySummary,
} from "./scheduler";

export type QueryResultLike<T> = {
  rows: T[];
};

export type Queryable = {
  query<T = any>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type TransactionClient = Queryable & {
  release(): void;
};

export type PoolLike = Queryable & {
  connect(): Promise<TransactionClient>;
  end?(): Promise<void>;
};

export type PostgresJobStoreOptions = {
  maxAttempts: number;
  leaseMs: number;
  retryBackoffBaseMs?: number;
  retryBackoffMaxMs?: number;
  now?: () => number;
  newAttemptId?: () => string;
  newJobId?: () => string;
};

type ReadyJobRow = {
  job_id: string;
  idempotency_key: string;
  payload: string;
};

type ClaimedJobRow = ReadyJobRow & {
  current_attempt_id: string;
  lease_expires_at: Date | string | number;
};

type ReportableJobRow = {
  job_id: string;
  state: string;
  attempts: number;
};

type ExpiredLeaseRow = {
  job_id: string;
  worker_id: string;
  attempt_id: string;
  attempts: string | number;
};

type ExtendedLeaseRow = {
  lease_expires_at: Date | string | number;
};

type FailedJobRow = {
  job_id: string;
  idempotency_key: string;
  payload: string;
  attempts: string | number;
  last_failure_reason?: string | null;
};

type MetricsRow = {
  total_tracked: string | number;
  queued: string | number;
  assigned: string | number;
  succeeded: string | number;
  failed: string | number;
  submitted: string | number;
  succeeded_count: string | number;
  retried_count: string | number;
};

type FailedAttemptMetricsRow = {
  failed_count: string | number;
  retried_count: string | number;
  jobs_requeued_from_dead_workers: string | number;
  jobs_requeued_from_expired_leases: string | number;
  jobs_failed_from_dead_workers: string | number;
  jobs_failed_from_expired_leases: string | number;
};

type QueueMetricsRow = {
  total: string | number;
  ready: string | number;
  delayed: string | number;
};

export class PostgresJobStore {
  private readonly maxAttempts: number;
  private readonly leaseMs: number;
  private readonly retryBackoffBaseMs: number;
  private readonly retryBackoffMaxMs: number;
  private readonly now: () => number;
  private readonly newAttemptId: () => string;
  private readonly newJobId: () => string;

  constructor(private readonly pool: PoolLike, options: PostgresJobStoreOptions) {
    this.maxAttempts = Math.max(1, Number(options.maxAttempts) || 1);
    this.leaseMs = Math.max(1, Number(options.leaseMs) || 30000);
    this.retryBackoffBaseMs = Math.max(0, Number(options.retryBackoffBaseMs) || 0);
    this.retryBackoffMaxMs = Math.max(this.retryBackoffBaseMs, Number(options.retryBackoffMaxMs) || this.retryBackoffBaseMs);
    this.now = options.now || Date.now;
    this.newAttemptId = options.newAttemptId || (() => `attempt-${randomUUID().slice(0, 12)}`);
    this.newJobId = options.newJobId || (() => `job-${randomUUID().slice(0, 8)}`);
  }

  static fromConnectionString(connectionString: string, options: PostgresJobStoreOptions): PostgresJobStore {
    return new PostgresJobStore(new Pool({ connectionString }), options);
  }

  async initialize(): Promise<void> {
    await this.pool.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_state') THEN
          CREATE TYPE job_state AS ENUM ('QUEUED', 'ASSIGNED', 'SUCCEEDED', 'FAILED');
        END IF;
      END
      $$;

      CREATE TABLE IF NOT EXISTS jobs (
        job_id text PRIMARY KEY,
        idempotency_key text NOT NULL UNIQUE,
        payload text NOT NULL,
        state job_state NOT NULL,
        assigned_to text,
        assigned_at timestamptz,
        current_attempt_id text,
        lease_expires_at timestamptz,
        available_at timestamptz DEFAULT now(),
        last_failure_reason text,
        attempts integer NOT NULL DEFAULT 0,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
      );

      CREATE INDEX IF NOT EXISTS jobs_ready_available_idx
        ON jobs (available_at, created_at, job_id)
        WHERE state = 'QUEUED';

      CREATE INDEX IF NOT EXISTS jobs_lease_expiry_idx
        ON jobs (lease_expires_at)
        WHERE state = 'ASSIGNED' AND lease_expires_at IS NOT NULL;

      CREATE TABLE IF NOT EXISTS job_attempts (
        attempt_id text PRIMARY KEY,
        job_id text NOT NULL REFERENCES jobs (job_id),
        worker_id text NOT NULL,
        started_at timestamptz NOT NULL DEFAULT now(),
        lease_expires_at timestamptz NOT NULL,
        finished_at timestamptz,
        success boolean,
        output text,
        duration_ms bigint,
        retry_scheduled boolean NOT NULL DEFAULT false
      );

      CREATE INDEX IF NOT EXISTS job_attempts_job_idx ON job_attempts (job_id, started_at DESC);

      ALTER TABLE jobs ADD COLUMN IF NOT EXISTS available_at timestamptz DEFAULT now();
      ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_failure_reason text;
      ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS retry_scheduled boolean NOT NULL DEFAULT false;
    `);
  }

  async close(): Promise<void> {
    await this.pool.end?.();
  }

  async submitJob(idempotencyKey: string | undefined, payload: string): Promise<{ ok: boolean; jobId: string }> {
    if (!payload || typeof payload !== "string") {
      return { ok: false, jobId: "" };
    }

    const idem = idempotencyKey ? String(idempotencyKey) : `idem-${randomUUID()}`;
    const result = await this.pool.query<{ job_id: string }>(
      `
        WITH inserted AS (
          INSERT INTO jobs (job_id, idempotency_key, payload, state, attempts)
          VALUES ($1, $2, $3, 'QUEUED', 0)
          ON CONFLICT (idempotency_key) DO NOTHING
          RETURNING job_id
        )
        SELECT job_id FROM inserted
        UNION ALL
        SELECT job_id FROM jobs WHERE idempotency_key = $2
        LIMIT 1
      `,
      [this.newJobId(), idem, String(payload)]
    );

    return { ok: Boolean(result.rows[0]?.job_id), jobId: result.rows[0]?.job_id || "" };
  }

  async requestJobs(workerId: string, maxJobs: number): Promise<AssignedJob[]> {
    const requested = Math.max(0, Math.floor(Number(maxJobs) || 0));
    if (!workerId || requested <= 0) return [];

    return this.withTransaction(async (client) => {
      const ready = await client.query<ReadyJobRow>(
        `
          SELECT job_id, idempotency_key, payload
          FROM jobs
          WHERE state = 'QUEUED' AND (available_at IS NULL OR available_at <= now())
          ORDER BY available_at, created_at, job_id
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        `,
        [requested]
      );

      const assigned: AssignedJob[] = [];
      for (const row of ready.rows) {
        const attemptId = this.newAttemptId();
        const leaseExpiresAt = new Date(this.now() + this.leaseMs);
        const claimed = await client.query<ClaimedJobRow>(
          `
            UPDATE jobs
            SET
              state = 'ASSIGNED',
              assigned_to = $2,
              assigned_at = now(),
              current_attempt_id = $3,
              lease_expires_at = $4,
              available_at = NULL,
              attempts = attempts + 1,
              updated_at = now()
            WHERE job_id = $1 AND state = 'QUEUED'
            RETURNING job_id, idempotency_key, payload, current_attempt_id, lease_expires_at
          `,
          [row.job_id, workerId, attemptId, leaseExpiresAt]
        );

        const job = claimed.rows[0];
        if (!job) continue;

        await client.query(
          `
            INSERT INTO job_attempts (attempt_id, job_id, worker_id, lease_expires_at)
            VALUES ($1, $2, $3, $4)
          `,
          [job.current_attempt_id, job.job_id, workerId, leaseExpiresAt]
        );

        assigned.push({
          jobId: job.job_id,
          idempotencyKey: job.idempotency_key,
          payload: job.payload,
          attemptId: job.current_attempt_id,
          leaseExpiresAtMs: toMillis(job.lease_expires_at),
        });
      }

      return assigned;
    });
  }

  async requestJob(workerId: string): Promise<{ hasJob: false } | { hasJob: true; job: AssignedJob }> {
    const jobs = await this.requestJobs(workerId, 1);
    if (jobs.length === 0) return { hasJob: false };

    return { hasJob: true, job: jobs[0]! };
  }

  async reportResult(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string,
    success: boolean,
    output: string,
    durationMs: number
  ): Promise<{ ok: boolean }> {
    return this.withTransaction(async (client) => {
      const existing = await client.query<ReportableJobRow>(
        `
          SELECT job_id, state, attempts
          FROM jobs
          WHERE job_id = $1 AND idempotency_key = $2
          FOR UPDATE
        `,
        [jobId, idempotencyKey]
      );

      const job = existing.rows[0];
      if (!job) return { ok: false };
      if (job.state === "SUCCEEDED") return { ok: true };

      const currentAttempt = await client.query<{ job_id: string }>(
        `
          SELECT job_id
          FROM jobs
          WHERE
            job_id = $1
            AND idempotency_key = $2
            AND assigned_to = $3
            AND current_attempt_id = $4
            AND state = 'ASSIGNED'
        `,
        [jobId, idempotencyKey, workerId, attemptId]
      );
      if (!currentAttempt.rows[0]) return { ok: false };

      const shouldRetry = !success && job.attempts < this.maxAttempts;
      await client.query(
        `
          UPDATE job_attempts
          SET finished_at = now(), success = $2, output = $3, duration_ms = $4, retry_scheduled = $5
          WHERE attempt_id = $1
        `,
        [attemptId, Boolean(success), output || "", Math.max(0, Number(durationMs) || 0), shouldRetry]
      );

      if (success) {
        await client.query(
          `
            UPDATE jobs
            SET
              state = 'SUCCEEDED',
              assigned_to = NULL,
              assigned_at = NULL,
              current_attempt_id = NULL,
              lease_expires_at = NULL,
              last_failure_reason = NULL,
              updated_at = now()
            WHERE job_id = $1
          `,
          [jobId]
        );
        return { ok: true };
      }

      const nextState = shouldRetry ? "QUEUED" : "FAILED";
      const availableAt = shouldRetry ? this.nextRetryAvailableAt(job.attempts) : null;
      await client.query(
        `
          UPDATE jobs
          SET
            state = $2::job_state,
            assigned_to = NULL,
            assigned_at = NULL,
            current_attempt_id = NULL,
            lease_expires_at = NULL,
            available_at = $3,
            last_failure_reason = 'reported failure',
            updated_at = now()
          WHERE job_id = $1
        `,
        [jobId, nextState, availableAt]
      );

      return { ok: true };
    });
  }

  async extendLease(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string
  ): Promise<{ ok: boolean; leaseExpiresAtMs: number }> {
    const leaseExpiresAt = new Date(this.now() + this.leaseMs);

    return this.withTransaction(async (client) => {
      const updated = await client.query<ExtendedLeaseRow>(
        `
          UPDATE jobs
          SET lease_expires_at = $5, updated_at = now()
          WHERE
            job_id = $1
            AND idempotency_key = $2
            AND assigned_to = $3
            AND current_attempt_id = $4
            AND state = 'ASSIGNED'
          RETURNING lease_expires_at
        `,
        [jobId, idempotencyKey, workerId, attemptId, leaseExpiresAt]
      );

      const row = updated.rows[0];
      if (!row) return { ok: false, leaseExpiresAtMs: 0 };

      await client.query(
        `
          UPDATE job_attempts
          SET lease_expires_at = $2
          WHERE attempt_id = $1 AND finished_at IS NULL
        `,
        [attemptId, leaseExpiresAt]
      );

      return { ok: true, leaseExpiresAtMs: toMillis(row.lease_expires_at) };
    });
  }

  async requeueExpiredLeases(limit = 1000): Promise<JobRecoveryOutcome[]> {
    return this.withTransaction(async (client) => {
      const expired = await client.query<ExpiredLeaseRow>(
        `
          SELECT
            job_id,
            assigned_to AS worker_id,
            current_attempt_id AS attempt_id,
            attempts
          FROM jobs
          WHERE state = 'ASSIGNED' AND lease_expires_at < now()
          ORDER BY lease_expires_at, job_id
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        `,
        [Math.max(1, Math.floor(Number(limit) || 1000))]
      );

      for (const row of expired.rows) {
        const shouldRetry = toNumber(row.attempts) < this.maxAttempts;
        const availableAt = shouldRetry ? this.nextRetryAvailableAt(row.attempts) : null;
        await client.query(
          `
            UPDATE job_attempts
            SET finished_at = now(), success = false, output = 'lease expired', retry_scheduled = $2
            WHERE attempt_id = $1 AND finished_at IS NULL
          `,
          [row.attempt_id, shouldRetry]
        );

        await client.query(
          `
          UPDATE jobs
          SET
              state = $3::job_state,
              assigned_to = NULL,
              assigned_at = NULL,
              current_attempt_id = NULL,
              lease_expires_at = NULL,
              available_at = $4,
              last_failure_reason = 'lease expired',
              updated_at = now()
            WHERE job_id = $1 AND current_attempt_id = $2
          `,
          [row.job_id, row.attempt_id, shouldRetry ? "QUEUED" : "FAILED", availableAt]
        );
      }

      return expired.rows.map((row) => ({
        jobId: row.job_id,
        workerId: row.worker_id,
        requeued: toNumber(row.attempts) < this.maxAttempts,
      }));
    });
  }

  async requeueAssignedJobs(jobIds: string[]): Promise<JobRecoverySummary> {
    const uniqueJobIds = Array.from(new Set(jobIds.filter(Boolean)));
    if (uniqueJobIds.length === 0) return { requeued: 0, failed: 0 };

    return this.withTransaction(async (client) => {
      const assigned = await client.query<ExpiredLeaseRow>(
        `
          SELECT
            job_id,
            assigned_to AS worker_id,
            current_attempt_id AS attempt_id,
            attempts
          FROM jobs
          WHERE job_id = ANY($1::text[]) AND state = 'ASSIGNED'
          FOR UPDATE
        `,
        [uniqueJobIds]
      );

      const summary: JobRecoverySummary = { requeued: 0, failed: 0 };
      for (const row of assigned.rows) {
        const shouldRetry = toNumber(row.attempts) < this.maxAttempts;
        const availableAt = shouldRetry ? this.nextRetryAvailableAt(row.attempts) : null;
        await client.query(
          `
            UPDATE job_attempts
            SET finished_at = now(), success = false, output = 'worker dead', retry_scheduled = $2
            WHERE attempt_id = $1 AND finished_at IS NULL
          `,
          [row.attempt_id, shouldRetry]
        );

        await client.query(
          `
          UPDATE jobs
          SET
              state = $3::job_state,
              assigned_to = NULL,
              assigned_at = NULL,
              current_attempt_id = NULL,
              lease_expires_at = NULL,
              available_at = $4,
              last_failure_reason = 'worker dead',
              updated_at = now()
            WHERE job_id = $1 AND current_attempt_id = $2
          `,
          [row.job_id, row.attempt_id, shouldRetry ? "QUEUED" : "FAILED", availableAt]
        );

        if (shouldRetry) {
          summary.requeued += 1;
        } else {
          summary.failed += 1;
        }
      }

      return summary;
    });
  }

  async listFailedJobs(limit = 100): Promise<FailedJob[]> {
    const requested = Math.max(0, Math.floor(Number(limit) || 100));
    if (requested === 0) return [];

    const result = await this.pool.query<FailedJobRow>(
      `
        SELECT job_id, idempotency_key, payload, attempts, last_failure_reason
        FROM jobs
        WHERE state = 'FAILED'
        ORDER BY updated_at DESC, job_id
        LIMIT $1
      `,
      [requested]
    );

    return result.rows.map((row) => ({
      jobId: row.job_id,
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      attempts: toNumber(row.attempts),
      lastFailureReason: parseFailureReason(row.last_failure_reason),
    }));
  }

  async replayFailedJob(jobId: string, preserveAttempts = false): Promise<{ ok: boolean; jobId: string }> {
    const result = await this.pool.query<{ job_id: string }>(
      `
        UPDATE jobs
        SET
          state = 'QUEUED',
          assigned_to = NULL,
          assigned_at = NULL,
          current_attempt_id = NULL,
          lease_expires_at = NULL,
          available_at = now(),
          last_failure_reason = NULL,
          attempts = CASE WHEN $2 THEN attempts ELSE 0 END,
          updated_at = now()
        WHERE job_id = $1 AND state = 'FAILED'
        RETURNING job_id
      `,
      [jobId, Boolean(preserveAttempts)]
    );

    return { ok: Boolean(result.rows[0]?.job_id), jobId: result.rows[0]?.job_id || "" };
  }

  async metrics(): Promise<JobMetrics> {
    const jobs = await this.pool.query<MetricsRow>(`
      SELECT
        COUNT(*) AS total_tracked,
        COUNT(*) FILTER (WHERE state = 'QUEUED') AS queued,
        COUNT(*) FILTER (WHERE state = 'ASSIGNED') AS assigned,
        COUNT(*) FILTER (WHERE state = 'SUCCEEDED') AS succeeded,
        COUNT(*) FILTER (WHERE state = 'FAILED') AS failed,
        COUNT(*) AS submitted,
        COUNT(*) FILTER (WHERE state = 'SUCCEEDED') AS succeeded_count,
        0 AS retried_count
      FROM jobs
    `);

    const attempts = await this.pool.query<FailedAttemptMetricsRow>(`
      SELECT
        COUNT(*) FILTER (WHERE success = false) AS failed_count,
        COUNT(*) FILTER (WHERE retry_scheduled) AS retried_count,
        COUNT(*) FILTER (WHERE output = 'worker dead' AND retry_scheduled) AS jobs_requeued_from_dead_workers,
        COUNT(*) FILTER (WHERE output = 'lease expired' AND retry_scheduled) AS jobs_requeued_from_expired_leases,
        0 AS jobs_failed_from_dead_workers,
        0 AS jobs_failed_from_expired_leases
      FROM job_attempts
    `);

    const jobRow = jobs.rows[0];
    const attemptRow = attempts.rows[0];

    return {
      totalTracked: toNumber(jobRow?.total_tracked),
      queued: toNumber(jobRow?.queued),
      assigned: toNumber(jobRow?.assigned),
      succeeded: toNumber(jobRow?.succeeded),
      failed: toNumber(jobRow?.failed),
      submitted: toNumber(jobRow?.submitted),
      succeededCount: toNumber(jobRow?.succeeded_count),
      failedCount: toNumber(attemptRow?.failed_count),
      retriedCount: toNumber(attemptRow?.retried_count),
    };
  }

  async queueDepth(): Promise<number> {
    const result = await this.pool.query<{ queue_depth: string | number }>(
      "SELECT COUNT(*) AS queue_depth FROM jobs WHERE state = 'QUEUED'"
    );

    return toNumber(result.rows[0]?.queue_depth);
  }

  async queueMetrics(): Promise<JobQueueMetrics> {
    const result = await this.pool.query<QueueMetricsRow>(`
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE available_at IS NULL OR available_at <= now()) AS ready,
        COUNT(*) FILTER (WHERE available_at > now()) AS delayed
      FROM jobs
      WHERE state = 'QUEUED'
    `);
    const row = result.rows[0];

    return {
      total: toNumber(row?.total),
      ready: toNumber(row?.ready),
      delayed: toNumber(row?.delayed),
    };
  }

  async faultMetrics(): Promise<{
    jobsRequeuedFromDeadWorkers: number;
    jobsRequeuedFromExpiredLeases: number;
    jobsFailedFromDeadWorkers: number;
    jobsFailedFromExpiredLeases: number;
  }> {
    const result = await this.pool.query<FailedAttemptMetricsRow>(`
      SELECT
        COUNT(*) FILTER (WHERE output = 'worker dead' AND retry_scheduled) AS jobs_requeued_from_dead_workers,
        COUNT(*) FILTER (WHERE output = 'lease expired' AND retry_scheduled) AS jobs_requeued_from_expired_leases,
        COUNT(*) FILTER (WHERE success = false) AS failed_count,
        0 AS retried_count,
        0 AS jobs_failed_from_dead_workers,
        0 AS jobs_failed_from_expired_leases
      FROM job_attempts
    `);
    const failedByReason = await this.pool.query<FailedAttemptMetricsRow>(`
      SELECT
        0 AS failed_count,
        0 AS retried_count,
        0 AS jobs_requeued_from_dead_workers,
        0 AS jobs_requeued_from_expired_leases,
        COUNT(*) FILTER (WHERE last_failure_reason = 'worker dead') AS jobs_failed_from_dead_workers,
        COUNT(*) FILTER (WHERE last_failure_reason = 'lease expired') AS jobs_failed_from_expired_leases
      FROM jobs
      WHERE state = 'FAILED'
    `);
    const row = result.rows[0];
    const failedReasonRow = failedByReason.rows[0];

    return {
      jobsRequeuedFromDeadWorkers: toNumber(row?.jobs_requeued_from_dead_workers),
      jobsRequeuedFromExpiredLeases: toNumber(row?.jobs_requeued_from_expired_leases),
      jobsFailedFromDeadWorkers: toNumber(failedReasonRow?.jobs_failed_from_dead_workers),
      jobsFailedFromExpiredLeases: toNumber(failedReasonRow?.jobs_failed_from_expired_leases),
    };
  }

  private async withTransaction<T>(fn: (client: TransactionClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  private nextRetryAvailableAt(attempts: string | number): Date {
    if (this.retryBackoffBaseMs <= 0) return new Date(this.now());

    const exponent = Math.max(0, toNumber(attempts) - 1);
    const delay = Math.min(this.retryBackoffMaxMs, this.retryBackoffBaseMs * 2 ** exponent);
    return new Date(this.now() + delay);
  }
}

function toMillis(value: Date | string | number): number {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  return new Date(value).getTime();
}

function toNumber(value: string | number | undefined): number {
  if (typeof value === "number") return value;
  if (!value) return 0;
  return Number(value) || 0;
}

function parseFailureReason(value: string | null | undefined): FailedJob["lastFailureReason"] {
  if (value === "reported failure" || value === "worker dead" || value === "lease expired") {
    return value;
  }

  return undefined;
}
