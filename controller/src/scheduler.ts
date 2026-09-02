import { randomUUID } from "crypto";

export type JobState = "QUEUED" | "ASSIGNED" | "SUCCEEDED" | "FAILED";
export type JobFailureReason = "reported failure" | "worker dead" | "lease expired";

export type JobRecord = {
  jobId: string;
  idempotencyKey: string;
  payload: string;
  state: JobState;
  assignedTo?: string | undefined;
  assignedAtMs?: number | undefined;
  currentAttemptId?: string | undefined;
  leaseExpiresAtMs?: number | undefined;
  availableAtMs?: number | undefined;
  lastFailureReason?: JobFailureReason | undefined;
  attempts: number;
};

export type SchedulerOptions = {
  maxAttempts: number;
  leaseMs: number;
  retryBackoffBaseMs?: number;
  retryBackoffMaxMs?: number;
  now?: () => number;
};

export type JobCounters = {
  submitted: number;
  succeededCount: number;
  failedCount: number;
  retriedCount: number;
  jobsRequeuedFromDeadWorkers: number;
  jobsRequeuedFromExpiredLeases: number;
  jobsFailedFromDeadWorkers: number;
  jobsFailedFromExpiredLeases: number;
};

type LeaseExpiration = {
  jobId: string;
  attemptId: string;
  leaseExpiresAtMs: number;
};

type DelayedRetry = {
  jobId: string;
  availableAtMs: number;
};

export type AssignedJob = Pick<JobRecord, "jobId" | "idempotencyKey" | "payload"> & {
  attemptId: string;
  leaseExpiresAtMs: number;
};

export type JobRecoveryOutcome = {
  jobId: string;
  workerId: string;
  requeued: boolean;
};

export type JobRecoverySummary = {
  requeued: number;
  failed: number;
};

export type FailedJob = Pick<JobRecord, "jobId" | "idempotencyKey" | "payload" | "attempts" | "lastFailureReason">;

export type JobQueueMetrics = {
  total: number;
  ready: number;
  delayed: number;
};

export type SchedulerSnapshot = {
  version: 1;
  jobs: JobRecord[];
  queue: string[];
  delayedRetries?: DelayedRetry[];
  counters: JobCounters;
};

export type JobMetrics = ReturnType<JobScheduler["metrics"]>;

export class JobScheduler {
  private readonly jobs = new Map<string, JobRecord>();
  private readonly jobByIdempotency = new Map<string, string>();
  private readonly completedByIdempotency = new Set<string>();
  private readonly queue: string[] = [];
  private readonly delayedRetries: DelayedRetry[] = [];
  private readonly leaseExpirations: LeaseExpiration[] = [];
  private readonly maxAttempts: number;
  private readonly retryBackoffBaseMs: number;
  private readonly retryBackoffMaxMs: number;
  private readonly now: () => number;
  private queueHead = 0;
  private delayedRetryHead = 0;
  private leaseExpirationHead = 0;
  private counters: JobCounters = {
    submitted: 0,
    succeededCount: 0,
    failedCount: 0,
    retriedCount: 0,
    jobsRequeuedFromDeadWorkers: 0,
    jobsRequeuedFromExpiredLeases: 0,
    jobsFailedFromDeadWorkers: 0,
    jobsFailedFromExpiredLeases: 0,
  };
  private stateCounts: Record<JobState, number> = {
    QUEUED: 0,
    ASSIGNED: 0,
    SUCCEEDED: 0,
    FAILED: 0,
  };

  constructor(options: SchedulerOptions) {
    this.maxAttempts = Math.max(1, Number(options.maxAttempts) || 1);
    this.leaseMs = Math.max(1, Number(options.leaseMs) || 30000);
    this.retryBackoffBaseMs = Math.max(0, Number(options.retryBackoffBaseMs) || 0);
    this.retryBackoffMaxMs = Math.max(this.retryBackoffBaseMs, Number(options.retryBackoffMaxMs) || this.retryBackoffBaseMs);
    this.now = options.now || Date.now;
  }

  private readonly leaseMs: number;

  static fromSnapshot(options: SchedulerOptions, snapshot: SchedulerSnapshot): JobScheduler {
    const scheduler = new JobScheduler(options);
    const queuedJobIds = new Set<string>();
    const delayedJobIds = new Set<string>();

    scheduler.counters = {
      submitted: Number(snapshot.counters?.submitted) || 0,
      succeededCount: Number(snapshot.counters?.succeededCount) || 0,
      failedCount: Number(snapshot.counters?.failedCount) || 0,
      retriedCount: Number(snapshot.counters?.retriedCount) || 0,
      jobsRequeuedFromDeadWorkers: Number(snapshot.counters?.jobsRequeuedFromDeadWorkers) || 0,
      jobsRequeuedFromExpiredLeases: Number(snapshot.counters?.jobsRequeuedFromExpiredLeases) || 0,
      jobsFailedFromDeadWorkers: Number(snapshot.counters?.jobsFailedFromDeadWorkers) || 0,
      jobsFailedFromExpiredLeases: Number(snapshot.counters?.jobsFailedFromExpiredLeases) || 0,
    };

    for (const savedJob of snapshot.jobs || []) {
      if (!isJobState(savedJob.state) || !savedJob.jobId || !savedJob.idempotencyKey) {
        continue;
      }

      const job: JobRecord = {
        jobId: String(savedJob.jobId),
        idempotencyKey: String(savedJob.idempotencyKey),
        payload: String(savedJob.payload),
        state: savedJob.state,
        assignedTo: savedJob.assignedTo,
        assignedAtMs: savedJob.assignedAtMs,
        currentAttemptId: savedJob.currentAttemptId,
        leaseExpiresAtMs: savedJob.leaseExpiresAtMs,
        availableAtMs: savedJob.availableAtMs,
        lastFailureReason: isJobFailureReason(savedJob.lastFailureReason) ? savedJob.lastFailureReason : undefined,
        attempts: Math.max(0, Number(savedJob.attempts) || 0),
      };

      scheduler.jobs.set(job.jobId, job);
      scheduler.jobByIdempotency.set(job.idempotencyKey, job.jobId);
      scheduler.stateCounts[job.state] += 1;

      if (job.state === "SUCCEEDED") {
        scheduler.completedByIdempotency.add(job.idempotencyKey);
      }
    }

    for (const jobId of snapshot.queue || []) {
      const job = scheduler.jobs.get(String(jobId));
      if (!job || job.state !== "QUEUED" || queuedJobIds.has(job.jobId)) {
        continue;
      }

      if (job.availableAtMs && job.availableAtMs > scheduler.now()) {
        scheduler.insertDelayedRetry({ jobId: job.jobId, availableAtMs: job.availableAtMs });
        delayedJobIds.add(job.jobId);
        continue;
      }

      scheduler.queue.push(job.jobId);
      queuedJobIds.add(job.jobId);
    }

    for (const delayedRetry of snapshot.delayedRetries || []) {
      const job = scheduler.jobs.get(String(delayedRetry.jobId));
      const availableAtMs = Number(delayedRetry.availableAtMs) || 0;
      if (!job || job.state !== "QUEUED" || queuedJobIds.has(job.jobId) || delayedJobIds.has(job.jobId)) {
        continue;
      }

      if (availableAtMs > scheduler.now()) {
        scheduler.insertDelayedRetry({ jobId: job.jobId, availableAtMs });
        delayedJobIds.add(job.jobId);
      }
    }

    for (const job of scheduler.jobs.values()) {
      if (job.state === "QUEUED" && !queuedJobIds.has(job.jobId) && !delayedJobIds.has(job.jobId)) {
        if (job.availableAtMs && job.availableAtMs > scheduler.now()) {
          scheduler.insertDelayedRetry({ jobId: job.jobId, availableAtMs: job.availableAtMs });
          delayedJobIds.add(job.jobId);
          continue;
        }

        scheduler.queue.push(job.jobId);
      }

      if (job.state === "ASSIGNED" && job.currentAttemptId && job.leaseExpiresAtMs) {
        scheduler.leaseExpirations.push({
          jobId: job.jobId,
          attemptId: job.currentAttemptId,
          leaseExpiresAtMs: job.leaseExpiresAtMs,
        });
      }
    }

    scheduler.leaseExpirations.sort((left, right) => left.leaseExpiresAtMs - right.leaseExpiresAtMs);
    return scheduler;
  }

  submitJob(idempotencyKey: string | undefined, payload: string): { ok: boolean; jobId: string } {
    if (!payload || typeof payload !== "string") {
      return { ok: false, jobId: "" };
    }

    const idem = idempotencyKey ? String(idempotencyKey) : `idem-${randomUUID()}`;
    const existingJobId = this.jobByIdempotency.get(idem);
    if (existingJobId) {
      return { ok: true, jobId: existingJobId };
    }

    const jobId = `job-${randomUUID().slice(0, 8)}`;
    const job: JobRecord = {
      jobId,
      idempotencyKey: idem,
      payload: String(payload),
      state: "QUEUED",
      attempts: 0,
    };

    this.jobs.set(jobId, job);
    this.jobByIdempotency.set(idem, jobId);
    this.enqueue(jobId);
    this.counters.submitted += 1;
    this.stateCounts.QUEUED += 1;

    return { ok: true, jobId };
  }

  requestJob(workerId: string): { hasJob: false } | { hasJob: true; job: AssignedJob } {
    const jobs = this.requestJobs(workerId, 1);
    if (jobs.length === 0) return { hasJob: false };

    return { hasJob: true, job: jobs[0]! };
  }

  requestJobs(workerId: string, maxJobs: number): AssignedJob[] {
    const requested = Math.max(0, Math.floor(Number(maxJobs) || 0));
    const assignedJobs: AssignedJob[] = [];
    this.promoteReadyRetries();

    while (this.queueHead < this.queue.length) {
      if (assignedJobs.length >= requested) break;

      const jobId = this.queue[this.queueHead];
      this.queueHead += 1;
      if (!jobId) continue;

      const job = this.jobs.get(jobId);
      if (!job || job.state !== "QUEUED") continue;
      if (job.availableAtMs && job.availableAtMs > this.now()) {
        this.insertDelayedRetry({ jobId: job.jobId, availableAtMs: job.availableAtMs });
        continue;
      }

      if (this.completedByIdempotency.has(job.idempotencyKey)) {
        this.transition(job, "SUCCEEDED");
        continue;
      }

      this.transition(job, "ASSIGNED");
      job.assignedTo = workerId;
      job.assignedAtMs = this.now();
      job.currentAttemptId = `attempt-${randomUUID().slice(0, 12)}`;
      job.leaseExpiresAtMs = job.assignedAtMs + this.leaseMs;
      job.availableAtMs = undefined;
      job.attempts += 1;
      this.leaseExpirations.push({
        jobId: job.jobId,
        attemptId: job.currentAttemptId,
        leaseExpiresAtMs: job.leaseExpiresAtMs,
      });

      assignedJobs.push({
        jobId: job.jobId,
        idempotencyKey: job.idempotencyKey,
        payload: job.payload,
        attemptId: job.currentAttemptId,
        leaseExpiresAtMs: job.leaseExpiresAtMs,
      });
    }

    this.compactQueueIfNeeded();
    return assignedJobs;
  }

  reportResult(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string,
    success: boolean
  ): { ok: boolean } {
    const job = this.jobs.get(jobId);
    if (!job || job.idempotencyKey !== idempotencyKey) {
      return { ok: false };
    }

    if (this.completedByIdempotency.has(idempotencyKey)) {
      return { ok: true };
    }

    if (job.state !== "ASSIGNED" || job.assignedTo !== workerId || job.currentAttemptId !== attemptId) {
      return { ok: false };
    }

    job.assignedTo = undefined;
    job.assignedAtMs = undefined;
    job.currentAttemptId = undefined;
    job.leaseExpiresAtMs = undefined;

    if (success) {
      this.transition(job, "SUCCEEDED");
      this.completedByIdempotency.add(idempotencyKey);
      job.lastFailureReason = undefined;
      this.counters.succeededCount += 1;
      return { ok: true };
    }

    this.finishFailedAttempt(job, "reported failure");
    return { ok: true };
  }

  extendLease(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string
  ): { ok: boolean; leaseExpiresAtMs: number } {
    const job = this.jobs.get(jobId);
    if (
      !job ||
      job.idempotencyKey !== idempotencyKey ||
      job.state !== "ASSIGNED" ||
      job.assignedTo !== workerId ||
      job.currentAttemptId !== attemptId
    ) {
      return { ok: false, leaseExpiresAtMs: 0 };
    }

    job.leaseExpiresAtMs = this.now() + this.leaseMs;
    this.leaseExpirations.push({
      jobId: job.jobId,
      attemptId,
      leaseExpiresAtMs: job.leaseExpiresAtMs,
    });

    return { ok: true, leaseExpiresAtMs: job.leaseExpiresAtMs };
  }

  requeueAssignedJobs(jobIds: string[]): JobRecoverySummary {
    const summary: JobRecoverySummary = { requeued: 0, failed: 0 };

    for (const jobId of jobIds) {
      const job = this.jobs.get(jobId);
      if (!job || job.state !== "ASSIGNED") continue;

      const outcome = this.finishFailedAttempt(job, "worker dead");
      if (outcome.requeued) {
        summary.requeued += 1;
      } else {
        summary.failed += 1;
      }
    }

    this.counters.jobsRequeuedFromDeadWorkers += summary.requeued;
    this.counters.jobsFailedFromDeadWorkers += summary.failed;
    return summary;
  }

  requeueExpiredLeases(): JobRecoveryOutcome[] {
    const expired: JobRecoveryOutcome[] = [];
    const timestamp = this.now();

    while (this.leaseExpirationHead < this.leaseExpirations.length) {
      const leaseExpiration = this.leaseExpirations[this.leaseExpirationHead];
      if (!leaseExpiration) {
        this.leaseExpirationHead += 1;
        continue;
      }

      if (timestamp <= leaseExpiration.leaseExpiresAtMs) {
        break;
      }

      this.leaseExpirationHead += 1;
      const job = this.jobs.get(leaseExpiration.jobId);
      if (
        !job ||
        job.state !== "ASSIGNED" ||
        job.currentAttemptId !== leaseExpiration.attemptId ||
        job.leaseExpiresAtMs !== leaseExpiration.leaseExpiresAtMs ||
        !job.assignedTo
      ) {
        continue;
      }

      const workerId = job.assignedTo;
      const outcome = this.finishFailedAttempt(job, "lease expired");
      expired.push({ jobId: job.jobId, workerId, requeued: outcome.requeued });
    }

    this.counters.jobsRequeuedFromExpiredLeases += expired.filter((job) => job.requeued).length;
    this.counters.jobsFailedFromExpiredLeases += expired.filter((job) => !job.requeued).length;
    this.compactLeaseExpirationsIfNeeded();
    return expired;
  }

  listFailedJobs(limit = 100): FailedJob[] {
    const requested = Math.max(0, Math.floor(Number(limit) || 100));
    if (requested === 0) return [];

    const failedJobs: FailedJob[] = [];
    for (const job of this.jobs.values()) {
      if (job.state !== "FAILED") continue;
      failedJobs.push({
        jobId: job.jobId,
        idempotencyKey: job.idempotencyKey,
        payload: job.payload,
        attempts: job.attempts,
        lastFailureReason: job.lastFailureReason,
      });
      if (failedJobs.length >= requested) break;
    }

    return failedJobs;
  }

  replayFailedJob(jobId: string, preserveAttempts = false): { ok: boolean; jobId: string } {
    const job = this.jobs.get(jobId);
    if (!job || job.state !== "FAILED") {
      return { ok: false, jobId: "" };
    }

    job.assignedTo = undefined;
    job.assignedAtMs = undefined;
    job.currentAttemptId = undefined;
    job.leaseExpiresAtMs = undefined;
    job.availableAtMs = undefined;
    job.lastFailureReason = undefined;
    if (!preserveAttempts) {
      job.attempts = 0;
    }
    this.transition(job, "QUEUED");
    this.enqueue(job.jobId);

    return { ok: true, jobId: job.jobId };
  }

  metrics() {
    return {
      totalTracked: this.jobs.size,
      queued: this.stateCounts.QUEUED,
      assigned: this.stateCounts.ASSIGNED,
      succeeded: this.stateCounts.SUCCEEDED,
      failed: this.stateCounts.FAILED,
      submitted: this.counters.submitted,
      succeededCount: this.counters.succeededCount,
      failedCount: this.counters.failedCount,
      retriedCount: this.counters.retriedCount,
    };
  }

  faultMetrics() {
    return {
      jobsRequeuedFromDeadWorkers: this.counters.jobsRequeuedFromDeadWorkers,
      jobsRequeuedFromExpiredLeases: this.counters.jobsRequeuedFromExpiredLeases,
      jobsFailedFromDeadWorkers: this.counters.jobsFailedFromDeadWorkers,
      jobsFailedFromExpiredLeases: this.counters.jobsFailedFromExpiredLeases,
    };
  }

  queueDepth(): number {
    return this.stateCounts.QUEUED;
  }

  queueMetrics(): JobQueueMetrics {
    let ready = 0;
    let delayed = 0;
    const timestamp = this.now();

    for (const job of this.jobs.values()) {
      if (job.state !== "QUEUED") continue;
      if (job.availableAtMs && job.availableAtMs > timestamp) {
        delayed += 1;
      } else {
        ready += 1;
      }
    }

    return {
      total: ready + delayed,
      ready,
      delayed,
    };
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.jobs.get(jobId);
  }

  toSnapshot(): SchedulerSnapshot {
    return {
      version: 1,
      jobs: Array.from(this.jobs.values()).map((job) => ({ ...job })),
      queue: this.queue.slice(this.queueHead),
      delayedRetries: this.delayedRetries.slice(this.delayedRetryHead),
      counters: { ...this.counters },
    };
  }

  private enqueue(jobId: string): void {
    this.queue.push(jobId);
  }

  private transition(job: JobRecord, nextState: JobState): void {
    if (job.state === nextState) return;

    this.stateCounts[job.state] -= 1;
    job.state = nextState;
    this.stateCounts[nextState] += 1;
  }

  private finishFailedAttempt(job: JobRecord, reason: JobFailureReason): { requeued: boolean } {
    job.assignedTo = undefined;
    job.assignedAtMs = undefined;
    job.currentAttemptId = undefined;
    job.leaseExpiresAtMs = undefined;
    job.lastFailureReason = reason;
    this.counters.failedCount += 1;

    if (job.attempts < this.maxAttempts) {
      this.transition(job, "QUEUED");
      const availableAtMs = this.nextRetryAvailableAtMs(job.attempts);
      if (availableAtMs > this.now()) {
        job.availableAtMs = availableAtMs;
        this.insertDelayedRetry({ jobId: job.jobId, availableAtMs });
      } else {
        job.availableAtMs = undefined;
        this.enqueue(job.jobId);
      }
      this.counters.retriedCount += 1;
      return { requeued: true };
    }

    job.availableAtMs = undefined;
    this.transition(job, "FAILED");
    return { requeued: false };
  }

  private nextRetryAvailableAtMs(attempts: number): number {
    if (this.retryBackoffBaseMs <= 0) return this.now();

    const exponent = Math.max(0, attempts - 1);
    const delay = Math.min(this.retryBackoffMaxMs, this.retryBackoffBaseMs * 2 ** exponent);
    return this.now() + delay;
  }

  private insertDelayedRetry(delayedRetry: DelayedRetry): void {
    let insertAt = this.delayedRetries.length;
    for (let index = this.delayedRetries.length - 1; index >= this.delayedRetryHead; index -= 1) {
      const existing = this.delayedRetries[index];
      if (!existing || existing.availableAtMs <= delayedRetry.availableAtMs) break;
      insertAt = index;
    }

    this.delayedRetries.splice(insertAt, 0, delayedRetry);
  }

  private promoteReadyRetries(): void {
    const timestamp = this.now();

    while (this.delayedRetryHead < this.delayedRetries.length) {
      const delayedRetry = this.delayedRetries[this.delayedRetryHead];
      if (!delayedRetry) {
        this.delayedRetryHead += 1;
        continue;
      }

      if (delayedRetry.availableAtMs > timestamp) break;

      this.delayedRetryHead += 1;
      const job = this.jobs.get(delayedRetry.jobId);
      if (!job || job.state !== "QUEUED" || job.availableAtMs !== delayedRetry.availableAtMs) {
        continue;
      }

      job.availableAtMs = undefined;
      this.enqueue(job.jobId);
    }

    this.compactDelayedRetriesIfNeeded();
  }

  private compactQueueIfNeeded(): void {
    if (this.queueHead < 1024 || this.queueHead * 2 < this.queue.length) return;

    this.queue.splice(0, this.queueHead);
    this.queueHead = 0;
  }

  private compactLeaseExpirationsIfNeeded(): void {
    if (this.leaseExpirationHead < 1024 || this.leaseExpirationHead * 2 < this.leaseExpirations.length) return;

    this.leaseExpirations.splice(0, this.leaseExpirationHead);
    this.leaseExpirationHead = 0;
  }

  private compactDelayedRetriesIfNeeded(): void {
    if (this.delayedRetryHead < 1024 || this.delayedRetryHead * 2 < this.delayedRetries.length) return;

    this.delayedRetries.splice(0, this.delayedRetryHead);
    this.delayedRetryHead = 0;
  }
}

function isJobState(value: unknown): value is JobState {
  return value === "QUEUED" || value === "ASSIGNED" || value === "SUCCEEDED" || value === "FAILED";
}

function isJobFailureReason(value: unknown): value is JobFailureReason {
  return value === "reported failure" || value === "worker dead" || value === "lease expired";
}
