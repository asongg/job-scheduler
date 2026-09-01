import { randomUUID } from "crypto";

export type JobState = "QUEUED" | "ASSIGNED" | "SUCCEEDED" | "FAILED";

export type JobRecord = {
  jobId: string;
  idempotencyKey: string;
  payload: string;
  state: JobState;
  assignedTo?: string | undefined;
  assignedAtMs?: number | undefined;
  attempts: number;
};

export type SchedulerOptions = {
  maxAttempts: number;
  now?: () => number;
};

type JobCounters = {
  submitted: number;
  succeededCount: number;
  failedCount: number;
  retriedCount: number;
  jobsRequeuedFromDeadWorkers: number;
};

export class JobScheduler {
  private readonly jobs = new Map<string, JobRecord>();
  private readonly jobByIdempotency = new Map<string, string>();
  private readonly completedByIdempotency = new Set<string>();
  private readonly queue: string[] = [];
  private readonly maxAttempts: number;
  private readonly now: () => number;
  private queueHead = 0;
  private counters: JobCounters = {
    submitted: 0,
    succeededCount: 0,
    failedCount: 0,
    retriedCount: 0,
    jobsRequeuedFromDeadWorkers: 0,
  };
  private stateCounts: Record<JobState, number> = {
    QUEUED: 0,
    ASSIGNED: 0,
    SUCCEEDED: 0,
    FAILED: 0,
  };

  constructor(options: SchedulerOptions) {
    this.maxAttempts = Math.max(1, Number(options.maxAttempts) || 1);
    this.now = options.now || Date.now;
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

  requestJob(workerId: string): { hasJob: false } | { hasJob: true; job: Pick<JobRecord, "jobId" | "idempotencyKey" | "payload"> } {
    const jobs = this.requestJobs(workerId, 1);
    if (jobs.length === 0) return { hasJob: false };

    return { hasJob: true, job: jobs[0]! };
  }

  requestJobs(workerId: string, maxJobs: number): Array<Pick<JobRecord, "jobId" | "idempotencyKey" | "payload">> {
    const requested = Math.max(0, Math.floor(Number(maxJobs) || 0));
    const assignedJobs: Array<Pick<JobRecord, "jobId" | "idempotencyKey" | "payload">> = [];

    while (this.queueHead < this.queue.length) {
      if (assignedJobs.length >= requested) break;

      const jobId = this.queue[this.queueHead];
      this.queueHead += 1;
      if (!jobId) continue;

      const job = this.jobs.get(jobId);
      if (!job || job.state !== "QUEUED") continue;

      if (this.completedByIdempotency.has(job.idempotencyKey)) {
        this.transition(job, "SUCCEEDED");
        continue;
      }

      this.transition(job, "ASSIGNED");
      job.assignedTo = workerId;
      job.assignedAtMs = this.now();
      job.attempts += 1;

      assignedJobs.push({
        jobId: job.jobId,
        idempotencyKey: job.idempotencyKey,
        payload: job.payload,
      });
    }

    this.compactQueueIfNeeded();
    return assignedJobs;
  }

  reportResult(jobId: string, idempotencyKey: string, success: boolean): { ok: boolean } {
    const job = this.jobs.get(jobId);
    if (!job || job.idempotencyKey !== idempotencyKey) {
      return { ok: false };
    }

    if (this.completedByIdempotency.has(idempotencyKey)) {
      return { ok: true };
    }

    job.assignedTo = undefined;
    job.assignedAtMs = undefined;

    if (success) {
      this.transition(job, "SUCCEEDED");
      this.completedByIdempotency.add(idempotencyKey);
      this.counters.succeededCount += 1;
      return { ok: true };
    }

    this.counters.failedCount += 1;
    if (job.attempts < this.maxAttempts) {
      this.transition(job, "QUEUED");
      this.enqueue(jobId);
      this.counters.retriedCount += 1;
      return { ok: true };
    }

    this.transition(job, "FAILED");
    return { ok: true };
  }

  requeueAssignedJobs(jobIds: string[]): number {
    let requeued = 0;

    for (const jobId of jobIds) {
      const job = this.jobs.get(jobId);
      if (!job || job.state !== "ASSIGNED") continue;

      job.assignedTo = undefined;
      job.assignedAtMs = undefined;
      this.transition(job, "QUEUED");
      this.enqueue(jobId);
      requeued += 1;
    }

    this.counters.jobsRequeuedFromDeadWorkers += requeued;
    return requeued;
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
    };
  }

  queueDepth(): number {
    return this.queue.length - this.queueHead;
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.jobs.get(jobId);
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

  private compactQueueIfNeeded(): void {
    if (this.queueHead < 1024 || this.queueHead * 2 < this.queue.length) return;

    this.queue.splice(0, this.queueHead);
    this.queueHead = 0;
  }
}
