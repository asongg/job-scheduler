import type {
  AssignedJob,
  FailedJob,
  JobMetrics,
  JobQueueMetrics,
  JobRecoveryOutcome,
  JobRecoverySummary,
  JobScheduler,
} from "./scheduler";

export type JobStoreFaultMetrics = {
  jobsRequeuedFromDeadWorkers: number;
  jobsRequeuedFromExpiredLeases: number;
  jobsFailedFromDeadWorkers: number;
  jobsFailedFromExpiredLeases: number;
};

export interface JobStore {
  submitJob(idempotencyKey: string | undefined, payload: string): Promise<{ ok: boolean; jobId: string }>;
  requestJob(workerId: string): Promise<{ hasJob: false } | { hasJob: true; job: AssignedJob }>;
  requestJobs(workerId: string, maxJobs: number): Promise<AssignedJob[]>;
  reportResult(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string,
    success: boolean,
    output: string,
    durationMs: number
  ): Promise<{ ok: boolean }>;
  extendLease(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string
  ): Promise<{ ok: boolean; leaseExpiresAtMs: number }>;
  requeueAssignedJobs(jobIds: string[]): Promise<JobRecoverySummary>;
  requeueExpiredLeases(): Promise<JobRecoveryOutcome[]>;
  listFailedJobs(limit?: number): Promise<FailedJob[]>;
  replayFailedJob(jobId: string, preserveAttempts?: boolean): Promise<{ ok: boolean; jobId: string }>;
  metrics(): Promise<JobMetrics>;
  queueMetrics(): Promise<JobQueueMetrics>;
  queueDepth(): Promise<number>;
  faultMetrics(): Promise<JobStoreFaultMetrics>;
}

export class MemoryJobStore implements JobStore {
  constructor(
    private readonly scheduler: JobScheduler,
    private readonly onChange: () => void = () => undefined
  ) {}

  async submitJob(idempotencyKey: string | undefined, payload: string) {
    const result = this.scheduler.submitJob(idempotencyKey, payload);
    if (result.ok) this.onChange();
    return result;
  }

  async requestJob(workerId: string) {
    const result = this.scheduler.requestJob(workerId);
    if (result.hasJob) this.onChange();
    return result;
  }

  async requestJobs(workerId: string, maxJobs: number) {
    const jobs = this.scheduler.requestJobs(workerId, maxJobs);
    if (jobs.length > 0) this.onChange();
    return jobs;
  }

  async reportResult(
    workerId: string,
    jobId: string,
    idempotencyKey: string,
    attemptId: string,
    success: boolean,
    _output: string,
    _durationMs: number
  ) {
    const result = this.scheduler.reportResult(workerId, jobId, idempotencyKey, attemptId, success);
    if (result.ok) this.onChange();
    return result;
  }

  async extendLease(workerId: string, jobId: string, idempotencyKey: string, attemptId: string) {
    const result = this.scheduler.extendLease(workerId, jobId, idempotencyKey, attemptId);
    if (result.ok) this.onChange();
    return result;
  }

  async requeueAssignedJobs(jobIds: string[]) {
    const summary = this.scheduler.requeueAssignedJobs(jobIds);
    if (summary.requeued > 0 || summary.failed > 0) this.onChange();
    return summary;
  }

  async requeueExpiredLeases() {
    const expired = this.scheduler.requeueExpiredLeases();
    if (expired.length > 0) this.onChange();
    return expired;
  }

  async listFailedJobs(limit?: number) {
    return this.scheduler.listFailedJobs(limit);
  }

  async replayFailedJob(jobId: string, preserveAttempts = false) {
    const result = this.scheduler.replayFailedJob(jobId, preserveAttempts);
    if (result.ok) this.onChange();
    return result;
  }

  async metrics() {
    return this.scheduler.metrics();
  }

  async queueMetrics() {
    return this.scheduler.queueMetrics();
  }

  async queueDepth() {
    return this.scheduler.queueDepth();
  }

  async faultMetrics() {
    return this.scheduler.faultMetrics();
  }
}
