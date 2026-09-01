export type WorkerState = {
  workerId: string;
  capacity: number;
  lastHeartbeatMs: number;
  runningJobs: number;
  alive: boolean;
  assignedJobIds: Set<string>;
};

export type WorkerRegistryOptions = {
  heartbeatTimeoutMs: number;
  now?: () => number;
};

export class WorkerRegistry {
  private readonly workers = new Map<string, WorkerState>();
  private readonly heartbeatTimeoutMs: number;
  private readonly now: () => number;
  private workersMarkedDead = 0;

  constructor(options: WorkerRegistryOptions) {
    this.heartbeatTimeoutMs = Math.max(1, Number(options.heartbeatTimeoutMs) || 3000);
    this.now = options.now || Date.now;
  }

  register(workerId: string, capacity: number): WorkerState | undefined {
    if (!workerId) return undefined;

    const state: WorkerState = {
      workerId,
      capacity: Math.max(1, Number(capacity) || 1),
      lastHeartbeatMs: this.now(),
      runningJobs: 0,
      alive: true,
      assignedJobIds: new Set<string>(),
    };

    this.workers.set(workerId, state);
    return state;
  }

  heartbeat(workerId: string, runningJobs: number): boolean {
    const worker = this.workers.get(workerId);
    if (!worker) return false;

    worker.lastHeartbeatMs = this.now();
    worker.runningJobs = Math.max(0, Number(runningJobs) || 0);
    worker.alive = true;
    return true;
  }

  canAcceptJob(workerId: string): boolean {
    return this.availableCapacity(workerId) > 0;
  }

  availableCapacity(workerId: string): number {
    const worker = this.workers.get(workerId);
    if (!worker || !worker.alive) return 0;

    return Math.max(0, worker.capacity - Math.max(worker.runningJobs, worker.assignedJobIds.size));
  }

  assignJob(workerId: string, jobId: string): void {
    const worker = this.workers.get(workerId);
    if (!worker) return;

    worker.assignedJobIds.add(jobId);
  }

  finishJob(workerId: string, jobId: string): void {
    const worker = this.workers.get(workerId);
    if (!worker) return;

    worker.assignedJobIds.delete(jobId);
  }

  detectDeadWorkers(): Array<{ workerId: string; assignedJobIds: string[] }> {
    const deadWorkers: Array<{ workerId: string; assignedJobIds: string[] }> = [];
    const timestamp = this.now();

    for (const worker of this.workers.values()) {
      if (!worker.alive || timestamp - worker.lastHeartbeatMs <= this.heartbeatTimeoutMs) {
        continue;
      }

      worker.alive = false;
      this.workersMarkedDead += 1;
      deadWorkers.push({
        workerId: worker.workerId,
        assignedJobIds: Array.from(worker.assignedJobIds),
      });
      worker.assignedJobIds.clear();
    }

    return deadWorkers;
  }

  snapshot() {
    let alive = 0;
    for (const worker of this.workers.values()) {
      if (worker.alive) alive += 1;
    }

    return {
      total: this.workers.size,
      alive,
    };
  }

  faultSnapshot() {
    return {
      workersMarkedDead: this.workersMarkedDead,
    };
  }
}
