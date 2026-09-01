import type { JobScheduler } from "./scheduler";
import type { WorkerRegistry } from "./registry";

export function metricsSnapshot(scheduler: JobScheduler, registry: WorkerRegistry) {
  return {
    time: new Date().toISOString(),
    workers: registry.snapshot(),
    queueDepth: scheduler.queueDepth(),
    jobs: scheduler.metrics(),
    faults: {
      ...registry.faultSnapshot(),
      ...scheduler.faultMetrics(),
    },
  };
}
