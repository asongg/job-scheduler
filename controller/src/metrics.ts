import type { JobStore } from "./jobStore";
import type { WorkerRegistry } from "./registry";

export async function metricsSnapshot(jobStore: JobStore, registry: WorkerRegistry) {
  return {
    time: new Date().toISOString(),
    workers: registry.snapshot(),
    queueDepth: await jobStore.queueDepth(),
    queue: await jobStore.queueMetrics(),
    jobs: await jobStore.metrics(),
    faults: {
      ...registry.faultSnapshot(),
      ...(await jobStore.faultMetrics()),
    },
  };
}
