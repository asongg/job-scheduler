import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as http from "http";
import { MemoryJobStore, type JobStore } from "./jobStore";
import { metricsSnapshot } from "./metrics";
import { createSchedulerPersistence, type SchedulerPersistence } from "./persistence";
import { PostgresJobStore } from "./postgresJobStore";
import { WorkerRegistry } from "./registry";
import { JobScheduler } from "./scheduler";

const PROTO_PATH = path.join(__dirname, "../../proto/scheduler.proto");
const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef) as any;

function envNumber(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

const HEARTBEAT_INTERVAL_MS = envNumber("HEARTBEAT_INTERVAL_MS", 1000);
const HEARTBEAT_TIMEOUT_MS = envNumber("HEARTBEAT_TIMEOUT_MS", 3000);
const MAX_ATTEMPTS = envNumber("MAX_ATTEMPTS", 3);
const JOB_LEASE_MS = envNumber("JOB_LEASE_MS", 30000);
const RETRY_BACKOFF_BASE_MS = envNumber("RETRY_BACKOFF_BASE_MS", 1000);
const RETRY_BACKOFF_MAX_MS = envNumber("RETRY_BACKOFF_MAX_MS", 30000);
const GRPC_ADDR = process.env.GRPC_ADDR || "0.0.0.0:50051";
const SCHEDULER_BACKEND = process.env.SCHEDULER_BACKEND || "memory";

const registry = new WorkerRegistry({ heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS });
let memoryScheduler = new JobScheduler({
  maxAttempts: MAX_ATTEMPTS,
  leaseMs: JOB_LEASE_MS,
  retryBackoffBaseMs: RETRY_BACKOFF_BASE_MS,
  retryBackoffMaxMs: RETRY_BACKOFF_MAX_MS,
});
let jobStore: JobStore = new MemoryJobStore(memoryScheduler, schedulePersist);
let closeJobStore: (() => Promise<void>) | undefined;
const persistence: SchedulerPersistence | undefined =
  SCHEDULER_BACKEND === "memory" ? createSchedulerPersistence(process.env) : undefined;
let persistTimer: NodeJS.Timeout | undefined;

function schedulePersist() {
  if (!persistence) return;
  if (persistTimer) return;

  persistTimer = setTimeout(() => {
    persistTimer = undefined;
    persistence.save(memoryScheduler.toSnapshot()).catch((error) => {
      console.error("Failed to persist scheduler snapshot", error);
    });
  }, 100);
}

async function failureDetectorLoop() {
  for (const deadWorker of registry.detectDeadWorkers()) {
    const recovery = await jobStore.requeueAssignedJobs(deadWorker.assignedJobIds);
    console.warn(
      `Worker ${deadWorker.workerId} considered DEAD. Re-queued ${recovery.requeued} assigned jobs; marked ${recovery.failed} failed.`
    );
  }

  for (const expiredLease of await jobStore.requeueExpiredLeases()) {
    registry.finishJob(expiredLease.workerId, expiredLease.jobId);
    const action = expiredLease.requeued ? "Re-queued" : "Marked failed";
    console.warn(`Job ${expiredLease.jobId} lease expired. ${action} from ${expiredLease.workerId}.`);
  }
}

function handleUnary(callback: grpc.sendUnaryData<any>, fn: () => Promise<any>) {
  fn()
    .then((result) => callback(null, result))
    .catch((error) => callback(error as grpc.ServiceError, null));
}

const serviceImpl = {
  RegisterWorker(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, capacity } = call.request;
    const worker = registry.register(workerId, capacity);
    if (!worker) return callback(null, { ok: false, heartbeatIntervalMs: 0, heartbeatTimeoutMs: 0 });

    console.log(`Registered worker ${workerId} cap=${worker.capacity}`);
    callback(null, { ok: true, heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS, heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS });
  },

  Heartbeat(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, runningJobs } = call.request;
    callback(null, { ok: registry.heartbeat(workerId, runningJobs) });
  },

  RequestJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { workerId } = call.request;
      if (!registry.canAcceptJob(workerId)) {
        return { hasJob: false };
      }

      const result = await jobStore.requestJob(workerId);
      if (result.hasJob) {
        registry.assignJob(workerId, result.job.jobId);
      }

      return result;
    });
  },

  RequestJobs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { workerId, maxJobs } = call.request;
      const availableCapacity = registry.availableCapacity(workerId);
      if (availableCapacity <= 0) {
        return { jobs: [] };
      }

      const jobs = await jobStore.requestJobs(workerId, Math.min(availableCapacity, Number(maxJobs) || 1));
      for (const job of jobs) {
        registry.assignJob(workerId, job.jobId);
      }

      return { jobs };
    });
  },

  ReportResult(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { workerId, jobId, idempotencyKey, attemptId, success, output, durationMs } = call.request;
      const result = await jobStore.reportResult(
        workerId,
        jobId,
        idempotencyKey,
        attemptId,
        Boolean(success),
        output,
        Number(durationMs) || 0
      );
      if (result.ok) {
        registry.finishJob(workerId, jobId);
      }
      return result;
    });
  },

  ReportResults(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const results = Array.isArray(call.request.results) ? call.request.results : [];
      return {
        results: await Promise.all(results.map(async (result: any) => {
          const report = await jobStore.reportResult(
            result.workerId,
            result.jobId,
            result.idempotencyKey,
            result.attemptId,
            Boolean(result.success),
            result.output,
            Number(result.durationMs) || 0
          );
          if (report.ok) {
            registry.finishJob(result.workerId, result.jobId);
          }
          return report;
        })),
      };
    });
  },

  ExtendLease(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { workerId, jobId, idempotencyKey, attemptId } = call.request;
      return jobStore.extendLease(workerId, jobId, idempotencyKey, attemptId);
    });
  },

  ExtendLeases(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const leases = Array.isArray(call.request.leases) ? call.request.leases : [];
      return {
        results: await Promise.all(leases.map((lease: any) => jobStore.extendLease(
          lease.workerId,
          lease.jobId,
          lease.idempotencyKey,
          lease.attemptId
        ))),
      };
    });
  },

  ListFailedJobs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      return { jobs: await jobStore.listFailedJobs(Number(call.request.limit) || 100) };
    });
  },

  ReplayFailedJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { jobId, preserveAttempts } = call.request;
      return jobStore.replayFailedJob(jobId, Boolean(preserveAttempts));
    });
  },

  SubmitJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const { idempotencyKey, payload } = call.request;
      return jobStore.submitJob(idempotencyKey, payload);
    });
  },

  SubmitJobs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    handleUnary(callback, async () => {
      const jobs = Array.isArray(call.request.jobs) ? call.request.jobs : [];
      return {
        results: await Promise.all(jobs.map((job: any) => jobStore.submitJob(job.idempotencyKey, job.payload))),
      };
    });
  },
};

function startMetricsServer() {
  const port = envNumber("METRICS_PORT", 8080);
  const server = http.createServer(async (req, res) => {
    if (req.url === "/metrics" || req.url === "/metrics.json") {
      try {
        const body = JSON.stringify(await metricsSnapshot(jobStore, registry), null, 2);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(body);
      } catch (error) {
        console.error("Failed to build metrics snapshot", error);
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("Failed to build metrics snapshot\n");
      }
      return;
    }
    res.writeHead(404);
    res.end("Not found. Try /metrics\n");
  });

  server.listen(port, "0.0.0.0", () => {
    console.log(`Metrics listening on http://localhost:${port}/metrics`);
  });
}

async function main() {
  if (SCHEDULER_BACKEND === "postgres") {
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error("SCHEDULER_BACKEND=postgres requires DATABASE_URL");
    }

    const postgresStore = PostgresJobStore.fromConnectionString(databaseUrl, {
      maxAttempts: MAX_ATTEMPTS,
      leaseMs: JOB_LEASE_MS,
      retryBackoffBaseMs: RETRY_BACKOFF_BASE_MS,
      retryBackoffMaxMs: RETRY_BACKOFF_MAX_MS,
    });
    await postgresStore.initialize();
    jobStore = postgresStore;
    closeJobStore = () => postgresStore.close();
    console.log("Using Postgres job store backend");
  } else if (SCHEDULER_BACKEND === "memory") {
    if (!persistence) {
      console.log("Using in-memory job store backend");
      startController();
      return;
    }

    const snapshot = await persistence.load();
    if (snapshot) {
      memoryScheduler = JobScheduler.fromSnapshot({
        maxAttempts: MAX_ATTEMPTS,
        leaseMs: JOB_LEASE_MS,
        retryBackoffBaseMs: RETRY_BACKOFF_BASE_MS,
        retryBackoffMaxMs: RETRY_BACKOFF_MAX_MS,
      }, snapshot);
      jobStore = new MemoryJobStore(memoryScheduler, schedulePersist);
      console.log("Loaded scheduler snapshot from persistence");
    }
    console.log("Using in-memory job store backend");
  } else {
    throw new Error(`Unknown SCHEDULER_BACKEND=${SCHEDULER_BACKEND}`);
  }

  startController();
}

function startController() {
  startMetricsServer();
  let detectorRunning = false;
  setInterval(() => {
    if (detectorRunning) return;
    detectorRunning = true;
    failureDetectorLoop()
      .catch((error) => console.error("Failure detector loop failed", error))
      .finally(() => {
        detectorRunning = false;
      });
  }, 500);

  const server = new grpc.Server();
  server.addService(proto.scheduler.Scheduler.service, serviceImpl);

  server.bindAsync(GRPC_ADDR, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`Controller listening on ${GRPC_ADDR}`);
    server.start();
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

process.on("SIGTERM", () => {
  closeJobStore?.().finally(() => process.exit(0));
});
