import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as http from "http";
import { metricsSnapshot } from "./metrics";
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
const GRPC_ADDR = process.env.GRPC_ADDR || "0.0.0.0:50051";

const registry = new WorkerRegistry({ heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS });
const scheduler = new JobScheduler({ maxAttempts: MAX_ATTEMPTS });

function failureDetectorLoop() {
  for (const deadWorker of registry.detectDeadWorkers()) {
    const requeued = scheduler.requeueAssignedJobs(deadWorker.assignedJobIds);
    console.warn(`Worker ${deadWorker.workerId} considered DEAD. Re-queued ${requeued} assigned jobs.`);
  }
}

setInterval(failureDetectorLoop, 500);

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
    const { workerId } = call.request;
    if (!registry.canAcceptJob(workerId)) {
      return callback(null, { hasJob: false });
    }

    const result = scheduler.requestJob(workerId);
    if (result.hasJob) {
      registry.assignJob(workerId, result.job.jobId);
    }

    callback(null, result);
  },

  RequestJobs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, maxJobs } = call.request;
    const availableCapacity = registry.availableCapacity(workerId);
    if (availableCapacity <= 0) {
      return callback(null, { jobs: [] });
    }

    const jobs = scheduler.requestJobs(workerId, Math.min(availableCapacity, Number(maxJobs) || 1));
    for (const job of jobs) {
      registry.assignJob(workerId, job.jobId);
    }

    callback(null, { jobs });
  },

  ReportResult(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, jobId, idempotencyKey, success } = call.request;
    registry.finishJob(workerId, jobId);
    callback(null, scheduler.reportResult(jobId, idempotencyKey, Boolean(success)));
  },

  ReportResults(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const results = Array.isArray(call.request.results) ? call.request.results : [];
    callback(null, {
      results: results.map((result: any) => {
        registry.finishJob(result.workerId, result.jobId);
        return scheduler.reportResult(result.jobId, result.idempotencyKey, Boolean(result.success));
      }),
    });
  },

  SubmitJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { idempotencyKey, payload } = call.request;
    callback(null, scheduler.submitJob(idempotencyKey, payload));
  },

  SubmitJobs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const jobs = Array.isArray(call.request.jobs) ? call.request.jobs : [];
    callback(null, {
      results: jobs.map((job: any) => scheduler.submitJob(job.idempotencyKey, job.payload)),
    });
  },
};

function startMetricsServer() {
  const port = envNumber("METRICS_PORT", 8080);
  const server = http.createServer((req, res) => {
    if (req.url === "/metrics" || req.url === "/metrics.json") {
      const body = JSON.stringify(metricsSnapshot(scheduler, registry), null, 2);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(body);
      return;
    }
    res.writeHead(404);
    res.end("Not found. Try /metrics\n");
  });

  server.listen(port, "0.0.0.0", () => {
    console.log(`Metrics listening on http://localhost:${port}/metrics`);
  });
}

function main() {
  startMetricsServer();
  const server = new grpc.Server();
  server.addService(proto.scheduler.Scheduler.service, serviceImpl);

  server.bindAsync(GRPC_ADDR, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`Controller listening on ${GRPC_ADDR}`);
    server.start();
  });
}

main();
