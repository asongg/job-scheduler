import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as http from "http";
import { randomUUID } from "crypto";


type WorkerState = {
  workerId: string;
  capacity: number;
  lastHeartbeatMs: number;
  runningJobs: number;
  alive: boolean;
  assignedJobIds: Set<string>;
};

type JobState = "QUEUED" | "ASSIGNED" | "SUCCEEDED" | "FAILED";

type JobRecord = {
  jobId: string;
  idempotencyKey: string;
  payload: string;
  state: JobState;
  assignedTo?: string | undefined;
  assignedAtMs?: number | undefined;
  attempts: number;
};

const PROTO_PATH = path.join(__dirname, "../../proto/scheduler.proto");
const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef) as any;

const HEARTBEAT_INTERVAL_MS = 1000;
const HEARTBEAT_TIMEOUT_MS = 3000;

const workers = new Map<string, WorkerState>();
const jobsQueue: string[] = [];
const jobs = new Map<string, JobRecord>();
const completedByIdempotency = new Set<string>();

let jobsSubmitted = 0;
let jobsSucceeded = 0;
let jobsFailed = 0;
let jobsRetried = 0;

let workersMarkedDead = 0;
let jobsRequeuedFromDeadWorkers = 0;


function now() {
  return Date.now();
}

function seedJobs() {
  for (let i = 1; i <= 50; i++) {
    const jobId = `job-${i}`;
    const idem = `idem-${i}`;
    const payload = i % 2 === 0 ? "sleep:200" : "fib:25";
    jobs.set(jobId, { jobId, idempotencyKey: idem, payload, state: "QUEUED", attempts: 0 });
    jobsQueue.push(jobId);
  }
  console.log(`Seeded ${jobsQueue.length} jobs`);
}

function markWorkerDead(workerId: string) {
  const w = workers.get(workerId);
  if (!w || !w.alive) return;

  w.alive = false;
  console.warn(`Worker ${workerId} considered DEAD. Re-queuing its assigned jobs...`);

  for (const jobId of w.assignedJobIds) {
    const jr = jobs.get(jobId);
    if (!jr) continue;
    if (jr.state === "ASSIGNED") {
      workersMarkedDead += 1;
      jr.state = "QUEUED";
      jr.assignedTo = undefined;
      jr.assignedAtMs = undefined;
      jobsQueue.push(jobId);
      jobsRequeuedFromDeadWorkers += 1;
    }
  }
  w.assignedJobIds.clear();
}

function failureDetectorLoop() {
  const t = now();
  for (const [id, w] of workers.entries()) {
    if (w.alive && t - w.lastHeartbeatMs > HEARTBEAT_TIMEOUT_MS) {
      markWorkerDead(id);
    }
  }
}

function metricsSnapshot() {
    const aliveWorkers = Array.from(workers.values()).filter((w) => w.alive).length;
  
    let queued = 0, assigned = 0, succeeded = 0, failed = 0;
    for (const jr of jobs.values()) {
      if (jr.state === "QUEUED") queued++;
      else if (jr.state === "ASSIGNED") assigned++;
      else if (jr.state === "SUCCEEDED") succeeded++;
      else if (jr.state === "FAILED") failed++;
    }
  
    return {
      time: new Date().toISOString(),
      workers: {
        total: workers.size,
        alive: aliveWorkers,
      },
      queueDepth: jobsQueue.length,
      jobs: {
        totalTracked: jobs.size,
        queued,
        assigned,
        succeeded,
        failed,
        submitted: jobsSubmitted,
        succeededCount: jobsSucceeded,
        failedCount: jobsFailed,
        retriedCount: jobsRetried,
      },
      faults: {
        workersMarkedDead,
        jobsRequeuedFromDeadWorkers
      },
    };
  }
  
setInterval(failureDetectorLoop, 500);

const serviceImpl = {
  RegisterWorker(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, capacity } = call.request;
    if (!workerId) return callback(null, { ok: false, heartbeatIntervalMs: 0, heartbeatTimeoutMs: 0 });

    const st: WorkerState = {
      workerId,
      capacity: Math.max(1, Number(capacity) || 1),
      lastHeartbeatMs: now(),
      runningJobs: 0,
      alive: true,
      assignedJobIds: new Set<string>(),
    };
    workers.set(workerId, st);

    console.log(`Registered worker ${workerId} cap=${st.capacity}`);
    callback(null, { ok: true, heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS, heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS });
  },

  Heartbeat(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, runningJobs } = call.request;
    const w = workers.get(workerId);
    if (!w) return callback(null, { ok: false });

    w.lastHeartbeatMs = now();
    w.runningJobs = Number(runningJobs) || 0;
    w.alive = true;
    callback(null, { ok: true });
  },

  RequestJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId } = call.request;
    const w = workers.get(workerId);
    if (!w || !w.alive) return callback(null, { hasJob: false });

    if (w.runningJobs >= w.capacity) return callback(null, { hasJob: false });

    while (jobsQueue.length > 0) {
      const jobId = jobsQueue.shift()!;
      const jr = jobs.get(jobId);
      if (!jr) continue;
      if (jr.state !== "QUEUED") continue;

      if (completedByIdempotency.has(jr.idempotencyKey)) {
        jr.state = "SUCCEEDED";
        continue;
      }

      jr.state = "ASSIGNED";
      jr.assignedTo = workerId;
      jr.assignedAtMs = now();
      jr.attempts += 1;

      w.assignedJobIds.add(jobId);

      return callback(null, {
        hasJob: true,
        job: { jobId: jr.jobId, idempotencyKey: jr.idempotencyKey, payload: jr.payload },
      });
    }

    callback(null, { hasJob: false });
  },

  ReportResult(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { workerId, jobId, idempotencyKey, success } = call.request;
    const w = workers.get(workerId);
    const jr = jobs.get(jobId);

    if (w) w.assignedJobIds.delete(jobId);

    if (!jr) return callback(null, { ok: false });

    if (completedByIdempotency.has(idempotencyKey)) {
      return callback(null, { ok: true });
    }

    if (success) {
      jr.state = "SUCCEEDED";
      completedByIdempotency.add(idempotencyKey);
      jobsSucceeded += 1;
    } else {
      jr.state = "FAILED";
      jobsFailed += 1;
      if (jr.attempts < 3) {
        jr.state = "QUEUED";
        jr.assignedTo = undefined;
        jr.assignedAtMs = undefined;
        jobsQueue.push(jobId);
        jobsRetried += 1;
      }
    }

    callback(null, { ok: true });
  },

  SubmitJob(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ) {
    const { idempotencyKey, payload } = call.request;
  
    if (!payload || typeof payload !== "string") {
      return callback(null, { ok: false, jobId: "" });
    }
  
    const idem = (idempotencyKey && String(idempotencyKey)) || `idem-${randomUUID()}`;
  
    if (completedByIdempotency.has(idem)) {
      return callback(null, { ok: true, jobId: "" });
    }
  
    const jobId = `job-${randomUUID().slice(0, 8)}`;
  
    jobs.set(jobId, {
      jobId,
      idempotencyKey: idem,
      payload: String(payload),
      state: "QUEUED",
      attempts: 0,
    });
  
    jobsQueue.push(jobId);
    jobsSubmitted += 1;
  
    callback(null, { ok: true, jobId });
  },
};

function startMetricsServer() {
    const port = Number(process.env.METRICS_PORT || "8080");
    const server = http.createServer((req, res) => {
      if (req.url === "/metrics" || req.url === "/metrics.json") {
        const body = JSON.stringify(metricsSnapshot(), null, 2);
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

  const addr = "0.0.0.0:50051";
  server.bindAsync(addr, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`Controller listening on ${addr}`);
    server.start();
  });
}

main();
