import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { randomUUID } from "crypto";

const PROTO_PATH = path.join(__dirname, "../../proto/scheduler.proto");
const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef) as any;

const CONTROLLER_ADDR = process.env.CONTROLLER_ADDR || "controller:50051";
const WORKER_ID = process.env.WORKER_ID || `worker-${randomUUID().slice(0, 8)}`;
const CAPACITY = Number(process.env.CAPACITY || "2");
const IDLE_POLL_MS = Number(process.env.IDLE_POLL_MS || "100");
const FULL_POLL_MS = Number(process.env.FULL_POLL_MS || "25");
const LEASE_RENEW_INTERVAL_MS = Number(process.env.LEASE_RENEW_INTERVAL_MS || "5000");
const LEASE_RENEW_THRESHOLD_MS = Number(process.env.LEASE_RENEW_THRESHOLD_MS || "10000");

const client = new proto.scheduler.Scheduler(
  CONTROLLER_ADDR,
  grpc.credentials.createInsecure()
);

let hbIntervalMs = 1000;
let runningJobs = 0;
let renewingLeases = false;

type RunningLease = {
  jobId: string;
  idempotencyKey: string;
  attemptId: string;
  leaseExpiresAtMs: number;
};

const runningLeases = new Map<string, RunningLease>();

function rpc<TReq, TRes>(fn: Function, req: TReq): Promise<TRes> {
  return new Promise((resolve, reject) => {
    fn.call(client, req, (err: any, res: any) => (err ? reject(err) : resolve(res)));
  });
}

async function register() {
  const res: any = await rpc(client.RegisterWorker, { workerId: WORKER_ID, capacity: CAPACITY });
  if (!res.ok) throw new Error("register failed");
  hbIntervalMs = Number(res.heartbeatIntervalMs) || 1000;
  console.log(`Registered as ${WORKER_ID} cap=${CAPACITY} hb=${hbIntervalMs}ms`);
}

async function heartbeatLoop() {
  setInterval(async () => {
    try {
      await rpc(client.Heartbeat, { workerId: WORKER_ID, runningJobs });
    } catch {
    }
  }, hbIntervalMs);
}

function leaseRenewalLoop() {
  setInterval(async () => {
    if (renewingLeases || runningLeases.size === 0) return;

    const now = Date.now();
    const dueLeases = Array.from(runningLeases.values()).filter(
      (lease) => lease.leaseExpiresAtMs - now <= LEASE_RENEW_THRESHOLD_MS
    );
    if (dueLeases.length === 0) return;

    renewingLeases = true;
    try {
      const res: any = await rpc(client.ExtendLeases, {
        leases: dueLeases.map((lease) => ({
          workerId: WORKER_ID,
          jobId: lease.jobId,
          idempotencyKey: lease.idempotencyKey,
          attemptId: lease.attemptId,
        })),
      });

      const results = Array.isArray(res.results) ? res.results : [];
      for (let index = 0; index < dueLeases.length; index += 1) {
        const lease = dueLeases[index]!;
        const result = results[index];
        if (!result?.ok) {
          console.warn(`Lease renewal rejected for job ${lease.jobId} attempt ${lease.attemptId}`);
          continue;
        }

        const runningLease = runningLeases.get(lease.jobId);
        if (runningLease?.attemptId === lease.attemptId) {
          runningLease.leaseExpiresAtMs = Number(result.leaseExpiresAtMs) || lease.leaseExpiresAtMs;
        }
      }
    } catch {
    } finally {
      renewingLeases = false;
    }
  }, Math.max(100, LEASE_RENEW_INTERVAL_MS));
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function fib(n: number): number {
  if (n <= 1) return n;
  let a = 0, b = 1;
  for (let i = 2; i <= n; i++) {
    const c = a + b;
    a = b;
    b = c;
  }
  return b;
}

async function executePayload(payload: string): Promise<string> {
  const [kind, arg] = payload.split(":");
  if (kind === "sleep") {
    const ms = Number(arg || "100");
    await sleep(ms);
    return `slept ${ms}ms`;
  }
  if (kind === "fib") {
    const n = Number(arg || "25");
    const t0 = Date.now();
    const v = fib(n);
    const dt = Date.now() - t0;
    return `fib(${n})=${v} in ${dt}ms`;
  }
  return `unknown payload=${payload}`;
}

async function workLoop() {
  while (true) {
    if (runningJobs >= CAPACITY) {
      await sleep(FULL_POLL_MS);
      continue;
    }

    let res: any;
    try {
      res = await rpc(client.RequestJobs, { workerId: WORKER_ID, maxJobs: CAPACITY - runningJobs });
    } catch {
      await sleep(200);
      continue;
    }

    const jobs = Array.isArray(res.jobs) ? res.jobs : [];
    if (jobs.length === 0) {
      await sleep(IDLE_POLL_MS);
      continue;
    }

    for (const job of jobs) {
      runningJobs += 1;
      runningLeases.set(job.jobId, {
        jobId: job.jobId,
        idempotencyKey: job.idempotencyKey,
        attemptId: job.attemptId,
        leaseExpiresAtMs: Number(job.leaseExpiresAtMs) || 0,
      });

      (async () => {
        const t0 = Date.now();
        let success = true;
        let output = "";
        try {
          output = await executePayload(job.payload);
        } catch (e: any) {
          success = false;
          output = String(e?.message || e);
        }
        const durationMs = Date.now() - t0;

        try {
          await rpc(client.ReportResults, {
            results: [{
              workerId: WORKER_ID,
              jobId: job.jobId,
              idempotencyKey: job.idempotencyKey,
              attemptId: job.attemptId,
              success,
              output,
              durationMs,
            }],
          });
        } catch {
        } finally {
          runningLeases.delete(job.jobId);
          runningJobs -= 1;
        }
      })();
    }
  }
}

(async function main() {
  await register();
  await heartbeatLoop();
  leaseRenewalLoop();
  await workLoop();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
