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

const client = new proto.scheduler.Scheduler(
  CONTROLLER_ADDR,
  grpc.credentials.createInsecure()
);

let hbIntervalMs = 1000;
let runningJobs = 0;

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
      // Controller down / network issue — keep trying
    }
  }, hbIntervalMs);
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
  // payload examples: "sleep:200" or "fib:25"
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
      await sleep(25);
      continue;
    }

    let res: any;
    try {
      res = await rpc(client.RequestJob, { workerId: WORKER_ID });
    } catch {
      await sleep(200);
      continue;
    }

    if (!res.hasJob) {
      await sleep(100);
      continue;
    }

    const job = res.job;
    runningJobs += 1;

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
        await rpc(client.ReportResult, {
          workerId: WORKER_ID,
          jobId: job.jobId,
          idempotencyKey: job.idempotencyKey,
          success,
          output,
          durationMs,
        });
      } catch {
        // If report fails, job may be retried (at-least-once semantics)
      } finally {
        runningJobs -= 1;
      }
    })();
  }
}

(async function main() {
  await register();
  await heartbeatLoop();
  await workLoop();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
