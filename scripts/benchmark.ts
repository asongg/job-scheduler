import http from "http";
import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { randomUUID } from "crypto";

const PROTO_PATH = path.join(__dirname, "../proto/scheduler.proto");
const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef) as any;

const GRPC_ADDR = process.env.CONTROLLER_ADDR || "localhost:50051";
const METRICS_URL = process.env.METRICS_URL || "http://localhost:8080/metrics";

const N = Number(process.env.N || "500");
const MODE = process.env.MODE || "mixed";
const SUBMIT_BATCH_SIZE = Number(process.env.SUBMIT_BATCH_SIZE || "100");

const client = new proto.scheduler.Scheduler(GRPC_ADDR, grpc.credentials.createInsecure());

function rpc<TReq, TRes>(fn: Function, req: TReq): Promise<TRes> {
  return new Promise((resolve, reject) => {
    fn.call(client, req, (err: any, res: any) => (err ? reject(err) : resolve(res)));
  });
}

function fetchJson(url: string): Promise<any> {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on("error", reject);
  });
}

function payloadFor(i: number) {
  if (MODE === "sleep") return "sleep:50";
  if (MODE === "fib") return "fib:27";
  return i % 2 === 0 ? "sleep:50" : "fib:27";
}

async function waitUntilDone(targetSucceededIncrease: number) {
  const start = Date.now();
  const baseline = await fetchJson(METRICS_URL);
  const baseSucceeded = baseline.jobs.succeededCount as number;

  while (true) {
    const m = await fetchJson(METRICS_URL);
    const succeeded = m.jobs.succeededCount as number;
    const delta = succeeded - baseSucceeded;

    if (delta >= targetSucceededIncrease) {
      return { elapsedMs: Date.now() - start, final: m, baseline };
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

(async () => {
  console.log(`Benchmark: submitting N=${N} jobs to ${GRPC_ADDR}`);
  console.log(`Metrics: ${METRICS_URL}`);

  const t0 = Date.now();

  for (let i = 0; i < N; i += SUBMIT_BATCH_SIZE) {
    const jobs = [];
    for (let j = i; j < Math.min(i + SUBMIT_BATCH_SIZE, N); j++) {
      jobs.push({
        idempotencyKey: `bench-${randomUUID()}`,
        payload: payloadFor(j),
      });
    }
    await rpc(client.SubmitJobs, { jobs });
  }

  const submitMs = Date.now() - t0;
  console.log(`Submitted ${N} jobs in ${submitMs} ms`);

  const done = await waitUntilDone(N);
  const totalMs = done.elapsedMs;

  const throughput = (N / (totalMs / 1000)).toFixed(2);

  console.log(`Completed ${N} jobs in ${totalMs} ms`);
  console.log(`Throughput: ${throughput} jobs/sec`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
