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

const ADDR = process.env.CONTROLLER_ADDR || "localhost:50051";

const client = new proto.scheduler.Scheduler(ADDR, grpc.credentials.createInsecure());

function rpc<TReq, TRes>(fn: Function, req: TReq): Promise<TRes> {
  return new Promise((resolve, reject) => {
    fn.call(client, req, (err: any, res: any) => (err ? reject(err) : resolve(res)));
  });
}

// Example payloads your worker already supports: "sleep:200" or "fib:25"
(async () => {
  const n = Number(process.env.N || "50");
  const mode = process.env.MODE || "mixed";

  for (let i = 0; i < n; i++) {
    const payload =
      mode === "sleep" ? "sleep:200" :
      mode === "fib" ? "fib:25" :
      (i % 2 === 0 ? "sleep:200" : "fib:25");

    await rpc(client.SubmitJob, {
      idempotencyKey: `idem-${randomUUID()}`,
      payload,
    });
  }

  console.log(`Submitted ${n} jobs to ${ADDR}`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
