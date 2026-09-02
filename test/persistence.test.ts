import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import test from "node:test";
import {
  createSchedulerPersistence,
  FileSchedulerPersistence,
  PostgresSnapshotPersistence,
} from "../controller/src/persistence";
import { JobScheduler } from "../controller/src/scheduler";

test("FileSchedulerPersistence saves and loads scheduler snapshots", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "dist-scheduler-"));
  const filePath = path.join(dir, "scheduler-state.json");
  const persistence = new FileSchedulerPersistence(filePath);
  const scheduler = new JobScheduler({ maxAttempts: 3, leaseMs: 30000 });
  const submitted = scheduler.submitJob("idem-1", "sleep:1");

  await persistence.save(scheduler.toSnapshot());
  const snapshot = await persistence.load();

  assert.equal(snapshot?.version, 1);
  const restored = snapshot && JobScheduler.fromSnapshot({ maxAttempts: 3, leaseMs: 30000 }, snapshot);
  assert.equal(restored?.submitJob("idem-1", "sleep:ignored").jobId, submitted.jobId);
});

test("createSchedulerPersistence prefers DATABASE_URL over STATE_FILE", () => {
  const persistence = createSchedulerPersistence({
    DATABASE_URL: "postgres://user:pass@localhost:5432/db",
    STATE_FILE: "/tmp/state.json",
  });

  assert.ok(persistence instanceof PostgresSnapshotPersistence);
});

test("createSchedulerPersistence uses file snapshots when only STATE_FILE is set", () => {
  const persistence = createSchedulerPersistence({
    STATE_FILE: "/tmp/state.json",
  });

  assert.ok(persistence instanceof FileSchedulerPersistence);
});
