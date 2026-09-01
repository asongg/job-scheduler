import assert from "node:assert/strict";
import test from "node:test";
import { WorkerRegistry } from "../controller/src/registry";

test("register clamps worker capacity and tracks availability", () => {
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => 1000 });

  const worker = registry.register("worker-1", 0);

  assert.equal(worker?.capacity, 1);
  assert.equal(registry.canAcceptJob("worker-1"), true);
  assert.deepEqual(registry.snapshot(), { total: 1, alive: 1 });
});

test("heartbeat updates running jobs and capacity checks", () => {
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => 1000 });
  registry.register("worker-1", 2);

  assert.equal(registry.heartbeat("worker-1", 2), true);
  assert.equal(registry.canAcceptJob("worker-1"), false);

  assert.equal(registry.heartbeat("worker-1", 1), true);
  assert.equal(registry.canAcceptJob("worker-1"), true);
});

test("availableCapacity accounts for controller-tracked assignments", () => {
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => 1000 });
  registry.register("worker-1", 3);

  registry.assignJob("worker-1", "job-1");
  registry.assignJob("worker-1", "job-2");

  assert.equal(registry.availableCapacity("worker-1"), 1);

  registry.heartbeat("worker-1", 3);
  assert.equal(registry.availableCapacity("worker-1"), 0);
});

test("detectDeadWorkers marks stale workers and returns their assignments once", () => {
  let timestamp = 1000;
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => timestamp });
  registry.register("worker-1", 2);
  registry.assignJob("worker-1", "job-1");
  registry.assignJob("worker-1", "job-2");

  timestamp = 4501;
  const deadWorkers = registry.detectDeadWorkers();

  assert.deepEqual(deadWorkers, [{ workerId: "worker-1", assignedJobIds: ["job-1", "job-2"] }]);
  assert.equal(registry.canAcceptJob("worker-1"), false);
  assert.deepEqual(registry.snapshot(), { total: 1, alive: 0 });
  assert.equal(registry.faultSnapshot().workersMarkedDead, 1);

  assert.deepEqual(registry.detectDeadWorkers(), []);
  assert.equal(registry.faultSnapshot().workersMarkedDead, 1);
});

test("finishJob removes only the completed assignment", () => {
  let timestamp = 1000;
  const registry = new WorkerRegistry({ heartbeatTimeoutMs: 3000, now: () => timestamp });
  registry.register("worker-1", 2);
  registry.assignJob("worker-1", "job-1");
  registry.assignJob("worker-1", "job-2");
  registry.finishJob("worker-1", "job-1");

  timestamp = 4501;
  const deadWorkers = registry.detectDeadWorkers();
  assert.deepEqual(deadWorkers, [{ workerId: "worker-1", assignedJobIds: ["job-2"] }]);
});
