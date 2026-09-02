import * as fs from "fs/promises";
import * as path from "path";
import { Pool } from "pg";
import type { SchedulerSnapshot } from "./scheduler";

export interface SchedulerPersistence {
  load(): Promise<SchedulerSnapshot | undefined>;
  save(snapshot: SchedulerSnapshot): Promise<void>;
}

export class FileSchedulerPersistence implements SchedulerPersistence {
  private saveInFlight: Promise<void> = Promise.resolve();

  constructor(private readonly filePath: string) {}

  async load(): Promise<SchedulerSnapshot | undefined> {
    try {
      const body = await fs.readFile(this.filePath, "utf8");
      const snapshot = JSON.parse(body) as SchedulerSnapshot;
      if (snapshot.version !== 1) {
        throw new Error(`unsupported scheduler snapshot version: ${String(snapshot.version)}`);
      }
      return snapshot;
    } catch (error: any) {
      if (error?.code === "ENOENT") return undefined;
      throw error;
    }
  }

  save(snapshot: SchedulerSnapshot): Promise<void> {
    this.saveInFlight = this.saveInFlight
      .catch(() => undefined)
      .then(() => this.writeSnapshot(snapshot));
    return this.saveInFlight;
  }

  private async writeSnapshot(snapshot: SchedulerSnapshot): Promise<void> {
    const dir = path.dirname(this.filePath);
    const tmpPath = path.join(dir, `${path.basename(this.filePath)}.tmp`);
    const body = `${JSON.stringify(snapshot, null, 2)}\n`;

    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(tmpPath, body, "utf8");
    await fs.rename(tmpPath, this.filePath);
  }
}

export class PostgresSnapshotPersistence implements SchedulerPersistence {
  private readonly pool: Pool;
  private ready: Promise<void> | undefined;
  private saveInFlight: Promise<void> = Promise.resolve();

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async load(): Promise<SchedulerSnapshot | undefined> {
    await this.ensureReady();
    const result = await this.pool.query<{ snapshot: SchedulerSnapshot }>(
      "SELECT snapshot FROM scheduler_snapshots WHERE id = true"
    );

    return result.rows[0]?.snapshot;
  }

  async save(snapshot: SchedulerSnapshot): Promise<void> {
    this.saveInFlight = this.saveInFlight
      .catch(() => undefined)
      .then(() => this.writeSnapshot(snapshot));
    return this.saveInFlight;
  }

  private async writeSnapshot(snapshot: SchedulerSnapshot): Promise<void> {
    await this.ensureReady();
    await this.pool.query(
      `
        INSERT INTO scheduler_snapshots (id, version, snapshot, updated_at)
        VALUES (true, $1, $2::jsonb, now())
        ON CONFLICT (id)
        DO UPDATE SET
          version = EXCLUDED.version,
          snapshot = EXCLUDED.snapshot,
          updated_at = now()
      `,
      [snapshot.version, JSON.stringify(snapshot)]
    );
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  private ensureReady(): Promise<void> {
    if (!this.ready) {
      this.ready = this.pool.query(`
        CREATE TABLE IF NOT EXISTS scheduler_snapshots (
          id boolean PRIMARY KEY DEFAULT true CHECK (id),
          version integer NOT NULL,
          snapshot jsonb NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      `).then(() => undefined).catch((error) => {
        this.ready = undefined;
        throw error;
      });
    }

    return this.ready;
  }
}

export function createSchedulerPersistence(env: NodeJS.ProcessEnv): SchedulerPersistence | undefined {
  if (env.DATABASE_URL) {
    return new PostgresSnapshotPersistence(env.DATABASE_URL);
  }

  if (env.STATE_FILE) {
    return new FileSchedulerPersistence(env.STATE_FILE);
  }

  return undefined;
}
