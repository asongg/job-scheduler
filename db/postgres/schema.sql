CREATE TABLE IF NOT EXISTS scheduler_snapshots (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  version integer NOT NULL,
  snapshot jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_state') THEN
    CREATE TYPE job_state AS ENUM ('QUEUED', 'ASSIGNED', 'SUCCEEDED', 'FAILED');
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS jobs (
  job_id text PRIMARY KEY,
  idempotency_key text NOT NULL UNIQUE,
  payload text NOT NULL,
  state job_state NOT NULL,
  assigned_to text,
  assigned_at timestamptz,
  current_attempt_id text,
  lease_expires_at timestamptz,
  available_at timestamptz DEFAULT now(),
  last_failure_reason text,
  attempts integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_ready_available_idx
  ON jobs (available_at, created_at, job_id)
  WHERE state = 'QUEUED';

CREATE INDEX IF NOT EXISTS jobs_lease_expiry_idx
  ON jobs (lease_expires_at)
  WHERE state = 'ASSIGNED' AND lease_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_attempts (
  attempt_id text PRIMARY KEY,
  job_id text NOT NULL REFERENCES jobs (job_id),
  worker_id text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  lease_expires_at timestamptz NOT NULL,
  finished_at timestamptz,
  success boolean,
  output text,
  duration_ms bigint,
  retry_scheduled boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS job_attempts_job_idx ON job_attempts (job_id, started_at DESC);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS available_at timestamptz DEFAULT now();
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_failure_reason text;
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS retry_scheduled boolean NOT NULL DEFAULT false;
