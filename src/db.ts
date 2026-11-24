// Durable hub state on node:sqlite (WAL). Every mutation runs inside BEGIN IMMEDIATE so concurrent lease calls
// (or a second hub process on the same file) can never hand one job to two workers.
import { randomUUID } from "node:crypto";
import { DatabaseSync, type SQLOutputValue } from "node:sqlite";
import {
  JobSpec, TERMINAL_STATUSES,
  type CompleteRequest, type FleetNode, type JobEvent, type JobEventType, type JobRecord, type JobSpecInput,
  type JobStatus, type NodeHeartbeat,
} from "./protocol.ts";
import { pickNode, type NodeCandidate } from "./scheduler.ts";

export interface DbOptions {
  /** How long a lease lives without a progress renewal. */
  leaseTtlMs?: number;
  /** A node whose last heartbeat is older than this is offline. */
  onlineWindowMs?: number;
  /**
   * A node competes for a job only if it asked for work this recently. Keeps a crashed worker whose heartbeat has
   * not aged out yet from holding jobs it ranks best for (including the one it just lost).
   */
  pollWindowMs?: number;
}

type Row = Record<string, SQLOutputValue>;
type EventListener = (event: JobEvent) => void;

export type ProgressOutcome =
  | { kind: "renewed"; leaseExpiresAt: number }
  | { kind: "stop"; reason: "cancelled" | "lease_lost" | "not_found" };

export type CompleteOutcome =
  | { kind: "completed"; job: JobRecord }
  | { kind: "rejected"; reason: string; job: JobRecord | null };

const SCHEMA = `
CREATE TABLE IF NOT EXISTS nodes (
  node_id   TEXT PRIMARY KEY,
  heartbeat TEXT NOT NULL,
  last_seen INTEGER NOT NULL,
  last_poll INTEGER
);
CREATE TABLE IF NOT EXISTS jobs (
  id               TEXT PRIMARY KEY,
  seq              INTEGER NOT NULL,
  spec             TEXT NOT NULL,
  status           TEXT NOT NULL,
  node_id          TEXT,
  attempt          INTEGER NOT NULL DEFAULT 0,
  lease_expires_at INTEGER,
  idempotency_key  TEXT UNIQUE,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  started_at       INTEGER,
  finished_at      INTEGER,
  result           TEXT,
  error            TEXT
);
CREATE INDEX IF NOT EXISTS jobs_status_seq ON jobs(status, seq);
CREATE INDEX IF NOT EXISTS jobs_node ON jobs(node_id, status);
CREATE TABLE IF NOT EXISTS job_events (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id  TEXT NOT NULL,
  ts      INTEGER NOT NULL,
  type    TEXT NOT NULL,
  node_id TEXT,
  message TEXT,
  data    TEXT
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events(job_id, id);
`;

export class SwarmDb {
  readonly leaseTtlMs: number;
  readonly onlineWindowMs: number;
  readonly pollWindowMs: number;
  private readonly db: DatabaseSync;
  private readonly listeners = new Set<EventListener>();
  private pendingEvents: JobEvent[] = [];

  constructor(path: string, options: DbOptions = {}) {
    this.leaseTtlMs = options.leaseTtlMs ?? 60_000;
    this.onlineWindowMs = options.onlineWindowMs ?? 45_000;
    this.pollWindowMs = options.pollWindowMs ?? 15_000;
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    this.db.exec(SCHEMA);
  }

  close(): void {
    this.db.close();
  }

  /** Subscribe to committed job events. Returns an unsubscribe function. */
  onEvent(listener: EventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  // ---------------- nodes ----------------

  upsertHeartbeat(hb: NodeHeartbeat, now = Date.now()): void {
    this.tx(() => {
      this.db.prepare(`INSERT INTO nodes (node_id, heartbeat, last_seen) VALUES (?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET heartbeat = excluded.heartbeat, last_seen = excluded.last_seen`)
        .run(hb.nodeId, JSON.stringify(hb), now);
    });
  }

  listNodes(now = Date.now()): FleetNode[] {
    const running = this.runningByNode();
    return this.db.prepare("SELECT * FROM nodes ORDER BY node_id").all().map((row) => {
      const heartbeat = JSON.parse(String(row.heartbeat)) as NodeHeartbeat;
      const lastSeen = Number(row.last_seen);
      const runningJobIds = running.get(heartbeat.nodeId) ?? [];
      return {
        nodeId: heartbeat.nodeId,
        online: now - lastSeen <= this.onlineWindowMs,
        lastSeen,
        freeSlots: Math.max(0, heartbeat.maxSlots - runningJobIds.length),
        runningJobIds,
        heartbeat,
      };
    });
  }

  /** Node ids whose last heartbeat is older than the online window. */
  staleNodeIds(now = Date.now()): string[] {
    return this.db.prepare("SELECT node_id FROM nodes WHERE last_seen < ? ORDER BY node_id")
      .all(now - this.onlineWindowMs).map((row) => String(row.node_id));
  }

  // ---------------- jobs ----------------

  /** Creates a job, or returns the existing one when idempotencyKey was seen before. */
  createJob(input: JobSpecInput, now = Date.now()): { job: JobRecord; deduplicated: boolean } {
    const parsed = JobSpec.parse(input);
    return this.tx(() => {
      if (parsed.idempotencyKey) {
        const existing = this.db.prepare("SELECT * FROM jobs WHERE idempotency_key = ?").get(parsed.idempotencyKey);
        if (existing) return { job: toJob(existing), deduplicated: true };
      }
      const id = `job_${randomUUID().replaceAll("-", "").slice(0, 16)}`;
      const spec: JobSpec = { ...parsed, taskBranch: parsed.taskBranch ?? `swarm/${id}` };
      const seq = Number(this.db.prepare("SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM jobs").get()!.n);
      this.db.prepare(`INSERT INTO jobs (id, seq, spec, status, idempotency_key, created_at, updated_at)
        VALUES (?, ?, ?, 'queued', ?, ?, ?)`).run(id, seq, JSON.stringify(spec), spec.idempotencyKey ?? null, now, now);
      this.appendEvent(id, "created", now, null, `queued for cli:${spec.cli}`);
      return { job: this.requireJob(id), deduplicated: false };
    });
  }

  getJob(id: string): JobRecord | null {
    const row = this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(id);
    return row ? toJob(row) : null;
  }

  listJobs(filter: { status?: JobStatus; limit?: number } = {}): JobRecord[] {
    const limit = Math.min(Math.max(filter.limit ?? 100, 1), 1000);
    const rows = filter.status
      ? this.db.prepare("SELECT * FROM jobs WHERE status = ? ORDER BY seq DESC LIMIT ?").all(filter.status, limit)
      : this.db.prepare("SELECT * FROM jobs ORDER BY seq DESC LIMIT ?").all(limit);
    return rows.map(toJob);
  }

  /**
   * Atomically hands the requesting node the oldest queued job for which the scheduler ranks it best among nodes
   * that are online and polling. Jobs better suited to another such node are left for that node's next poll.
   */
  leaseNextJob(nodeId: string, now = Date.now()): JobRecord | null {
    return this.tx(() => {
      this.recoverExpiredLeasesInTx(now);
      if (!this.db.prepare("UPDATE nodes SET last_poll = ? WHERE node_id = ?").run(now, nodeId).changes) return null;
      const polling = new Set(this.db.prepare("SELECT node_id FROM nodes WHERE last_poll >= ?").all(now - this.pollWindowMs)
        .map((row) => String(row.node_id)));
      const candidates: NodeCandidate[] = this.listNodes(now).filter((node) => node.online && polling.has(node.nodeId));
      if (!candidates.some((node) => node.nodeId === nodeId)) return null;
      for (const row of this.db.prepare("SELECT * FROM jobs WHERE status = 'queued' ORDER BY seq").all()) {
        const job = toJob(row);
        if (pickNode(job.spec, candidates) !== nodeId) continue;
        const leaseExpiresAt = now + this.leaseTtlMs;
        this.db.prepare(`UPDATE jobs SET status = 'running', node_id = ?, attempt = attempt + 1, lease_expires_at = ?,
          started_at = ?, updated_at = ? WHERE id = ? AND status = 'queued'`).run(nodeId, leaseExpiresAt, now, now, job.id);
        this.appendEvent(job.id, "leased", now, nodeId, `attempt ${job.attempt + 1}`);
        return this.requireJob(job.id);
      }
      return null;
    });
  }

  /** Renews the lease and records a progress message. Tells the worker to stop when it no longer owns the job. */
  recordProgress(jobId: string, nodeId: string, message: string | undefined, data: unknown, now = Date.now()): ProgressOutcome {
    return this.tx(() => {
      const job = this.getJob(jobId);
      if (!job) return { kind: "stop", reason: "not_found" } as const;
      if (job.status === "cancelled") return { kind: "stop", reason: "cancelled" } as const;
      if (job.status !== "running" || job.nodeId !== nodeId) return { kind: "stop", reason: "lease_lost" } as const;
      const leaseExpiresAt = now + this.leaseTtlMs;
      this.db.prepare("UPDATE jobs SET lease_expires_at = ?, updated_at = ? WHERE id = ?").run(leaseExpiresAt, now, jobId);
      if (message !== undefined || data !== undefined) this.appendEvent(jobId, "progress", now, nodeId, message ?? null, data);
      return { kind: "renewed", leaseExpiresAt } as const;
    });
  }

  completeJob(jobId: string, req: CompleteRequest, now = Date.now()): CompleteOutcome {
    return this.tx(() => {
      const job = this.getJob(jobId);
      if (!job) return { kind: "rejected", reason: "no such job", job: null } as const;
      if (job.status !== "running" || job.nodeId !== req.nodeId) {
        return { kind: "rejected", reason: `job is ${job.status} on ${job.nodeId ?? "no node"}`, job } as const;
      }
      this.db.prepare(`UPDATE jobs SET status = ?, result = ?, error = ?, lease_expires_at = NULL, finished_at = ?, updated_at = ?
        WHERE id = ?`).run(req.status, JSON.stringify(req.result), req.error ?? null, now, now, jobId);
      this.appendEvent(jobId, req.status, now, req.nodeId,
        req.error ?? `exit ${req.result.exitCode ?? "none"}${req.result.verify ? `, verify exit ${req.result.verify.exitCode ?? "none"}` : ""}`,
        { diffStat: req.result.diffStat, branch: req.result.branch, pushed: req.result.pushed });
      return { kind: "completed", job: this.requireJob(jobId) } as const;
    });
  }

  /** Cancels a queued or running job. A running job's worker learns about it on its next progress call. */
  cancelJob(jobId: string, reason = "cancelled by request", now = Date.now()): JobRecord | null {
    return this.tx(() => {
      const job = this.getJob(jobId);
      if (!job) return null;
      if (TERMINAL_STATUSES.has(job.status)) return job;
      this.db.prepare(`UPDATE jobs SET status = 'cancelled', error = ?, lease_expires_at = NULL, finished_at = ?, updated_at = ?
        WHERE id = ?`).run(reason, now, now, jobId);
      this.appendEvent(jobId, "cancelled", now, job.nodeId, reason);
      return this.requireJob(jobId);
    });
  }

  /**
   * Running jobs whose lease ran out, or whose node went offline, go back to the queue while retries remain
   * (attempt <= maxRetries), otherwise fail. Returns the ids it touched.
   */
  recoverExpiredLeases(now = Date.now()): string[] {
    return this.tx(() => this.recoverExpiredLeasesInTx(now));
  }

  // ---------------- events ----------------

  listEvents(jobId: string, afterId = 0): JobEvent[] {
    return this.db.prepare("SELECT * FROM job_events WHERE job_id = ? AND id > ? ORDER BY id").all(jobId, afterId).map(toEvent);
  }

  // ---------------- internals ----------------

  private recoverExpiredLeasesInTx(now: number): string[] {
    const stale = new Set(this.staleNodeIds(now));
    const touched: string[] = [];
    for (const row of this.db.prepare("SELECT * FROM jobs WHERE status = 'running'").all()) {
      const job = toJob(row);
      const nodeGone = job.nodeId !== null && stale.has(job.nodeId);
      if (!nodeGone && (job.leaseExpiresAt ?? Infinity) >= now) continue;
      const why = nodeGone ? `node ${job.nodeId} offline` : `lease expired on ${job.nodeId}`;
      this.appendEvent(job.id, "lease_expired", now, job.nodeId, why);
      if (job.attempt <= job.spec.maxRetries) {
        this.db.prepare(`UPDATE jobs SET status = 'queued', node_id = NULL, lease_expires_at = NULL, started_at = NULL, updated_at = ?
          WHERE id = ?`).run(now, job.id);
        this.appendEvent(job.id, "requeued", now, null, `retry ${job.attempt} of ${job.spec.maxRetries}`);
      } else {
        const error = `${why}; no retries left after ${job.attempt} attempt(s)`;
        this.db.prepare(`UPDATE jobs SET status = 'failed', error = ?, lease_expires_at = NULL, finished_at = ?, updated_at = ?
          WHERE id = ?`).run(error, now, now, job.id);
        this.appendEvent(job.id, "failed", now, job.nodeId, error);
      }
      touched.push(job.id);
    }
    return touched;
  }

  private runningByNode(): Map<string, string[]> {
    const map = new Map<string, string[]>();
    for (const row of this.db.prepare("SELECT id, node_id FROM jobs WHERE status = 'running' ORDER BY seq").all()) {
      const node = String(row.node_id);
      map.set(node, [...(map.get(node) ?? []), String(row.id)]);
    }
    return map;
  }

  private requireJob(id: string): JobRecord {
    const job = this.getJob(id);
    if (!job) throw new Error(`job ${id} vanished inside its own transaction`);
    return job;
  }

  private appendEvent(jobId: string, type: JobEventType, ts: number, nodeId: string | null, message: string | null, data?: unknown): void {
    const info = this.db.prepare("INSERT INTO job_events (job_id, ts, type, node_id, message, data) VALUES (?, ?, ?, ?, ?, ?)")
      .run(jobId, ts, type, nodeId, message, data === undefined ? null : JSON.stringify(data));
    this.pendingEvents.push({ id: Number(info.lastInsertRowid), jobId, ts, type, nodeId, message, data: data ?? null });
  }

  private tx<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    let result: T;
    try {
      result = fn();
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      this.pendingEvents = [];
      throw error;
    }
    const events = this.pendingEvents;
    this.pendingEvents = [];
    for (const event of events) for (const listener of this.listeners) listener(event);
    return result;
  }
}

const nullableNumber = (value: SQLOutputValue | undefined) => (value === null || value === undefined ? null : Number(value));
const nullableString = (value: SQLOutputValue | undefined) => (value === null || value === undefined ? null : String(value));

function toJob(row: Row): JobRecord {
  return {
    id: String(row.id),
    spec: JSON.parse(String(row.spec)) as JobSpec,
    status: String(row.status) as JobStatus,
    nodeId: nullableString(row.node_id),
    attempt: Number(row.attempt),
    leaseExpiresAt: nullableNumber(row.lease_expires_at),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    startedAt: nullableNumber(row.started_at),
    finishedAt: nullableNumber(row.finished_at),
    result: row.result === null || row.result === undefined ? null : JSON.parse(String(row.result)),
    error: nullableString(row.error),
  };
}

function toEvent(row: Row): JobEvent {
  return {
    id: Number(row.id),
    jobId: String(row.job_id),
    ts: Number(row.ts),
    type: String(row.type) as JobEventType,
    nodeId: nullableString(row.node_id),
    message: nullableString(row.message),
    data: row.data === null || row.data === undefined ? null : JSON.parse(String(row.data)),
  };
}
