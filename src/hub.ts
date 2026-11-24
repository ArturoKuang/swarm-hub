// Layer 1: the hub. Owns the SQLite queue, hands out leases, streams job events, and feeds the reconciler.
import { timingSafeEqual } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { z } from "zod";
import { SwarmDb } from "./db.ts";
import { CompleteRequest, JobSpec, JobStatus, NodeHeartbeat, ProgressRequest, TERMINAL_STATUSES, type JobEvent } from "./protocol.ts";
import { createReconciler, ombUrlFromEnv, type Reconciler } from "./reconciler.ts";
import { errorMessage, intFromEnv, isMain } from "./util.ts";

export interface HubOptions {
  db: SwarmDb;
  host?: string;
  port?: number;
  /** When set, non-loopback callers must send `Authorization: Bearer <token>`. */
  token?: string;
  reconciler?: Reconciler;
  /** Interval for expired-lease recovery. */
  sweepMs?: number;
  now?: () => number;
  log?: (message: string) => void;
}

export interface Hub {
  server: Server;
  listen(): Promise<string>;
  close(): Promise<void>;
}

const MAX_BODY_BYTES = 2 * 1024 * 1024;
const SSE_KEEPALIVE_MS = 15_000;

class HttpError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function createHub(options: HubOptions): Hub {
  const { db } = options;
  const now = options.now ?? Date.now;
  const log = options.log ?? ((message: string) => console.error(`[hub] ${message}`));
  const sseClients = new Set<ServerResponse>();

  const routes: Array<[method: string, pattern: RegExp, handler: (ctx: Ctx) => Promise<void> | void]> = [
    ["GET", /^\/health$/, ({ res }) => send(res, 200, { ok: true, now: now() })],

    ["GET", /^\/api\/fleet$/, ({ res }) => send(res, 200, { nodes: db.listNodes(now()) })],

    ["POST", /^\/api\/nodes\/heartbeat$/, async ({ req, res }) => {
      const hb = NodeHeartbeat.parse(await readJson(req));
      db.upsertHeartbeat(hb, now());
      options.reconciler?.observe(db.listNodes(now()).filter((n) => n.online).map((n) => n.heartbeat));
      send(res, 200, { ok: true });
    }],

    ["POST", /^\/api\/jobs$/, async ({ req, res }) => {
      const spec = JobSpec.parse(await readJson(req));
      const created = db.createJob(spec, now());
      send(res, created.deduplicated ? 200 : 201, created);
    }],

    ["GET", /^\/api\/jobs$/, ({ res, url }) => {
      const status = url.searchParams.get("status");
      const limit = url.searchParams.get("limit");
      send(res, 200, {
        jobs: db.listJobs({
          status: status ? JobStatus.parse(status) : undefined,
          limit: limit ? intFromEnv(limit, 100) : undefined,
        }),
      });
    }],

    ["GET", /^\/api\/jobs\/([\w-]+)$/, ({ res, params }) => send(res, 200, { job: requireJob(params[0]!) })],

    ["GET", /^\/api\/jobs\/([\w-]+)\/events$/, ({ req, res, url, params }) => {
      const jobId = params[0]!;
      requireJob(jobId);
      const after = intFromEnv(url.searchParams.get("after") ?? String(req.headers["last-event-id"] ?? ""), 0);
      if (String(req.headers.accept ?? "").includes("text/event-stream")) streamEvents(req, res, jobId, after);
      else send(res, 200, { events: db.listEvents(jobId, after) });
    }],

    ["POST", /^\/api\/jobs\/([\w-]+)\/cancel$/, async ({ req, res, params }) => {
      const body = z.object({ reason: z.string().max(1000).optional() }).parse(await readJson(req, true));
      const job = db.cancelJob(params[0]!, body.reason, now());
      if (!job) throw new HttpError(404, "no such job");
      send(res, 200, { job });
    }],

    ["POST", /^\/api\/workers\/([\w-]+)\/lease$/, ({ res, params }) => {
      send(res, 200, { job: db.leaseNextJob(params[0]!, now()) });
    }],

    ["POST", /^\/api\/jobs\/([\w-]+)\/progress$/, async ({ req, res, params }) => {
      const body = ProgressRequest.parse(await readJson(req));
      const outcome = db.recordProgress(params[0]!, body.nodeId, body.message, body.data, now());
      send(res, 200, outcome.kind === "renewed"
        ? { ok: true, cancelled: false, leaseExpiresAt: outcome.leaseExpiresAt }
        : { ok: false, cancelled: true, leaseExpiresAt: null, reason: outcome.reason });
    }],

    ["POST", /^\/api\/jobs\/([\w-]+)\/complete$/, async ({ req, res, params }) => {
      const body = CompleteRequest.parse(await readJson(req));
      const outcome = db.completeJob(params[0]!, body, now());
      if (outcome.kind === "rejected") send(res, outcome.job ? 409 : 404, { error: outcome.reason, job: outcome.job });
      else send(res, 200, { job: outcome.job });
    }],
  ];

  function requireJob(id: string) {
    const job = db.getJob(id);
    if (!job) throw new HttpError(404, "no such job");
    return job;
  }

  function streamEvents(req: IncomingMessage, res: ServerResponse, jobId: string, after: number): void {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    sseClients.add(res);
    let lastId = after;
    const write = (event: JobEvent) => {
      if (res.writableEnded || event.id <= lastId) return;
      lastId = event.id;
      res.write(`id: ${event.id}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      if (TERMINAL_STATUSES.has(event.type as JobStatus)) finish();
    };
    const keepalive = setInterval(() => res.write(": keepalive\n\n"), SSE_KEEPALIVE_MS);
    const unsubscribe = db.onEvent((event) => {
      if (event.jobId === jobId) write(event);
    });
    function finish() {
      cleanup();
      res.end("event: end\ndata: {}\n\n");
    }
    function cleanup() {
      clearInterval(keepalive);
      unsubscribe();
      sseClients.delete(res);
    }
    req.on("close", cleanup);
    // Subscribe first, then replay: an event committed in between is delivered once (deduped by id).
    for (const event of db.listEvents(jobId, after)) write(event);
    if (!res.writableEnded && TERMINAL_STATUSES.has(requireJob(jobId).status)) finish();
  }

  function authorized(req: IncomingMessage): boolean {
    if (!options.token) return true;
    if (isLoopback(req.socket.remoteAddress)) return true;
    const header = req.headers.authorization ?? "";
    const given = Buffer.from(header.startsWith("Bearer ") ? header.slice(7) : "");
    const expected = Buffer.from(options.token);
    return given.length === expected.length && timingSafeEqual(given, expected);
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://hub.local");
    try {
      if (url.pathname !== "/health" && !authorized(req)) throw new HttpError(401, "missing or invalid bearer token");
      const pathMatches = routes.filter(([, pattern]) => pattern.test(url.pathname));
      if (!pathMatches.length) throw new HttpError(404, `no route: ${req.method} ${url.pathname}`);
      const route = pathMatches.find(([method]) => method === req.method);
      if (!route) throw new HttpError(405, `method ${req.method} not allowed on ${url.pathname}`);
      const params = route[1].exec(url.pathname)!.slice(1);
      await route[2]({ req, res, url, params });
    } catch (error) {
      if (res.headersSent) {
        res.destroy();
        return;
      }
      if (error instanceof HttpError) send(res, error.status, { error: error.message });
      else if (error instanceof z.ZodError) send(res, 400, { error: "invalid request", issues: error.issues });
      else {
        log(`${req.method} ${url.pathname}: ${errorMessage(error)}`);
        send(res, 500, { error: "internal error" });
      }
    }
  });

  let sweep: NodeJS.Timeout | null = null;

  return {
    server,
    listen() {
      return new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(options.port ?? 8900, options.host ?? "0.0.0.0", () => {
          server.off("error", reject);
          sweep = setInterval(() => {
            try {
              for (const id of db.recoverExpiredLeases(now())) log(`recovered expired lease on ${id}`);
            } catch (error) {
              log(`lease sweep failed: ${errorMessage(error)}`);
            }
          }, options.sweepMs ?? 5000);
          sweep.unref();
          const { address, port } = server.address() as AddressInfo;
          const host = address === "0.0.0.0" || address === "::" ? "127.0.0.1" : address;
          resolve(`http://${host.includes(":") ? `[${host}]` : host}:${port}`);
        });
      });
    },
    async close() {
      if (sweep) clearInterval(sweep);
      options.reconciler?.close();
      for (const res of sseClients) res.end();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

interface Ctx { req: IncomingMessage; res: ServerResponse; url: URL; params: string[] }

function isLoopback(address: string | undefined): boolean {
  return address === "127.0.0.1" || address === "::1" || address === "::ffff:127.0.0.1";
}

function send(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(text) });
  res.end(text);
}

async function readJson(req: IncomingMessage, allowEmpty = false): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, "request body too large");
    chunks.push(chunk);
  }
  const text = Buffer.concat(chunks).toString("utf8").trim();
  if (!text) {
    if (allowEmpty) return {};
    throw new HttpError(400, "request body required");
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new HttpError(400, "request body is not valid JSON");
  }
}

if (isMain(import.meta.url)) {
  const dbPath = process.env.SWARM_HUB_DB ?? join(homedir(), ".swarm-hub", "hub.db");
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new SwarmDb(dbPath, { leaseTtlMs: intFromEnv(process.env.SWARM_LEASE_TTL_MS, 60_000) });
  const log = (message: string) => console.error(`[hub] ${message}`);
  const ombUrl = ombUrlFromEnv();
  const hub = createHub({
    db,
    host: process.env.SWARM_HUB_HOST ?? "0.0.0.0",
    port: intFromEnv(process.env.SWARM_HUB_PORT, 8900),
    token: process.env.SWARM_HUB_TOKEN || undefined,
    reconciler: createReconciler({ ombUrl, token: process.env.OMB_TOKEN || undefined, fallbackBotName: process.env.OMB_FALLBACK_BOT, log }),
    log,
  });
  const url = await hub.listen();
  log(`listening on ${url} (db ${dbPath}, auth ${process.env.SWARM_HUB_TOKEN ? "token" : "open"}, OpenMausBot ${ombUrl ?? "disabled"})`);
  const stop = async () => {
    await hub.close();
    db.close();
    process.exit(0);
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}
