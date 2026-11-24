// Layer 3: MCP server over stdio (newline-delimited JSON-RPC 2.0). Lets any MCP client dispatch work to the swarm.
// stdout carries protocol messages only; logs go to stderr.
import { createInterface } from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import { createHubClient, type HubClient } from "./client.ts";
import { JobSpec, JobStatus, TERMINAL_STATUSES, type JobRecord } from "./protocol.ts";
import { errorMessage, isMain } from "./util.ts";

export const SUPPORTED_PROTOCOL_VERSIONS = ["2025-03-26", "2024-11-05"] as const;
const SERVER_INFO = { name: "swarm-hub", version: "0.1.0" };

const JsonRpcRequest = z.object({
  jsonrpc: z.literal("2.0"),
  id: z.union([z.string(), z.number()]).optional(),
  method: z.string(),
  params: z.unknown().optional(),
});

type JsonRpcId = string | number | null;
type JsonRpcResponse =
  | { jsonrpc: "2.0"; id: JsonRpcId; result: unknown }
  | { jsonrpc: "2.0"; id: JsonRpcId; error: { code: number; message: string; data?: unknown } };

const ERR = { parse: -32700, invalidRequest: -32600, methodNotFound: -32601, invalidParams: -32602, internal: -32603 } as const;

class RpcError extends Error {
  readonly code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

const JobIdArgs = z.object({ jobId: z.string().min(1) });
const WaitArgs = z.object({
  jobIds: z.array(z.string().min(1)).min(1).max(100),
  timeoutSec: z.number().positive().max(3600).default(300),
  pollMs: z.number().int().min(50).max(30_000).default(1000),
});
const ListArgs = z.object({ status: JobStatus.optional(), limit: z.number().int().min(1).max(500).default(50) });
const CancelArgs = JobIdArgs.extend({ reason: z.string().max(1000).optional() });
const NoArgs = z.object({});

interface Tool<T extends z.ZodType> {
  description: string;
  args: T;
  run(hub: HubClient, args: z.infer<T>): Promise<unknown>;
}

const tool = <T extends z.ZodType>(t: Tool<T>) => t;

const summarize = (job: JobRecord) => ({
  id: job.id, status: job.status, nodeId: job.nodeId, attempt: job.attempt, cli: job.spec.cli, repo: job.spec.repo ?? null,
  branch: job.spec.taskBranch ?? null, error: job.error,
});

export const TOOLS = {
  list_fleet: tool({
    description: "List swarm worker nodes with online state, free slots, tags, CLIs, resources, repos, and Hermes profiles.",
    args: NoArgs,
    async run(hub) {
      const { nodes } = await hub.fleet();
      return nodes.map(({ heartbeat: hb, ...node }) => ({
        ...node, hostname: hb.hostname, tailscaleIp: hb.tailscaleIp, os: hb.os, arch: hb.arch, tags: hb.tags,
        clis: hb.clis, resources: hb.resources, maxSlots: hb.maxSlots, repos: hb.repos.map((r) => r.name),
        hermesProfiles: hb.hermesProfiles.map((p) => p.name),
      }));
    },
  }),
  dispatch_task: tool({
    description: "Queue a job on the swarm. The hub routes it to an online node that satisfies cli, requiredTags, repo, and minDiskGb. "
      + "For cli 'shell' the prompt is the shell command. Returns the job id; pass an idempotencyKey to make retries safe.",
    args: JobSpec,
    async run(hub, spec) {
      const { job, deduplicated } = await hub.createJob(spec);
      return { ...summarize(job), deduplicated };
    },
  }),
  wait_jobs: tool({
    description: "Wait until every listed job is finished (succeeded, failed, or cancelled) or the timeout passes. Returns each job's status.",
    args: WaitArgs,
    async run(hub, { jobIds, timeoutSec, pollMs }) {
      const deadline = Date.now() + timeoutSec * 1000;
      for (;;) {
        const jobs = await Promise.all(jobIds.map((id) => hub.getJob(id)));
        const done = jobs.every((job) => TERMINAL_STATUSES.has(job.status));
        if (done || Date.now() >= deadline) return { done, timedOut: !done, jobs: jobs.map(summarize) };
        await sleep(Math.min(pollMs, Math.max(0, deadline - Date.now())));
      }
    },
  }),
  get_job_result: tool({
    description: "Get a job's full record: spec, status, result (exit code, output tail, verify result, diff stat, branch, push state), and events.",
    args: JobIdArgs,
    async run(hub, { jobId }) {
      const [job, events] = await Promise.all([hub.getJob(jobId), hub.events(jobId)]);
      return { job, events };
    },
  }),
  list_jobs: tool({
    description: "List recent jobs, newest first, optionally filtered by status.",
    args: ListArgs,
    async run(hub, filter) {
      return (await hub.listJobs(filter)).map(summarize);
    },
  }),
  cancel_job: tool({
    description: "Cancel a queued or running job. A running job's worker stops at its next lease renewal.",
    args: CancelArgs,
    async run(hub, { jobId, reason }) {
      return summarize(await hub.cancel(jobId, reason));
    },
  }),
} satisfies Record<string, Tool<z.ZodType>>;

type ToolName = keyof typeof TOOLS;

export function toolList() {
  return Object.entries(TOOLS).map(([name, t]) => {
    const { $schema: _ignored, ...inputSchema } = z.toJSONSchema(t.args, { io: "input" }) as Record<string, unknown>;
    return { name, description: t.description, inputSchema };
  });
}

/** Handles one parsed message. Returns null for notifications (no response is sent). */
export async function handleMessage(hub: HubClient, message: unknown): Promise<JsonRpcResponse | null> {
  const id: JsonRpcId = isRecord(message) && (typeof message.id === "string" || typeof message.id === "number") ? message.id : null;
  const parsed = JsonRpcRequest.safeParse(message);
  if (!parsed.success) return { jsonrpc: "2.0", id, error: { code: ERR.invalidRequest, message: "invalid JSON-RPC request" } };
  const req = parsed.data;
  if (req.id === undefined) return null; // notification: notifications/initialized, notifications/cancelled, ...
  try {
    return { jsonrpc: "2.0", id: req.id, result: await dispatch(hub, req.method, req.params) };
  } catch (error) {
    const code = error instanceof RpcError ? error.code : ERR.internal;
    return { jsonrpc: "2.0", id: req.id, error: { code, message: errorMessage(error) } };
  }
}

async function dispatch(hub: HubClient, method: string, params: unknown): Promise<unknown> {
  switch (method) {
    case "initialize": {
      const requested = isRecord(params) ? params.protocolVersion : undefined;
      const protocolVersion = SUPPORTED_PROTOCOL_VERSIONS.find((v) => v === requested) ?? SUPPORTED_PROTOCOL_VERSIONS[0];
      return {
        protocolVersion,
        capabilities: { tools: { listChanged: false } },
        serverInfo: SERVER_INFO,
        instructions: "Dispatch coding or shell jobs to the swarm with dispatch_task, then wait_jobs and get_job_result.",
      };
    }
    case "ping":
      return {};
    case "tools/list":
      return { tools: toolList() };
    case "tools/call": {
      const call = z.object({ name: z.string(), arguments: z.record(z.string(), z.unknown()).optional() }).safeParse(params);
      if (!call.success) throw new RpcError(ERR.invalidParams, "tools/call needs {name, arguments}");
      const t = Object.hasOwn(TOOLS, call.data.name) ? TOOLS[call.data.name as ToolName] : undefined;
      if (!t) throw new RpcError(ERR.invalidParams, `unknown tool: ${call.data.name}`);
      const args = t.args.safeParse(call.data.arguments ?? {});
      // Tool-level failures are results with isError, so the model can see and react to them.
      if (!args.success) return toolError(`invalid arguments: ${z.prettifyError(args.error)}`);
      try {
        const value = await (t.run as (hub: HubClient, args: unknown) => Promise<unknown>)(hub, args.data);
        return { content: [{ type: "text", text: JSON.stringify(value, null, 2) }], isError: false };
      } catch (error) {
        return toolError(errorMessage(error));
      }
    }
    default:
      throw new RpcError(ERR.methodNotFound, `method not found: ${method}`);
  }
}

const toolError = (text: string) => ({ content: [{ type: "text", text }], isError: true });

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Serves MCP on the given streams until input closes. Requests are handled concurrently; each reply is one line. */
export function serveStdio(hub: HubClient, input: NodeJS.ReadableStream = process.stdin, output: NodeJS.WritableStream = process.stdout): Promise<void> {
  const write = (response: JsonRpcResponse) => output.write(`${JSON.stringify(response)}\n`);
  const pending = new Set<Promise<void>>();
  const rl = createInterface({ input, crlfDelay: Infinity });
  rl.on("line", (line) => {
    if (!line.trim()) return;
    let message: unknown;
    try {
      message = JSON.parse(line);
    } catch {
      write({ jsonrpc: "2.0", id: null, error: { code: ERR.parse, message: "parse error" } });
      return;
    }
    if (Array.isArray(message)) {
      // JSON-RPC batches (2025-03-26): answer with an array of the non-notification responses.
      const task = Promise.all(message.map((m) => handleMessage(hub, m))).then((responses) => {
        const replies = responses.filter((r): r is JsonRpcResponse => r !== null);
        if (replies.length) output.write(`${JSON.stringify(replies)}\n`);
      });
      track(task);
      return;
    }
    track(handleMessage(hub, message).then((response) => { if (response) write(response); }));
  });
  function track(task: Promise<void>) {
    const guarded = task.catch((error) => console.error(`[mcp] ${errorMessage(error)}`)).finally(() => pending.delete(guarded));
    pending.add(guarded);
  }
  return new Promise((resolve) => rl.on("close", () => void Promise.allSettled(pending).then(() => resolve())));
}

if (isMain(import.meta.url)) {
  const hubUrl = process.env.SWARM_HUB_URL ?? "http://127.0.0.1:8900";
  await serveStdio(createHubClient(hubUrl, process.env.SWARM_HUB_TOKEN || undefined));
}
