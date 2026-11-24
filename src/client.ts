// Typed HTTP client for the hub API, used by the worker and the MCP server. Responses are parsed, not trusted.
import { z } from "zod";
import {
  CreateJobResponse, FleetResponse, JobEvent, JobRecord, LeaseResponse, ProgressResponse,
  type CompleteRequest, type JobSpecInput, type JobStatus, type NodeHeartbeat, type ProgressRequest,
} from "./protocol.ts";

export class HubError extends Error {
  readonly status: number;
  readonly body: unknown;
  constructor(status: number, message: string, body: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export interface HubClient {
  fleet(): Promise<FleetResponse>;
  heartbeat(hb: NodeHeartbeat): Promise<void>;
  createJob(spec: JobSpecInput): Promise<CreateJobResponse>;
  listJobs(filter?: { status?: JobStatus; limit?: number }): Promise<JobRecord[]>;
  getJob(id: string): Promise<JobRecord>;
  events(id: string, after?: number): Promise<JobEvent[]>;
  cancel(id: string, reason?: string): Promise<JobRecord>;
  lease(nodeId: string): Promise<JobRecord | null>;
  progress(id: string, body: ProgressRequest): Promise<ProgressResponse>;
  complete(id: string, body: CompleteRequest): Promise<{ ok: true } | { ok: false; status: number; error: string }>;
}

export function createHubClient(baseUrl: string, token?: string, timeoutMs = 15_000): HubClient {
  const base = baseUrl.replace(/\/+$/, "");

  async function call<T extends z.ZodType>(schema: T, method: string, path: string, body?: unknown): Promise<z.infer<T>> {
    const headers: Record<string, string> = { accept: "application/json" };
    if (body !== undefined) headers["content-type"] = "application/json";
    if (token) headers.authorization = `Bearer ${token}`;
    const res = await fetch(`${base}${path}`, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let json: unknown = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      // non-JSON error bodies fall through to the status check below
    }
    if (!res.ok) {
      const message = (json as { error?: unknown } | null)?.error;
      throw new HubError(res.status, `${method} ${path} → ${res.status}${typeof message === "string" ? `: ${message}` : ""}`, json);
    }
    return schema.parse(json);
  }

  const jobBody = z.object({ job: JobRecord });

  return {
    fleet: () => call(FleetResponse, "GET", "/api/fleet"),
    heartbeat: async (hb) => {
      await call(z.unknown(), "POST", "/api/nodes/heartbeat", hb);
    },
    createJob: (spec) => call(CreateJobResponse, "POST", "/api/jobs", spec),
    listJobs: async (filter = {}) => {
      const query = new URLSearchParams();
      if (filter.status) query.set("status", filter.status);
      if (filter.limit) query.set("limit", String(filter.limit));
      const qs = query.size ? `?${query}` : "";
      return (await call(z.object({ jobs: z.array(JobRecord) }), "GET", `/api/jobs${qs}`)).jobs;
    },
    getJob: async (id) => (await call(jobBody, "GET", `/api/jobs/${encodeURIComponent(id)}`)).job,
    events: async (id, after = 0) =>
      (await call(z.object({ events: z.array(JobEvent) }), "GET", `/api/jobs/${encodeURIComponent(id)}/events?after=${after}`)).events,
    cancel: async (id, reason) => (await call(jobBody, "POST", `/api/jobs/${encodeURIComponent(id)}/cancel`, reason ? { reason } : {})).job,
    lease: async (nodeId) => (await call(LeaseResponse, "POST", `/api/workers/${encodeURIComponent(nodeId)}/lease`, {})).job,
    progress: (id, body) => call(ProgressResponse, "POST", `/api/jobs/${encodeURIComponent(id)}/progress`, body),
    complete: async (id, body) => {
      try {
        await call(z.unknown(), "POST", `/api/jobs/${encodeURIComponent(id)}/complete`, body);
        return { ok: true };
      } catch (error) {
        if (error instanceof HubError && (error.status === 409 || error.status === 404)) {
          return { ok: false, status: error.status, error: error.message };
        }
        throw error;
      }
    },
  };
}
