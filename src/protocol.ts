// Wire contract shared by hub, worker, and MCP server. Every network payload is parsed with these schemas
// at the boundary; everything past the parse trusts the inferred types.
import { z } from "zod";

export const AGENT_CLIS = ["claude", "codex", "agy", "hermes"] as const;
export const JOB_CLIS = ["claude", "codex", "agy", "shell"] as const;
export const JOB_STATUSES = ["queued", "running", "succeeded", "failed", "cancelled"] as const;
export const TERMINAL_STATUSES: ReadonlySet<JobStatus> = new Set(["succeeded", "failed", "cancelled"]);

export const CliInfo = z.object({
  available: z.boolean(),
  version: z.string().nullable(),
});
export type CliInfo = z.infer<typeof CliInfo>;

export const RepoInfo = z.object({
  name: z.string().min(1),
  path: z.string().min(1),
  remoteUrl: z.string().nullable(),
  branch: z.string().nullable(),
});
export type RepoInfo = z.infer<typeof RepoInfo>;

export const HermesProfile = z.object({
  name: z.string().min(1),
  soulHash: z.string().min(1),
  soul: z.string(),
});
export type HermesProfile = z.infer<typeof HermesProfile>;

export const NodeHeartbeat = z.object({
  nodeId: z.string().min(1).max(128),
  hostname: z.string(),
  tailscaleIp: z.string().nullable(),
  os: z.string().min(1),
  arch: z.string().min(1),
  tags: z.array(z.string()).default([]),
  clis: z.object({ claude: CliInfo, codex: CliInfo, agy: CliInfo, hermes: CliInfo }),
  resources: z.object({
    freeDiskGb: z.number().nonnegative(),
    freeRamGb: z.number().nonnegative(),
    cpuCount: z.number().int().positive(),
  }),
  maxSlots: z.number().int().nonnegative(),
  activeJobIds: z.array(z.string()).default([]),
  repos: z.array(RepoInfo).default([]),
  hermesProfiles: z.array(HermesProfile).default([]),
});
export type NodeHeartbeat = z.infer<typeof NodeHeartbeat>;

export const JobCli = z.enum(JOB_CLIS);
export type JobCli = z.infer<typeof JobCli>;

const branchName = z.string().min(1).max(200).regex(/^(?!-)[\w./-]+$/, "invalid git branch name");

export const JobSpec = z.object({
  repo: z.string().min(1).optional()
    .describe("Repo directory name as workers report it. Without repoUrl, only nodes that have it checked out are eligible."),
  repoUrl: z.string().min(1).optional().describe("Clone URL; lets any node run the job by cloning into ~/.swarm-worker/repos."),
  baseBranch: branchName.optional().describe("Branch the task branch starts from. Defaults to the repo's current HEAD."),
  taskBranch: branchName.optional().describe("Branch the work lands on. Defaults to swarm/<jobId>."),
  prompt: z.string().min(1).describe("Agent prompt, or the shell command when cli is 'shell'."),
  cli: JobCli.default("claude"),
  model: z.string().min(1).optional(),
  skillLoadout: z.array(z.string().min(1)).default([]),
  requiredTags: z.array(z.string().min(1)).default([]),
  minDiskGb: z.number().nonnegative().default(0),
  preferredNodeId: z.string().min(1).optional(),
  verifyCmd: z.string().min(1).optional(),
  timeoutSec: z.number().int().positive().max(86_400).default(1800),
  maxRetries: z.number().int().nonnegative().max(10).default(1)
    .describe("Re-queues after a lost lease (worker crash or offline). Task failures are reported, not retried."),
  requestedByBot: z.string().min(1).optional(),
  idempotencyKey: z.string().min(1).max(200).optional(),
});
export type JobSpec = z.infer<typeof JobSpec>;
export type JobSpecInput = z.input<typeof JobSpec>;

export const JobStatus = z.enum(JOB_STATUSES);
export type JobStatus = z.infer<typeof JobStatus>;

export const JobResult = z.object({
  exitCode: z.number().int().nullable(),
  timedOut: z.boolean(),
  outputTail: z.string(),
  verify: z.object({ cmd: z.string(), exitCode: z.number().int().nullable(), outputTail: z.string() }).nullable(),
  diffStat: z.string(),
  branch: z.string().nullable(),
  commit: z.string().nullable(),
  pushed: z.boolean(),
  pushError: z.string().nullable(),
  durationMs: z.number().nonnegative(),
});
export type JobResult = z.infer<typeof JobResult>;

export const JobRecord = z.object({
  id: z.string(),
  spec: JobSpec,
  status: JobStatus,
  nodeId: z.string().nullable(),
  attempt: z.number().int(),
  leaseExpiresAt: z.number().nullable(),
  createdAt: z.number(),
  updatedAt: z.number(),
  startedAt: z.number().nullable(),
  finishedAt: z.number().nullable(),
  result: JobResult.nullable(),
  error: z.string().nullable(),
});
export type JobRecord = z.infer<typeof JobRecord>;

export const JOB_EVENT_TYPES = [
  "created", "leased", "progress", "succeeded", "failed", "cancelled", "lease_expired", "requeued",
] as const;
export const JobEventType = z.enum(JOB_EVENT_TYPES);
export type JobEventType = z.infer<typeof JobEventType>;

export const JobEvent = z.object({
  id: z.number().int(),
  jobId: z.string(),
  ts: z.number(),
  type: JobEventType,
  nodeId: z.string().nullable(),
  message: z.string().nullable(),
  data: z.unknown().nullable(),
});
export type JobEvent = z.infer<typeof JobEvent>;

export const FleetNode = z.object({
  nodeId: z.string(),
  online: z.boolean(),
  lastSeen: z.number(),
  freeSlots: z.number().int(),
  runningJobIds: z.array(z.string()),
  heartbeat: NodeHeartbeat,
});
export type FleetNode = z.infer<typeof FleetNode>;

// ---- request / response bodies ----

export const LeaseResponse = z.object({ job: JobRecord.nullable() });
export type LeaseResponse = z.infer<typeof LeaseResponse>;

export const ProgressRequest = z.object({
  nodeId: z.string().min(1),
  message: z.string().max(16_000).optional(),
  data: z.unknown().optional(),
});
export type ProgressRequest = z.infer<typeof ProgressRequest>;
export const ProgressResponse = z.object({ ok: z.boolean(), cancelled: z.boolean(), leaseExpiresAt: z.number().nullable() });
export type ProgressResponse = z.infer<typeof ProgressResponse>;

export const CompleteRequest = z.object({
  nodeId: z.string().min(1),
  status: z.enum(["succeeded", "failed"]),
  result: JobResult,
  error: z.string().optional(),
});
export type CompleteRequest = z.infer<typeof CompleteRequest>;

export const CreateJobResponse = z.object({ job: JobRecord, deduplicated: z.boolean() });
export type CreateJobResponse = z.infer<typeof CreateJobResponse>;

export const FleetResponse = z.object({ nodes: z.array(FleetNode) });
export type FleetResponse = z.infer<typeof FleetResponse>;
