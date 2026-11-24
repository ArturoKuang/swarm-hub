// Layer 2: the stateless worker daemon. Advertises what this machine can do, leases jobs that fit, runs each in
// an isolated git worktree, and reports the result. All durable state lives in the hub; a restart loses nothing
// but in-flight jobs, which the hub re-queues when their leases expire.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, watch, type FSWatcher } from "node:fs";
import { mkdir, readdir, readFile, rm, stat, statfs } from "node:fs/promises";
import { arch, availableParallelism, freemem, homedir, hostname, networkInterfaces, platform } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { createHubClient, type HubClient } from "./client.ts";
import {
  AGENT_CLIS,
  type CliInfo, type CompleteRequest, type HermesProfile, type JobRecord, type JobResult, type JobSpec, type NodeHeartbeat,
  type RepoInfo,
} from "./protocol.ts";
import { errorMessage, intFromEnv, isMain } from "./util.ts";

export interface WorkerConfig {
  hubUrl: string;
  hubToken?: string;
  nodeId: string;
  projectsDir: string;
  /** Worktrees go in <workDir>/workspaces, clones of repoUrl jobs in <workDir>/repos. */
  workDir: string;
  hermesProfilesDir: string;
  maxSlots: number;
  extraTags: string[];
  pollMs: number;
  heartbeatMs: number;
  /** How often a running job renews its lease (must be well under the hub's lease TTL). */
  renewMs: number;
  watch: boolean;
  log: (message: string) => void;
}

export function workerConfigFromEnv(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const home = homedir();
  return {
    hubUrl: env.SWARM_HUB_URL ?? "http://127.0.0.1:8900",
    hubToken: env.SWARM_HUB_TOKEN || undefined,
    nodeId: env.SWARM_NODE_ID || hostname().replace(/\.local$/, "").toLowerCase().replace(/[^\w-]/g, "-"),
    projectsDir: env.SWARM_PROJECTS_DIR || join(home, "Projects"),
    workDir: env.SWARM_WORK_DIR || join(home, ".swarm-worker"),
    hermesProfilesDir: env.SWARM_HERMES_PROFILES_DIR || join(home, ".hermes", "profiles"),
    maxSlots: intFromEnv(env.SWARM_MAX_SLOTS, 2),
    extraTags: (env.SWARM_EXTRA_TAGS ?? "").split(",").map((t) => t.trim()).filter(Boolean),
    pollMs: intFromEnv(env.SWARM_POLL_MS, 2000),
    heartbeatMs: intFromEnv(env.SWARM_HEARTBEAT_MS, 15_000),
    renewMs: intFromEnv(env.SWARM_RENEW_MS, 10_000),
    watch: env.SWARM_WATCH !== "0",
    log: (message) => console.error(`[worker] ${message}`),
  };
}

// ---------------------------------------------------------------------------------------------------------------
// Process execution
// ---------------------------------------------------------------------------------------------------------------

const OUTPUT_TAIL_BYTES = 64 * 1024;

export interface RunResult { code: number | null; output: string; timedOut: boolean; aborted: boolean }

export interface RunOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  timeoutMs?: number;
  signal?: AbortSignal;
  onOutput?: (chunk: string) => void;
}

/** Spawns in its own process group so timeout/cancel kill the whole tree. Output is a bounded tail of stdout+stderr. */
export function run(command: string, args: readonly string[], options: RunOptions = {}): Promise<RunResult> {
  return new Promise((resolvePromise) => {
    let output = "";
    let timedOut = false;
    let aborted = false;
    const child = spawn(command, args, {
      cwd: options.cwd, env: options.env ?? process.env, detached: true, stdio: ["ignore", "pipe", "pipe"],
    });
    const append = (buf: Buffer) => {
      const text = buf.toString("utf8");
      output = (output + text).slice(-OUTPUT_TAIL_BYTES);
      options.onOutput?.(text);
    };
    child.stdout.on("data", append);
    child.stderr.on("data", append);
    const killTree = () => {
      if (child.pid === undefined || child.exitCode !== null) return;
      try {
        process.kill(-child.pid, "SIGTERM");
      } catch {
        child.kill("SIGTERM");
      }
      setTimeout(() => {
        if (child.exitCode !== null || child.pid === undefined) return;
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          child.kill("SIGKILL");
        }
      }, 5000).unref();
    };
    const timer = options.timeoutMs ? setTimeout(() => { timedOut = true; killTree(); }, options.timeoutMs) : null;
    const onAbort = () => { aborted = true; killTree(); };
    if (options.signal?.aborted) onAbort();
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const done = (code: number | null) => {
      if (timer) clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      resolvePromise({ code, output, timedOut, aborted });
    };
    child.on("error", (error) => { output += errorMessage(error); done(null); });
    child.on("close", (code) => done(code));
  });
}

async function git(cwd: string, args: readonly string[], timeoutMs = 120_000): Promise<string> {
  const result = await run("git", args, { cwd, timeoutMs, env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } });
  if (result.code !== 0) throw new Error(`git ${args.join(" ")} failed (${result.code}): ${result.output.trim().slice(-2000)}`);
  return result.output.trim();
}

async function gitOk(cwd: string, args: readonly string[]): Promise<boolean> {
  return (await run("git", args, { cwd, timeoutMs: 30_000 })).code === 0;
}

// ---------------------------------------------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------------------------------------------

const EXTRA_PATH_DIRS = [join(homedir(), ".local", "bin"), join(homedir(), ".cargo", "bin"), "/opt/homebrew/bin", "/usr/local/bin"];

/** LaunchAgents start with a minimal PATH; make the usual install locations visible. */
export function augmentPath(env: NodeJS.ProcessEnv = process.env): void {
  const parts = (env.PATH ?? "").split(delimiter).filter(Boolean);
  for (const dir of EXTRA_PATH_DIRS) if (!parts.includes(dir)) parts.push(dir);
  env.PATH = parts.join(delimiter);
}

function onPath(command: string): string | null {
  for (const dir of (process.env.PATH ?? "").split(delimiter)) {
    if (dir && existsSync(join(dir, command))) return join(dir, command);
  }
  return null;
}

async function detectCli(command: string): Promise<CliInfo> {
  if (!onPath(command)) return { available: false, version: null };
  const result = await run(command, ["--version"], { timeoutMs: 8000 });
  const version = result.output.split("\n").map((line) => line.trim()).find(Boolean) ?? null;
  return { available: result.code === 0, version: result.code === 0 ? version : null };
}

async function detectSdkTags(): Promise<string[]> {
  const tags = ["sdk:node"];
  if (platform() === "darwin" && (await run("xcrun", ["--find", "xcodebuild"], { timeoutMs: 8000 })).code === 0) tags.push("sdk:xcode");
  const androidHomes = [process.env.ANDROID_HOME, process.env.ANDROID_SDK_ROOT, join(homedir(), "Library", "Android", "sdk"), join(homedir(), "Android", "Sdk")];
  if (androidHomes.some((dir) => dir && existsSync(join(dir, "platform-tools")))) tags.push("sdk:android");
  if (onPath("cargo")) tags.push("sdk:rust");
  if (onPath("docker")) tags.push("sdk:docker");
  return tags;
}

/** CGNAT range 100.64.0.0/10 is what Tailscale hands out. */
function isTailscaleIp(ip: string): boolean {
  const [a, b] = ip.split(".").map(Number);
  return a === 100 && b !== undefined && b >= 64 && b <= 127;
}

async function detectTailscaleIp(): Promise<string | null> {
  for (const addrs of Object.values(networkInterfaces())) {
    for (const addr of addrs ?? []) if (addr.family === "IPv4" && isTailscaleIp(addr.address)) return addr.address;
  }
  for (const cmd of ["tailscale", "/Applications/Tailscale.app/Contents/MacOS/Tailscale"]) {
    if (!cmd.startsWith("/") && !onPath(cmd)) continue;
    if (cmd.startsWith("/") && !existsSync(cmd)) continue;
    const result = await run(cmd, ["ip", "-4"], { timeoutMs: 5000 });
    const ip = result.output.split("\n").map((l) => l.trim()).find((l) => /^\d+\.\d+\.\d+\.\d+$/.test(l));
    if (ip) return ip;
  }
  return null;
}

/** Reads branch and origin URL straight from .git files: no process per repo, safe to run every heartbeat. */
async function readRepo(path: string, name: string): Promise<RepoInfo | null> {
  const dotGit = join(path, ".git");
  let gitDir: string;
  try {
    const info = await stat(dotGit);
    if (info.isDirectory()) gitDir = dotGit;
    else {
      const pointer = (await readFile(dotGit, "utf8")).match(/^gitdir:\s*(.+)$/m)?.[1]?.trim();
      if (!pointer) return null;
      gitDir = resolve(path, pointer);
    }
  } catch {
    return null;
  }
  const commonDir = await readFile(join(gitDir, "commondir"), "utf8").then((d) => resolve(gitDir, d.trim()), () => gitDir);
  const head = await readFile(join(gitDir, "HEAD"), "utf8").catch(() => "");
  const branch = head.match(/^ref: refs\/heads\/(.+)$/m)?.[1]?.trim() ?? null;
  const config = await readFile(join(commonDir, "config"), "utf8").catch(() => "");
  const origin = config.split(/^\[/m).find((section) => section.startsWith('remote "origin"]'));
  const remoteUrl = origin?.match(/^\s*url\s*=\s*(.+)$/m)?.[1]?.trim() ?? null;
  return { name, path, remoteUrl, branch };
}

export async function scanRepos(projectsDir: string): Promise<RepoInfo[]> {
  const entries = await readdir(projectsDir, { withFileTypes: true }).catch(() => []);
  const repos = await Promise.all(entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("."))
    .map((entry) => readRepo(join(projectsDir, entry.name), entry.name)));
  return repos.filter((repo): repo is RepoInfo => repo !== null).sort((a, b) => a.name.localeCompare(b.name));
}

const MAX_SOUL_BYTES = 64 * 1024;

export async function scanHermesProfiles(profilesDir: string): Promise<HermesProfile[]> {
  const entries = await readdir(profilesDir, { withFileTypes: true }).catch(() => []);
  const profiles = await Promise.all(entries.filter((entry) => entry.isDirectory()).map(async (entry) => {
    const soul = await readFile(join(profilesDir, entry.name, "SOUL.md"), "utf8").catch(() => null);
    if (soul === null) return null;
    const trimmed = soul.slice(0, MAX_SOUL_BYTES);
    return { name: entry.name, soul: trimmed, soulHash: createHash("sha256").update(trimmed).digest("hex").slice(0, 16) };
  }));
  return profiles.filter((p): p is HermesProfile => p !== null).sort((a, b) => a.name.localeCompare(b.name));
}

interface SlowFacts { clis: NodeHeartbeat["clis"]; sdkTags: string[]; tailscaleIp: string | null; at: number }
const SLOW_FACTS_TTL_MS = 10 * 60_000;

/** Builds a heartbeat. CLI versions, SDKs, and the Tailscale IP are cached; disk, RAM, repos, and profiles are live. */
export function createDetector(config: WorkerConfig) {
  let slow: SlowFacts | null = null;
  async function slowFacts(): Promise<SlowFacts> {
    if (slow && Date.now() - slow.at < SLOW_FACTS_TTL_MS) return slow;
    const [claude, codex, agy, hermes] = await Promise.all(AGENT_CLIS.map(detectCli));
    const [sdkTags, tailscaleIp] = await Promise.all([detectSdkTags(), detectTailscaleIp()]);
    slow = { clis: { claude: claude!, codex: codex!, agy: agy!, hermes: hermes! }, sdkTags, tailscaleIp, at: Date.now() };
    return slow;
  }
  return async (activeJobIds: string[]): Promise<NodeHeartbeat> => {
    const facts = await slowFacts();
    const disk = await statfs(existsSync(config.projectsDir) ? config.projectsDir : homedir());
    const gib = 1024 ** 3;
    const [repos, hermesProfiles] = await Promise.all([scanRepos(config.projectsDir), scanHermesProfiles(config.hermesProfilesDir)]);
    return {
      nodeId: config.nodeId,
      hostname: hostname(),
      tailscaleIp: facts.tailscaleIp,
      os: platform(),
      arch: arch(),
      tags: [...new Set([...facts.sdkTags, ...config.extraTags])],
      clis: facts.clis,
      resources: {
        freeDiskGb: round2((disk.bavail * disk.bsize) / gib),
        freeRamGb: round2(freemem() / gib),
        cpuCount: availableParallelism(),
      },
      maxSlots: config.maxSlots,
      activeJobIds,
      repos,
      hermesProfiles,
    };
  };
}

const round2 = (n: number) => Math.round(n * 100) / 100;

// ---------------------------------------------------------------------------------------------------------------
// Job execution
// ---------------------------------------------------------------------------------------------------------------

/** Prompt handed to an agent CLI: skill loadout first, then the task, then the ground rules of the worktree. */
export function composePrompt(spec: JobSpec): string {
  if (spec.cli === "shell") return spec.prompt;
  const parts: string[] = [];
  if (spec.skillLoadout.length) {
    parts.push(`## Skill loadout\nBefore starting, load and follow these skills: ${spec.skillLoadout.join(", ")}.`);
  }
  parts.push(`## Task\n${spec.prompt}`);
  parts.push([
    "## Ground rules",
    `- You are in an isolated git worktree on branch \`${spec.taskBranch}\`. Work only inside it.`,
    "- Leave your changes in the working tree or commit them; the swarm worker commits, verifies, and pushes.",
    spec.verifyCmd ? `- The worker will run \`${spec.verifyCmd}\` afterwards; make it pass.` : null,
  ].filter(Boolean).join("\n"));
  return parts.join("\n\n");
}

export function buildCommand(spec: JobSpec, prompt: string): { command: string; args: string[] } {
  const model = spec.model ? ["--model", spec.model] : [];
  switch (spec.cli) {
    case "shell": return { command: "/bin/sh", args: ["-c", prompt] };
    case "claude": return { command: "claude", args: ["-p", prompt, "--dangerously-skip-permissions", ...model] };
    case "agy": return { command: "agy", args: ["-p", prompt, "--dangerously-skip-permissions", ...model] };
    case "codex": return { command: "codex", args: ["exec", "--approve-for-me", "--skip-git-repo-check", ...model, "--", prompt] };
  }
}

interface Workspace { dir: string; repoDir: string | null; baseSha: string | null }

async function prepareWorkspace(job: JobRecord, config: WorkerConfig): Promise<Workspace> {
  const { spec } = job;
  const dir = join(config.workDir, "workspaces", job.id);
  await rm(dir, { recursive: true, force: true });
  await mkdir(join(config.workDir, "workspaces"), { recursive: true });

  const local = spec.repo ? (await scanRepos(config.projectsDir)).find((repo) => repo.name === spec.repo) : undefined;
  let repoDir: string;
  let baseRef: string;
  if (local) {
    repoDir = local.path;
    if (spec.baseBranch && await gitOk(repoDir, ["rev-parse", "--verify", "--quiet", `refs/heads/${spec.baseBranch}`])) baseRef = spec.baseBranch;
    else if (spec.baseBranch) {
      await git(repoDir, ["fetch", "origin", spec.baseBranch]);
      baseRef = `origin/${spec.baseBranch}`;
    } else baseRef = "HEAD";
  } else if (spec.repoUrl) {
    const name = (spec.repo ?? spec.repoUrl.split("/").pop()!.replace(/\.git$/, "")).replace(/[^\w.-]/g, "_");
    repoDir = join(config.workDir, "repos", name);
    if (existsSync(join(repoDir, ".git"))) await git(repoDir, ["fetch", "origin", "--prune"]);
    else {
      await mkdir(join(config.workDir, "repos"), { recursive: true });
      await git(config.workDir, ["clone", spec.repoUrl, repoDir], 600_000);
    }
    baseRef = spec.baseBranch ? `origin/${spec.baseBranch}`
      : await gitOk(repoDir, ["rev-parse", "--verify", "--quiet", "origin/HEAD"]) ? "origin/HEAD" : "HEAD";
  } else if (spec.repo) {
    throw new Error(`repo ${spec.repo} is not checked out under ${config.projectsDir} and the job has no repoUrl`);
  } else {
    await mkdir(dir, { recursive: true });
    return { dir, repoDir: null, baseSha: null };
  }

  await git(repoDir, ["worktree", "prune"]);
  await git(repoDir, ["worktree", "add", "-B", spec.taskBranch!, dir, baseRef]);
  return { dir, repoDir, baseSha: await git(dir, ["rev-parse", "HEAD"]) };
}

async function commitAndPush(ws: Workspace, job: JobRecord, push: boolean) {
  const empty = { diffStat: "", commit: null, pushed: false, pushError: null };
  if (!ws.repoDir || !ws.baseSha) return empty;
  await git(ws.dir, ["add", "-A"]);
  if (await git(ws.dir, ["status", "--porcelain"])) {
    const identity = await gitOk(ws.dir, ["config", "user.email"]) ? [] : ["-c", "user.name=swarm-worker", "-c", `user.email=swarm-worker@${hostname()}`];
    const subject = job.spec.prompt.split("\n")[0]!.slice(0, 72);
    await git(ws.dir, [...identity, "commit", "--no-verify", "-m", `swarm(${job.id}): ${subject}`]);
  }
  const head = await git(ws.dir, ["rev-parse", "HEAD"]);
  if (head === ws.baseSha) return empty;
  const diffStat = await git(ws.dir, ["diff", "--stat", ws.baseSha, head]);
  if (!push || !await gitOk(ws.dir, ["remote", "get-url", "origin"])) return { diffStat, commit: head, pushed: false, pushError: null };
  const pushed = await run("git", ["push", "-u", "origin", job.spec.taskBranch!], {
    cwd: ws.dir, timeoutMs: 180_000, env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
  });
  return { diffStat, commit: head, pushed: pushed.code === 0, pushError: pushed.code === 0 ? null : pushed.output.trim().slice(-1000) };
}

async function removeWorkspace(ws: Workspace | null, job: JobRecord, config: WorkerConfig): Promise<void> {
  const dir = ws?.dir ?? join(config.workDir, "workspaces", job.id);
  if (ws?.repoDir) await run("git", ["worktree", "remove", "--force", dir], { cwd: ws.repoDir, timeoutMs: 60_000 });
  await rm(dir, { recursive: true, force: true });
  if (ws?.repoDir) await run("git", ["worktree", "prune"], { cwd: ws.repoDir, timeoutMs: 60_000 });
}

const PROGRESS_TAIL_CHARS = 2000;

/**
 * Runs one leased job end to end and returns the completion report. Renews the lease while it runs and aborts
 * when the hub says the job was cancelled or reassigned (returns null then: there is nothing left to report).
 */
export async function executeJob(job: JobRecord, config: WorkerConfig, hub: HubClient, stopSignal?: AbortSignal): Promise<CompleteRequest | null> {
  const started = Date.now();
  const { spec } = job;
  const abort = new AbortController();
  const onStop = () => abort.abort();
  stopSignal?.addEventListener("abort", onStop, { once: true });
  let recentOutput = "";
  let lostLease = false;

  const renew = async (message?: string) => {
    try {
      const res = await hub.progress(job.id, { nodeId: config.nodeId, message });
      if (res.cancelled) {
        lostLease = true;
        abort.abort();
      }
    } catch (error) {
      config.log(`${job.id}: lease renewal failed: ${errorMessage(error)}`);
    }
  };
  const renewTimer = setInterval(() => {
    const message = recentOutput ? recentOutput.slice(-PROGRESS_TAIL_CHARS) : undefined;
    recentOutput = "";
    void renew(message);
  }, config.renewMs);

  let ws: Workspace | null = null;
  try {
    await renew(`preparing workspace on ${config.nodeId}`);
    ws = await prepareWorkspace(job, config);
    const prompt = composePrompt(spec);
    const { command, args } = buildCommand(spec, prompt);
    const env = {
      ...process.env,
      SWARM_JOB_ID: job.id,
      SWARM_TASK_BRANCH: spec.taskBranch ?? "",
      SWARM_WORKSPACE: ws.dir,
      SWARM_SKILLS: spec.skillLoadout.join(","),
      GIT_TERMINAL_PROMPT: "0",
    };
    const deadline = started + spec.timeoutSec * 1000;
    const main = await run(command, args, {
      cwd: ws.dir, env, timeoutMs: spec.timeoutSec * 1000, signal: abort.signal,
      onOutput: (chunk) => { recentOutput = (recentOutput + chunk).slice(-PROGRESS_TAIL_CHARS); },
    });
    if (main.aborted || abort.signal.aborted) {
      config.log(`${job.id}: stopped (${lostLease ? "cancelled or reassigned by hub" : "worker shutting down"})`);
      return null;
    }

    let verify: JobResult["verify"] = null;
    if (spec.verifyCmd && main.code === 0 && !main.timedOut) {
      const v = await run("/bin/sh", ["-c", spec.verifyCmd], {
        cwd: ws.dir, env, timeoutMs: Math.max(60_000, deadline - Date.now()), signal: abort.signal,
      });
      if (v.aborted) return null;
      verify = { cmd: spec.verifyCmd, exitCode: v.timedOut ? null : v.code, outputTail: v.output.slice(-8000) };
    }
    const passed = main.code === 0 && !main.timedOut && (verify === null || verify.exitCode === 0);
    const vcs = await commitAndPush(ws, job, passed);
    const result: JobResult = {
      exitCode: main.code,
      timedOut: main.timedOut,
      outputTail: main.output.slice(-16_000),
      verify,
      diffStat: vcs.diffStat,
      branch: ws.repoDir ? spec.taskBranch ?? null : null,
      commit: vcs.commit,
      pushed: vcs.pushed,
      pushError: vcs.pushError,
      durationMs: Date.now() - started,
    };
    const error = passed ? undefined
      : main.timedOut ? `timed out after ${spec.timeoutSec}s`
      : main.code !== 0 ? `${spec.cli} exited with ${main.code}`
      : `verify command exited with ${verify?.exitCode}`;
    return { nodeId: config.nodeId, status: passed ? "succeeded" : "failed", result, error };
  } catch (error) {
    if (abort.signal.aborted) return null;
    return {
      nodeId: config.nodeId,
      status: "failed",
      error: errorMessage(error),
      result: {
        exitCode: null, timedOut: false, outputTail: "", verify: null, diffStat: "", branch: null, commit: null,
        pushed: false, pushError: null, durationMs: Date.now() - started,
      },
    };
  } finally {
    clearInterval(renewTimer);
    stopSignal?.removeEventListener("abort", onStop);
    await removeWorkspace(ws, job, config).catch((error) => config.log(`${job.id}: cleanup failed: ${errorMessage(error)}`));
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Daemon
// ---------------------------------------------------------------------------------------------------------------

export interface Worker {
  /** Pushes a heartbeat now (also what the file watchers trigger, debounced). */
  heartbeat(): Promise<void>;
  /** Stops polling, aborts running jobs, waits for them to clean up. */
  stop(): Promise<void>;
  readonly activeJobIds: string[];
}

const WATCH_DEBOUNCE_MS = 750;

export function startWorker(config: WorkerConfig, hub: HubClient = createHubClient(config.hubUrl, config.hubToken)): Worker {
  augmentPath();
  const detect = createDetector(config);
  const shutdown = new AbortController();
  const active = new Map<string, Promise<void>>();
  const watchers: FSWatcher[] = [];

  async function heartbeat(): Promise<void> {
    try {
      await hub.heartbeat(await detect([...active.keys()]));
    } catch (error) {
      config.log(`heartbeat failed: ${errorMessage(error)}`);
    }
  }

  function launch(job: JobRecord): void {
    config.log(`${job.id}: leased (${job.spec.cli}${job.spec.repo ? ` on ${job.spec.repo}` : ""}, attempt ${job.attempt})`);
    const task = (async () => {
      const report = await executeJob(job, config, hub, shutdown.signal);
      if (!report) return;
      const res = await hub.complete(job.id, report);
      config.log(`${job.id}: ${report.status}${res.ok ? "" : ` (hub rejected: ${res.error})`}`);
    })()
      .catch((error) => config.log(`${job.id}: ${errorMessage(error)}`))
      .finally(() => {
        active.delete(job.id);
        void heartbeat();
      });
    active.set(job.id, task);
  }

  async function pollLoop(): Promise<void> {
    await heartbeat();
    while (!shutdown.signal.aborted) {
      let leased = false;
      if (active.size < config.maxSlots) {
        try {
          const job = await hub.lease(config.nodeId);
          if (job) {
            launch(job);
            leased = true;
          }
        } catch (error) {
          config.log(`lease failed: ${errorMessage(error)}`);
        }
      }
      if (!leased) await sleep(config.pollMs, undefined, { signal: shutdown.signal }).catch(() => {});
    }
  }

  let debounce: NodeJS.Timeout | null = null;
  const scheduleHeartbeat = () => {
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => { debounce = null; void heartbeat(); }, WATCH_DEBOUNCE_MS);
  };
  if (config.watch) {
    for (const [dir, recursive] of [[config.projectsDir, false], [config.hermesProfilesDir, true]] as const) {
      if (!existsSync(dir)) continue;
      try {
        watchers.push(watch(dir, { recursive, persistent: false }, scheduleHeartbeat));
      } catch (error) {
        config.log(`cannot watch ${dir}: ${errorMessage(error)}`);
      }
    }
  }

  const beat = setInterval(() => void heartbeat(), config.heartbeatMs);
  const loop = pollLoop();
  config.log(`node ${config.nodeId} polling ${config.hubUrl} with ${config.maxSlots} slot(s)`);

  return {
    heartbeat,
    get activeJobIds() {
      return [...active.keys()];
    },
    async stop() {
      shutdown.abort();
      clearInterval(beat);
      if (debounce) clearTimeout(debounce);
      for (const watcher of watchers) watcher.close();
      await loop;
      await Promise.allSettled(active.values());
    },
  };
}

if (isMain(import.meta.url)) {
  const config = workerConfigFromEnv();
  const worker = startWorker(config);
  const stop = async () => {
    config.log("shutting down; in-flight jobs will be re-queued by the hub");
    await worker.stop();
    process.exit(0);
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}
