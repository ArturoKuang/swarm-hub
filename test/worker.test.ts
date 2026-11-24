import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { JobSpec, type JobRecord, type NodeHeartbeat } from "../src/protocol.ts";
import { buildCommand, composePrompt, scanHermesProfiles, scanRepos, startWorker, type Worker, type WorkerConfig } from "../src/worker.ts";
import { api, makeRepo, runCleanups, sh, startHub, tempDir, waitFor } from "./helpers.ts";

const workers: Worker[] = [];
afterEach(async () => {
  // Workers stop before the hub and temp dirs go away.
  while (workers.length) await workers.pop()!.stop();
  await runCleanups();
});

interface Rig { url: string; projectsDir: string; workDir: string; hermesDir: string; origin: string; repo: string; worker: Worker }

async function rig(overrides: Partial<WorkerConfig> = {}): Promise<Rig> {
  const root = tempDir();
  const projectsDir = join(root, "Projects");
  const workDir = join(root, "swarm-worker");
  const hermesDir = join(root, "hermes", "profiles");
  mkdirSync(projectsDir, { recursive: true });
  mkdirSync(join(hermesDir, "coder"), { recursive: true });
  writeFileSync(join(hermesDir, "coder", "SOUL.md"), "# Coder\nShip it.\n");
  mkdirSync(join(root, "origins"));
  const origin = makeRepo(projectsDir, "demo", join(root, "origins"));
  const { url } = await startHub({ db: { leaseTtlMs: 5_000 } });
  const worker = startWorker({
    hubUrl: url, nodeId: "test-node", projectsDir, workDir, hermesProfilesDir: hermesDir, maxSlots: 2, extraTags: ["lab"],
    pollMs: 50, heartbeatMs: 60_000, renewMs: 150, watch: true, log: () => {}, ...overrides,
  });
  workers.push(worker);
  return { url, projectsDir, workDir, hermesDir, origin, repo: join(projectsDir, "demo"), worker };
}

async function dispatchAndWait(url: string, spec: Record<string, unknown>, timeoutMs = 20_000): Promise<JobRecord> {
  const created = await api(url, "POST", "/api/jobs", { cli: "shell", ...spec });
  expect(created.status).toBe(201);
  const id = created.body.job.id;
  return waitFor(async () => {
    const job = (await api(url, "GET", `/api/jobs/${id}`)).body.job as JobRecord;
    return ["succeeded", "failed", "cancelled"].includes(job.status) ? job : null;
  }, timeoutMs, `job ${id}`);
}

const fleetNode = async (url: string): Promise<NodeHeartbeat | undefined> =>
  (await api(url, "GET", "/api/fleet")).body.nodes.find((n: any) => n.nodeId === "test-node")?.heartbeat;

describe("worker", () => {
  it("advertises repos, Hermes profiles, CLIs, and resources in its heartbeat", async () => {
    const { url, origin } = await rig();
    const hb = await waitFor(() => fleetNode(url), 10_000, "first heartbeat");
    expect(hb.repos).toEqual([{ name: "demo", path: expect.stringContaining("demo"), remoteUrl: origin, branch: "main" }]);
    expect(hb.hermesProfiles).toEqual([{ name: "coder", soul: "# Coder\nShip it.\n", soulHash: expect.stringMatching(/^[0-9a-f]{16}$/) }]);
    expect(hb.tags).toEqual(expect.arrayContaining(["sdk:node", "lab"]));
    expect(hb.os).toBe(process.platform);
    expect(hb.resources.freeDiskGb).toBeGreaterThan(0);
    expect(hb.resources.cpuCount).toBeGreaterThan(0);
    expect(Object.keys(hb.clis).sort()).toEqual(["agy", "claude", "codex", "hermes"]);
  });

  it("pushes an immediate heartbeat when a new project or Hermes profile appears", async () => {
    const { url, projectsDir, hermesDir } = await rig();
    await waitFor(() => fleetNode(url), 10_000, "first heartbeat");
    makeRepo(projectsDir, "fresh", join(projectsDir, "..", "origins"));
    mkdirSync(join(hermesDir, "qa"));
    writeFileSync(join(hermesDir, "qa", "SOUL.md"), "# QA\n");
    // heartbeatMs is 60s, so only the fs.watch path can deliver this in time.
    const hb = await waitFor(async () => {
      const node = await fleetNode(url);
      return node?.repos.some((r) => r.name === "fresh") && node.hermesProfiles.some((p) => p.name === "qa") ? node : null;
    }, 8_000, "watch-triggered heartbeat");
    expect(hb.repos.map((r) => r.name)).toEqual(["demo", "fresh"]);
  });

  it("runs a shell job end to end in an isolated git worktree, verifies, commits, pushes, and cleans up", async () => {
    const { url, repo, origin, workDir } = await rig();
    const job = await dispatchAndWait(url, {
      repo: "demo",
      prompt: "echo hello-swarm > hello.txt && echo \"ran in $(git rev-parse --abbrev-ref HEAD) for $SWARM_JOB_ID\"",
      verifyCmd: "grep -q hello-swarm hello.txt",
    });

    expect(job.status).toBe("succeeded");
    const branch = `swarm/${job.id}`;
    expect(job.result).toMatchObject({ exitCode: 0, timedOut: false, branch, pushed: true, pushError: null, verify: { exitCode: 0 } });
    expect(job.result!.outputTail).toContain(`ran in ${branch} for ${job.id}`);
    expect(job.result!.diffStat).toMatch(/hello\.txt \| 1 \+/);

    // The commit landed on the task branch locally and on the remote; the checkout on main is untouched.
    expect(execFileSync("git", ["show", `${branch}:hello.txt`], { cwd: repo, encoding: "utf8" })).toBe("hello-swarm\n");
    expect(execFileSync("git", ["rev-parse", branch], { cwd: origin, encoding: "utf8" }).trim()).toBe(job.result!.commit);
    expect(sh(repo, "rev-parse", "--abbrev-ref", "HEAD")).toBe("main");
    expect(existsSync(join(repo, "hello.txt"))).toBe(false);
    // Worktree removed.
    expect(sh(repo, "worktree", "list").split("\n")).toHaveLength(1);
    expect(readdirSync(join(workDir, "workspaces"))).toEqual([]);

    const events = (await api(url, "GET", `/api/jobs/${job.id}/events`)).body.events.map((e: any) => e.type);
    expect(events[0]).toBe("created");
    expect(events).toContain("leased");
    expect(events.at(-1)).toBe("succeeded");
  });

  it("clones from repoUrl when the repo is not checked out locally", async () => {
    const { url, origin, workDir } = await rig();
    const job = await dispatchAndWait(url, { repo: "remote-only", repoUrl: origin, baseBranch: "main", prompt: "test -f README.md && touch cloned.txt" });
    expect(job.status).toBe("succeeded");
    expect(job.result).toMatchObject({ pushed: true, diffStat: expect.stringContaining("cloned.txt") });
    expect(existsSync(join(workDir, "repos", "remote-only", ".git"))).toBe(true);
  });

  it("reports a failing verifyCmd as failed and does not push", async () => {
    const { url, origin } = await rig();
    const job = await dispatchAndWait(url, { repo: "demo", prompt: "echo nope > out.txt", verifyCmd: "grep -q yes out.txt" });
    expect(job.status).toBe("failed");
    expect(job.error).toMatch(/verify command exited with 1/);
    expect(job.result).toMatchObject({ exitCode: 0, verify: { exitCode: 1 }, pushed: false, commit: expect.any(String) });
    expect(() => execFileSync("git", ["rev-parse", "--verify", `swarm/${job.id}`], { cwd: origin, stdio: "ignore" })).toThrow();
  });

  it("kills a job at its timeout", async () => {
    const { url } = await rig();
    const started = Date.now();
    const job = await dispatchAndWait(url, { prompt: "sleep 30", timeoutSec: 1 });
    expect(job.status).toBe("failed");
    expect(job.result).toMatchObject({ timedOut: true });
    expect(Date.now() - started).toBeLessThan(10_000);
  });

  it("stops a running job when it is cancelled and frees the slot", async () => {
    const { url, worker, workDir } = await rig();
    const created = await api(url, "POST", "/api/jobs", { cli: "shell", repo: "demo", prompt: "echo started; sleep 60" });
    const id = created.body.job.id;
    await waitFor(() => worker.activeJobIds.includes(id), 10_000, "job to start");
    await api(url, "POST", `/api/jobs/${id}/cancel`);
    await waitFor(() => worker.activeJobIds.length === 0, 10_000, "worker to drop the cancelled job");
    expect((await api(url, "GET", `/api/jobs/${id}`)).body.job.status).toBe("cancelled");
    expect(readdirSync(join(workDir, "workspaces"))).toEqual([]);
  });

  it("runs jobs in parallel up to maxSlots", async () => {
    const { url } = await rig({ maxSlots: 3 });
    const started = Date.now();
    const jobs = await Promise.all([1, 2, 3].map((i) => dispatchAndWait(url, { repo: "demo", prompt: `sleep 1.5 && echo ${i} > f${i}.txt` })));
    expect(jobs.map((j) => j.status)).toEqual(["succeeded", "succeeded", "succeeded"]);
    expect(Date.now() - started).toBeLessThan(4_000); // serial would take ≥ 4.5s
    expect(new Set(jobs.map((j) => j.result!.branch)).size).toBe(3);
  });
});

describe("worker building blocks", () => {
  it("scans repos and profiles from disk", async () => {
    const root = tempDir();
    mkdirSync(join(root, "p", "not-a-repo"), { recursive: true });
    mkdirSync(join(root, "o"));
    makeRepo(join(root, "p"), "r1", join(root, "o"));
    expect((await scanRepos(join(root, "p"))).map((r) => r.name)).toEqual(["r1"]);
    expect(await scanRepos(join(root, "missing"))).toEqual([]);
    expect(await scanHermesProfiles(join(root, "missing"))).toEqual([]);
  });

  it("injects the skill loadout into agent prompts and builds each CLI's command line", () => {
    const spec = JobSpec.parse({ prompt: "Fix the bug", cli: "claude", model: "opus", skillLoadout: ["tdd", "deslop"], taskBranch: "swarm/x", verifyCmd: "npm test" });
    const prompt = composePrompt(spec);
    expect(prompt).toMatch(/^## Skill loadout\nBefore starting, load and follow these skills: tdd, deslop\./);
    expect(prompt).toContain("## Task\nFix the bug");
    expect(prompt).toContain("`npm test`");
    expect(buildCommand(spec, prompt)).toEqual({ command: "claude", args: ["-p", prompt, "--dangerously-skip-permissions", "--model", "opus"] });
    expect(buildCommand({ ...spec, cli: "codex" }, "p").args).toEqual(["exec", "--approve-for-me", "--skip-git-repo-check", "--model", "opus", "--", "p"]);
    expect(buildCommand({ ...spec, cli: "agy" }, "p").command).toBe("agy");
    const shell = JobSpec.parse({ prompt: "make test", cli: "shell", skillLoadout: ["tdd"] });
    expect(buildCommand(shell, composePrompt(shell))).toEqual({ command: "/bin/sh", args: ["-c", "make test"] });
  });
});
