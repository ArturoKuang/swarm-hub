import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { SwarmDb, type DbOptions } from "../src/db.ts";
import { createHub, type Hub, type HubOptions } from "../src/hub.ts";
import type { NodeHeartbeat } from "../src/protocol.ts";

const cleanups: Array<() => unknown> = [];

/** Runs registered cleanups in reverse order; call from afterEach. */
export async function runCleanups(): Promise<void> {
  while (cleanups.length) await cleanups.pop()!();
}

export function tempDir(prefix = "swarm-test-"): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

export function heartbeat(overrides: Partial<NodeHeartbeat> & { nodeId: string }): NodeHeartbeat {
  const off = { available: false, version: null };
  return {
    hostname: `${overrides.nodeId}.local`,
    tailscaleIp: "100.100.0.1",
    os: "darwin",
    arch: "arm64",
    tags: [],
    clis: { claude: { available: true, version: "1.0.0" }, codex: off, agy: off, hermes: off },
    resources: { freeDiskGb: 100, freeRamGb: 16, cpuCount: 8 },
    maxSlots: 2,
    activeJobIds: [],
    repos: [],
    hermesProfiles: [],
    ...overrides,
  };
}

export interface TestHub { url: string; db: SwarmDb; hub: Hub }

export async function startHub(options: Omit<HubOptions, "db"> & { db?: DbOptions } = {}): Promise<TestHub> {
  const db = new SwarmDb(join(tempDir(), "hub.db"), options.db);
  const hub = createHub({ host: "127.0.0.1", port: 0, log: () => {}, ...options, db });
  const url = await hub.listen();
  cleanups.push(() => db.close());
  cleanups.push(() => hub.close());
  return { url, db, hub };
}

export async function api<T = any>(url: string, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<{ status: number; body: T }> {
  const res = await fetch(`${url}${path}`, {
    method,
    headers: { ...(body === undefined ? {} : { "content-type": "application/json" }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

export async function waitFor<T>(probe: () => Promise<T | undefined | null | false> | T | undefined | null | false, timeoutMs = 15_000, label = "condition"): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await sleep(50);
  }
}

export function sh(cwd: string, ...args: string[]): string {
  return execFileSync("git", ["-c", "user.name=test", "-c", "user.email=test@example.com", ...args], { cwd, encoding: "utf8" }).trim();
}

/** A repo at <projectsDir>/<name> with one commit on main, pushed to a bare origin. Returns the origin path. */
export function makeRepo(projectsDir: string, name: string, originsDir: string): string {
  const origin = join(originsDir, `${name}.git`);
  execFileSync("git", ["init", "--bare", "-q", "-b", "main", origin]);
  const repo = join(projectsDir, name);
  execFileSync("git", ["init", "-q", "-b", "main", repo]);
  writeFileSync(join(repo, "README.md"), `# ${name}\n`);
  sh(repo, "add", "README.md");
  sh(repo, "commit", "-q", "-m", "init");
  sh(repo, "remote", "add", "origin", origin);
  sh(repo, "push", "-q", "-u", "origin", "main");
  return origin;
}
