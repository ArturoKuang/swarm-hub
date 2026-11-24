import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { startWorker, type Worker } from "../src/worker.ts";
import { makeRepo, runCleanups, startHub, tempDir } from "./helpers.ts";

const MCP_ENTRY = fileURLToPath(new URL("../src/mcp.ts", import.meta.url));

let child: ChildProcessWithoutNullStreams | null = null;
let worker: Worker | null = null;
afterEach(async () => {
  child?.kill();
  child = null;
  await worker?.stop();
  worker = null;
  await runCleanups();
});

/** Minimal MCP client: newline-delimited JSON-RPC over the child's stdio. */
function mcpClient(hubUrl: string) {
  child = spawn(process.execPath, ["--experimental-strip-types", "--disable-warning=ExperimentalWarning", MCP_ENTRY], {
    env: { ...process.env, SWARM_HUB_URL: hubUrl },
  });
  const proc = child;
  const lines: unknown[] = [];
  const waiters = new Map<number | string, (msg: any) => void>();
  let stderr = "";
  proc.stderr.on("data", (d) => { stderr += d; });
  createInterface({ input: proc.stdout }).on("line", (line) => {
    const msg = JSON.parse(line); // every stdout line must be valid JSON-RPC
    lines.push(msg);
    waiters.get(msg.id)?.(msg);
  });
  let nextId = 1;
  return {
    lines,
    stderr: () => stderr,
    send(message: unknown) {
      proc.stdin.write(`${JSON.stringify(message)}\n`);
    },
    request(method: string, params?: unknown, timeoutMs = 20_000): Promise<any> {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`no response to ${method}; stderr: ${stderr}`)), timeoutMs);
        waiters.set(id, (msg) => { clearTimeout(timer); resolve(msg); });
        proc.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, ...(params === undefined ? {} : { params }) })}\n`);
      });
    },
    async callTool(name: string, args: unknown): Promise<{ isError: boolean; value: any; text: string }> {
      const res = await this.request("tools/call", { name, arguments: args }, 60_000);
      expect(res.error).toBeUndefined();
      const text = res.result.content[0].text as string;
      let value: unknown = null;
      try { value = JSON.parse(text); } catch { /* error text */ }
      return { isError: res.result.isError, value, text };
    },
  };
}

describe("MCP stdio server", () => {
  it("initialize → tools/list → list_fleet → dispatch_task → wait_jobs → get_job_result", async () => {
    const root = tempDir();
    const projectsDir = join(root, "Projects");
    mkdirSync(projectsDir);
    mkdirSync(join(root, "origins"));
    makeRepo(projectsDir, "demo", join(root, "origins"));
    const { url } = await startHub();
    worker = startWorker({
      hubUrl: url, nodeId: "mcp-node", projectsDir, workDir: join(root, "work"), hermesProfilesDir: join(root, "none"),
      maxSlots: 2, extraTags: [], pollMs: 50, heartbeatMs: 60_000, renewMs: 200, watch: false, log: () => {},
    });
    const mcp = mcpClient(url);

    const init = await mcp.request("initialize", {
      protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "vitest", version: "0" },
    });
    expect(init).toMatchObject({
      jsonrpc: "2.0", id: 1,
      result: { protocolVersion: "2025-03-26", capabilities: { tools: {} }, serverInfo: { name: "swarm-hub" } },
    });
    mcp.send({ jsonrpc: "2.0", method: "notifications/initialized" });

    const list = await mcp.request("tools/list");
    const tools = list.result.tools as Array<{ name: string; inputSchema: any; description: string }>;
    expect(tools.map((t) => t.name)).toEqual(["list_fleet", "dispatch_task", "wait_jobs", "get_job_result", "list_jobs", "cancel_job"]);
    for (const t of tools) {
      expect(t.inputSchema.type).toBe("object");
      expect(t.description.length).toBeGreaterThan(10);
    }
    expect(tools.find((t) => t.name === "dispatch_task")!.inputSchema.required).toEqual(["prompt"]);

    let fleet: any[] = [];
    for (let i = 0; i < 100 && !fleet.length; i++) {
      fleet = (await mcp.callTool("list_fleet", {})).value;
      if (!fleet.length) await new Promise((r) => setTimeout(r, 100));
    }
    expect(fleet[0]).toMatchObject({ nodeId: "mcp-node", online: true, repos: ["demo"] });

    const dispatched = await mcp.callTool("dispatch_task", {
      cli: "shell", repo: "demo", prompt: "echo mcp-ok > mcp.txt && cat mcp.txt", verifyCmd: "test -s mcp.txt", idempotencyKey: "mcp-test-1",
    });
    expect(dispatched.isError).toBe(false);
    expect(dispatched.value).toMatchObject({ status: "queued", deduplicated: false });
    const jobId = dispatched.value.id as string;
    const again = await mcp.callTool("dispatch_task", { cli: "shell", prompt: "echo other", idempotencyKey: "mcp-test-1" });
    expect(again.value).toMatchObject({ id: jobId, deduplicated: true });

    const waited = await mcp.callTool("wait_jobs", { jobIds: [jobId], timeoutSec: 30, pollMs: 100 });
    expect(waited.value).toMatchObject({ done: true, timedOut: false, jobs: [{ id: jobId, status: "succeeded", nodeId: "mcp-node" }] });

    const result = await mcp.callTool("get_job_result", { jobId });
    expect(result.value.job.result).toMatchObject({ exitCode: 0, verify: { exitCode: 0 }, branch: `swarm/${jobId}` });
    expect(result.value.job.result.outputTail).toContain("mcp-ok");
    expect(result.value.job.result.diffStat).toContain("mcp.txt");
    expect(result.value.events.map((e: any) => e.type)).toContain("succeeded");

    const listed = await mcp.callTool("list_jobs", { status: "succeeded" });
    expect(listed.value.map((j: any) => j.id)).toEqual([jobId]);

    // Only responses for requests: the notification produced no output line.
    expect(mcp.lines.every((m: any) => m.id !== undefined)).toBe(true);
  });

  it("negotiates the protocol version and reports errors the spec way", async () => {
    const { url } = await startHub();
    const mcp = mcpClient(url);
    expect((await mcp.request("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "old", version: "0" } }))
      .result.protocolVersion).toBe("2024-11-05");
    expect((await mcp.request("initialize", { protocolVersion: "1999-01-01", capabilities: {}, clientInfo: { name: "x", version: "0" } }))
      .result.protocolVersion).toBe("2025-03-26");
    expect((await mcp.request("ping")).result).toEqual({});
    expect((await mcp.request("resources/list")).error).toMatchObject({ code: -32601 });
    expect((await mcp.request("tools/call", { name: "nope", arguments: {} })).error).toMatchObject({ code: -32602 });

    const badArgs = await mcp.callTool("dispatch_task", { cli: "shell" });
    expect(badArgs.isError).toBe(true);
    expect(badArgs.text).toMatch(/invalid arguments/);
    const missing = await mcp.callTool("get_job_result", { jobId: "job_missing" });
    expect(missing.isError).toBe(true);
    expect(missing.text).toMatch(/404/);

    const queued = await mcp.callTool("dispatch_task", { cli: "shell", prompt: "true", requiredTags: ["sdk:nowhere"] });
    const cancelled = await mcp.callTool("cancel_job", { jobId: queued.value.id, reason: "test" });
    expect(cancelled.value).toMatchObject({ status: "cancelled", error: "test" });
    const timedOut = await mcp.callTool("wait_jobs", { jobIds: [queued.value.id], timeoutSec: 0.2, pollMs: 50 });
    expect(timedOut.value.done).toBe(true);

    mcp.send({ jsonrpc: "2.0", id: 99 }); // missing method → invalid request
    child!.stdin.write("{not json\n");
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && !mcp.lines.some((m: any) => m.error?.code === -32700)) await new Promise((r) => setTimeout(r, 20));
    expect(mcp.lines).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: 99, error: expect.objectContaining({ code: -32600 }) }),
      expect.objectContaining({ id: null, error: expect.objectContaining({ code: -32700 }) }),
    ]));
    expect(mcp.stderr()).not.toMatch(/Error/);
  });
});
