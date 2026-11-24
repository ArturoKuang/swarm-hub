import { request } from "node:http";
import { networkInterfaces } from "node:os";
import { afterEach, describe, expect, it } from "vitest";
import type { JobRecord, JobResult } from "../src/protocol.ts";
import { api, heartbeat, runCleanups, startHub } from "./helpers.ts";

afterEach(runCleanups);

const okResult: JobResult = {
  exitCode: 0, timedOut: false, outputTail: "ok", verify: null, diffStat: "", branch: null, commit: null,
  pushed: false, pushError: null, durationMs: 1,
};

async function lease(url: string, nodeId: string): Promise<JobRecord | null> {
  const res = await api(url, "POST", `/api/workers/${nodeId}/lease`, {});
  expect(res.status).toBe(200);
  return res.body.job;
}

async function dispatch(url: string, spec: Record<string, unknown>): Promise<JobRecord> {
  const res = await api(url, "POST", "/api/jobs", { cli: "shell", prompt: "true", ...spec });
  expect(res.status).toBe(201);
  return res.body.job;
}

describe("hub", () => {
  it("serves /health and validates payloads at the boundary", async () => {
    const { url } = await startHub();
    expect((await api(url, "GET", "/health")).body.ok).toBe(true);
    const bad = await api(url, "POST", "/api/nodes/heartbeat", { nodeId: "x" });
    expect(bad.status).toBe(400);
    expect(bad.body.issues.length).toBeGreaterThan(0);
    expect((await api(url, "POST", "/api/jobs", { prompt: "" })).status).toBe(400);
    expect((await api(url, "GET", "/api/jobs/nope")).status).toBe(404);
    expect((await api(url, "DELETE", "/api/jobs")).status).toBe(405);
  });

  it("routes jobs across two workers by their distinct capability tags", async () => {
    const { url } = await startHub();
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "ios-box", tags: ["sdk:xcode"] }));
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "droid-box", os: "linux", tags: ["sdk:android"] }));
    const iosJob = await dispatch(url, { requiredTags: ["sdk:xcode"] });
    const droidJob = await dispatch(url, { requiredTags: ["sdk:android", "os:linux"] });

    // droid-box polls first but must skip the older iOS job it cannot run.
    expect((await lease(url, "droid-box"))?.id).toBe(droidJob.id);
    expect(await lease(url, "droid-box")).toBeNull();
    expect((await lease(url, "ios-box"))?.id).toBe(iosJob.id);

    const fleet = (await api(url, "GET", "/api/fleet")).body.nodes;
    expect(fleet.map((n: any) => [n.nodeId, n.online, n.runningJobIds])).toEqual([
      ["droid-box", true, [droidJob.id]],
      ["ios-box", true, [iosJob.id]],
    ]);
  });

  it("fans parallel jobs out across nodes without ever double-leasing", async () => {
    const { url } = await startHub();
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "a", maxSlots: 2 }));
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "b", maxSlots: 2 }));
    const jobs = await Promise.all([1, 2, 3, 4, 5].map((i) => dispatch(url, { prompt: `echo ${i}` })));

    const leased: Array<[string, JobRecord | null]> = [];
    for (let round = 0; round < 3; round++) {
      const results = await Promise.all(["a", "b", "a", "b"].map(async (node) => [node, await lease(url, node)] as [string, JobRecord | null]));
      leased.push(...results);
    }
    const granted = leased.filter(([, job]) => job);
    expect(granted).toHaveLength(4); // 2 nodes × 2 slots; the 5th job waits
    expect(new Set(granted.map(([, job]) => job!.id)).size).toBe(4);
    expect(granted.filter(([node]) => node === "a")).toHaveLength(2);
    expect(granted.filter(([node]) => node === "b")).toHaveLength(2);
    for (const [node, job] of granted) expect(job!.nodeId).toBe(node);

    const queued = (await api(url, "GET", "/api/jobs?status=queued")).body.jobs;
    expect(queued.map((j: JobRecord) => j.id)).toEqual([jobs[4]!.id]);
  });

  it("re-queues a crashed worker's job after its lease expires and lets another node finish it", async () => {
    let clock = 1_000_000;
    const { url } = await startHub({ now: () => clock, db: { leaseTtlMs: 30_000 } });
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "crashy" }));
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "steady" }));
    const job = await dispatch(url, { maxRetries: 1, preferredNodeId: "crashy" });

    const first = await lease(url, "crashy");
    expect(first).toMatchObject({ id: job.id, attempt: 1, status: "running" });
    // crashy dies: no renewals. Time passes beyond the lease TTL while steady keeps heartbeating.
    clock += 31_000;
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "steady" }));

    const second = await lease(url, "steady");
    expect(second).toMatchObject({ id: job.id, attempt: 2, nodeId: "steady" });

    // A zombie crashy that wakes up is told to stop and cannot complete the job.
    const zombie = await api(url, "POST", `/api/jobs/${job.id}/progress`, { nodeId: "crashy", message: "still here" });
    expect(zombie.body).toMatchObject({ ok: false, cancelled: true });
    expect((await api(url, "POST", `/api/jobs/${job.id}/complete`, { nodeId: "crashy", status: "succeeded", result: okResult })).status).toBe(409);

    const done = await api(url, "POST", `/api/jobs/${job.id}/complete`, { nodeId: "steady", status: "succeeded", result: okResult });
    expect(done.body.job).toMatchObject({ status: "succeeded", nodeId: "steady", attempt: 2 });
    const types = (await api(url, "GET", `/api/jobs/${job.id}/events`)).body.events.map((e: any) => e.type);
    expect(types).toEqual(["created", "leased", "lease_expired", "requeued", "leased", "succeeded"]);
  });

  it("fails a job once lease expiries exhaust maxRetries", async () => {
    let clock = 1_000_000;
    const { url, db } = await startHub({ now: () => clock, db: { leaseTtlMs: 10_000 } });
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "n" }));
    const job = await dispatch(url, { maxRetries: 0 });
    await lease(url, "n");
    clock += 11_000;
    expect(db.recoverExpiredLeases(clock)).toEqual([job.id]);
    const after = (await api(url, "GET", `/api/jobs/${job.id}`)).body.job;
    expect(after.status).toBe("failed");
    expect(after.error).toMatch(/no retries left/);
  });

  it("renews leases on progress so long jobs survive", async () => {
    let clock = 1_000_000;
    const { url } = await startHub({ now: () => clock, db: { leaseTtlMs: 10_000 } });
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "n" }));
    const job = await dispatch(url, {});
    await lease(url, "n");
    for (let i = 0; i < 5; i++) {
      clock += 8_000;
      await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "n" }));
      expect((await api(url, "POST", `/api/jobs/${job.id}/progress`, { nodeId: "n", message: `step ${i}` })).body.ok).toBe(true);
      await lease(url, "n"); // triggers lease recovery; must not reclaim a renewed job
    }
    expect((await api(url, "GET", `/api/jobs/${job.id}`)).body.job).toMatchObject({ status: "running", attempt: 1 });
  });

  it("marks nodes offline after 45s of silence and re-queues their jobs", async () => {
    let clock = 1_000_000;
    const { url } = await startHub({ now: () => clock, db: { leaseTtlMs: 600_000 } });
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "gone" }));
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "alive" }));
    const job = await dispatch(url, { preferredNodeId: "gone" });
    await lease(url, "gone");
    clock += 46_000;
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "alive" }));
    const nodes = (await api(url, "GET", "/api/fleet")).body.nodes;
    expect(nodes.map((n: any) => [n.nodeId, n.online])).toEqual([["alive", true], ["gone", false]]);
    expect((await lease(url, "alive"))?.id).toBe(job.id);
  });

  it("deduplicates jobs by idempotencyKey", async () => {
    const { url } = await startHub();
    const first = await api(url, "POST", "/api/jobs", { cli: "shell", prompt: "true", idempotencyKey: "bot-42:task-7" });
    const again = await api(url, "POST", "/api/jobs", { cli: "shell", prompt: "something else", idempotencyKey: "bot-42:task-7" });
    expect(first.status).toBe(201);
    expect(again.status).toBe(200);
    expect(again.body).toMatchObject({ deduplicated: true, job: { id: first.body.job.id, spec: { prompt: "true" } } });
    const concurrent = await Promise.all([1, 2, 3, 4].map(() =>
      api(url, "POST", "/api/jobs", { cli: "shell", prompt: "x", idempotencyKey: "race" })));
    expect(new Set(concurrent.map((r) => r.body.job.id)).size).toBe(1);
    expect(concurrent.filter((r) => !r.body.deduplicated)).toHaveLength(1);
    expect((await api(url, "GET", "/api/jobs")).body.jobs).toHaveLength(2);
  });

  it("cancels queued jobs immediately and tells a running worker to stop", async () => {
    const { url } = await startHub();
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "n", maxSlots: 1 }));
    const running = await dispatch(url, {});
    const queued = await dispatch(url, {});
    await lease(url, "n");

    expect((await api(url, "POST", `/api/jobs/${queued.id}/cancel`)).body.job.status).toBe("cancelled");
    expect((await api(url, "POST", `/api/jobs/${running.id}/cancel`, { reason: "operator" })).body.job).toMatchObject({ status: "cancelled", error: "operator" });
    expect((await api(url, "POST", `/api/jobs/${running.id}/progress`, { nodeId: "n" })).body).toMatchObject({ ok: false, cancelled: true });
    expect((await api(url, "POST", `/api/jobs/${running.id}/complete`, { nodeId: "n", status: "succeeded", result: okResult })).status).toBe(409);
    expect(await lease(url, "n")).toBeNull();
  });

  it("streams job events over SSE and closes when the job finishes", async () => {
    const { url } = await startHub();
    await api(url, "POST", "/api/nodes/heartbeat", heartbeat({ nodeId: "n" }));
    const job = await dispatch(url, {});
    const res = await fetch(`${url}/api/jobs/${job.id}/events`, { headers: { accept: "text/event-stream" } });
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    const body = res.text();

    await lease(url, "n");
    await api(url, "POST", `/api/jobs/${job.id}/progress`, { nodeId: "n", message: "halfway" });
    await api(url, "POST", `/api/jobs/${job.id}/complete`, { nodeId: "n", status: "succeeded", result: okResult });

    const text = await body;
    const events = [...text.matchAll(/^event: (\w+)$/gm)].map((m) => m[1]);
    expect(events).toEqual(["created", "leased", "progress", "succeeded", "end"]);
    expect(text).toContain('"message":"halfway"');

    // A finished job replays its history and closes at once; ?after= skips what the client has.
    const replay = await (await fetch(`${url}/api/jobs/${job.id}/events?after=2`, { headers: { accept: "text/event-stream" } })).text();
    expect([...replay.matchAll(/^event: (\w+)$/gm)].map((m) => m[1])).toEqual(["progress", "succeeded", "end"]);
  });

  it("requires the bearer token off-loopback when SWARM_HUB_TOKEN is set", async () => {
    const lan = Object.values(networkInterfaces()).flat().find((a) => a && a.family === "IPv4" && !a.internal)?.address;
    const { url } = await startHub({ token: "s3cret", host: "0.0.0.0" });
    const port = new URL(url).port;
    expect((await api(url, "GET", "/api/fleet")).status).toBe(200); // loopback is trusted
    if (!lan) return; // no non-loopback interface on this machine
    const remote = `http://${lan}:${port}`;
    const status = (headers: Record<string, string> = {}) => new Promise<number>((resolve, reject) => {
      // Bind the client to the LAN address so the hub sees a non-loopback peer.
      const req = request(`${remote}/api/fleet`, { localAddress: lan, headers }, (res) => { res.resume(); resolve(res.statusCode!); });
      req.on("error", reject);
      req.end();
    });
    expect(await status()).toBe(401);
    expect(await status({ authorization: "Bearer wrong!" })).toBe(401);
    expect(await status({ authorization: "Bearer s3cret" })).toBe(200);
    expect((await fetch(`${remote}/health`)).status).toBe(200);
  });
});
