import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { createReconciler, DEFAULT_OMB_URL, ombUrlFromEnv } from "../src/reconciler.ts";
import { api, heartbeat, runCleanups, startHub, waitFor } from "./helpers.ts";

afterEach(runCleanups);

interface FakeBot { id: string; name: string; soul: string }
interface FakeGroup { id: string; name: string; memberIds: string[]; dm?: boolean; setup?: any }

/** In-memory OpenMausBot implementing the five routes the reconciler uses, with OMB's own validation rules. */
async function fakeOmb(seed: { bots?: FakeBot[]; groups?: FakeGroup[] } = {}) {
  const bots: FakeBot[] = [...(seed.bots ?? [])];
  const groups: FakeGroup[] = [...(seed.groups ?? [])];
  const calls: string[] = [];
  let n = 0;
  const server: Server = createServer(async (req, res) => {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : undefined;
    const path = req.url!.split("?")[0]!;
    calls.push(`${req.method} ${path}`);
    const send = (status: number, value: unknown) => { res.writeHead(status, { "content-type": "application/json" }); res.end(JSON.stringify(value)); };
    let m: RegExpMatchArray | null;
    if (req.method === "GET" && path === "/api/bots") return send(200, { bots, groups });
    if (req.method === "POST" && path === "/api/bots") {
      const bot = { id: `bot-${++n}`, name: body.name, soul: "" };
      bots.push(bot);
      return send(201, { bot });
    }
    if (req.method === "PATCH" && (m = path.match(/^\/api\/bots\/([\w-]+)\/profile$/))) {
      const bot = bots.find((b) => b.id === m![1]);
      if (!bot) return send(404, { error: "no such bot" });
      bot.soul = body.soul;
      return send(200, { bot });
    }
    if (req.method === "POST" && path === "/api/groups") {
      if (!Array.isArray(body.memberIds) || !body.memberIds.length || body.memberIds.some((id: string) => !bots.some((b) => b.id === id))) {
        return send(400, { error: "unknown channel member" });
      }
      const group = { id: `room-${++n}`, name: body.name, memberIds: body.memberIds };
      groups.push(group);
      return send(201, { group });
    }
    if (req.method === "PATCH" && (m = path.match(/^\/api\/groups\/([\w-]+)\/setup$/))) {
      const group = groups.find((g) => g.id === m![1]);
      if (!group) return send(404, { error: "no such room" });
      const r = body.defaultResponder;
      if (body.action !== "complete" || typeof body.bulletin !== "string" || r?.kind !== "auto" || !group.memberIds.includes(r.fallbackBotId)) {
        return send(400, { error: "invalid default responder" });
      }
      group.setup = body;
      return send(200, { group });
    }
    send(404, { error: `no route: ${req.method} ${path}` });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return { url, bots, groups, calls, close: () => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }) };
}

const profile = (name: string, soul = `# ${name}\n`) => ({ name, soul, soulHash: `h-${name}-${soul.length}` });
const repo = (name: string) => ({ name, path: `/nonexistent/${name}`, remoteUrl: `git@example.com:${name}.git`, branch: "main" });

const fleet = [
  heartbeat({ nodeId: "mac-mini", hermesProfiles: [profile("qa"), profile("coder")], repos: [repo("alpha"), repo("existing")] }),
  heartbeat({ nodeId: "studio", hermesProfiles: [profile("coder"), profile("lead")], repos: [repo("alpha"), repo("beta")] }),
];

describe("OpenMausBot reconciler", () => {
  it("creates only missing bots and rooms, never duplicating existing ones", async () => {
    const omb = await fakeOmb({
      bots: [{ id: "existing-qa", name: "QA", soul: "curated by hand" }],
      groups: [{ id: "room-old", name: "Repo: Existing", memberIds: ["existing-qa"] }, { id: "dm-1", name: "repo: beta", memberIds: [], dm: true }],
    });
    const reconciler = createReconciler({ ombUrl: omb.url, debounceMs: 10, fallbackBotName: "lead" });
    reconciler.observe(fleet);
    const result = await reconciler.flush();

    expect(result).toEqual({ kind: "synced", createdBots: ["coder", "lead"], createdGroups: ["repo: alpha", "repo: beta"], errors: [] });
    expect(omb.bots.map((b) => b.name)).toEqual(["QA", "coder", "lead"]);
    expect(omb.bots.find((b) => b.name === "QA")!.soul).toBe("curated by hand"); // existing bot untouched
    expect(omb.bots.find((b) => b.name === "coder")!.soul).toBe("# coder\n");
    const lead = omb.bots.find((b) => b.name === "lead")!;
    const alpha = omb.groups.find((g) => g.name === "repo: alpha")!;
    expect(alpha.memberIds.sort()).toEqual(["bot-1", "bot-2", "existing-qa"].sort());
    expect(alpha.setup.defaultResponder).toEqual({ kind: "auto", fallbackBotId: lead.id });
    expect(alpha.setup.bulletin).toContain("mac-mini, studio");
    expect(omb.groups.filter((g) => !g.dm).map((g) => g.name)).toEqual(["Repo: Existing", "repo: alpha", "repo: beta"]);

    // Same fleet again: no requests at all. A fresh reconciler (hub restart): it lists, then writes nothing.
    const before = omb.calls.length;
    expect(await reconciler.sync()).toEqual({ kind: "unchanged" });
    expect(omb.calls.length).toBe(before);
    const restarted = createReconciler({ ombUrl: omb.url, debounceMs: 10 });
    restarted.observe(fleet);
    expect(await restarted.flush()).toEqual({ kind: "synced", createdBots: [], createdGroups: [], errors: [] });
    expect(omb.calls.slice(before)).toEqual(["GET /api/bots"]);
    expect(omb.bots).toHaveLength(3);
    expect(omb.groups).toHaveLength(4);
    await omb.close();
  });

  it("debounces bursts of heartbeats into one sync and picks up newly discovered profiles", async () => {
    const omb = await fakeOmb();
    const reconciler = createReconciler({ ombUrl: omb.url, debounceMs: 40 });
    for (let i = 0; i < 10; i++) reconciler.observe([fleet[0]!]);
    await reconciler.flush();
    expect(omb.calls.filter((c) => c === "GET /api/bots")).toHaveLength(1);

    const grown = heartbeat({ ...fleet[0]!, hermesProfiles: [...fleet[0]!.hermesProfiles, profile("design")] });
    reconciler.observe([grown]);
    expect(await reconciler.flush()).toMatchObject({ kind: "synced", createdBots: ["design"], createdGroups: [] });
    expect(omb.bots.map((b) => b.name).sort()).toEqual(["coder", "design", "qa"]);
    await omb.close();
  });

  it("runs overlapping sync requests one at a time, without duplicate writes", async () => {
    const omb = await fakeOmb();
    const reconciler = createReconciler({ ombUrl: omb.url });
    reconciler.observe(fleet);
    await Promise.all([reconciler.sync(), reconciler.sync(), reconciler.sync()]);
    await reconciler.flush();
    expect(omb.bots.map((b) => b.name).sort()).toEqual(["coder", "lead", "qa"]);
    expect(omb.groups.map((g) => g.name).sort()).toEqual(["repo: alpha", "repo: beta", "repo: existing"]);
    await omb.close();
  });

  it("skips gracefully when OpenMausBot is unreachable or disabled, and recovers later", async () => {
    const omb = await fakeOmb();
    const deadUrl = omb.url;
    await omb.close();
    const logs: string[] = [];
    const offline = createReconciler({ ombUrl: deadUrl, requestTimeoutMs: 500, log: (m) => logs.push(m) });
    offline.observe(fleet);
    expect((await offline.sync()).kind).toBe("unreachable");
    expect((await offline.sync()).kind).toBe("unreachable");
    expect(logs.filter((l) => l.includes("unreachable"))).toHaveLength(1); // logged once, not every heartbeat

    const disabled = createReconciler({ ombUrl: null });
    disabled.observe(fleet);
    expect(await disabled.sync()).toEqual({ kind: "disabled" });

    expect(ombUrlFromEnv({})).toBe(DEFAULT_OMB_URL);
    expect(ombUrlFromEnv({ OMB_URL: "" })).toBeNull();
    expect(ombUrlFromEnv({ OMB_URL: "off" })).toBeNull();
    expect(ombUrlFromEnv({ OMB_URL: "http://10.0.0.2:8799/" })).toBe("http://10.0.0.2:8799");
  });

  it("syncs in real time from heartbeats posted to the hub", async () => {
    const omb = await fakeOmb();
    const reconciler = createReconciler({ ombUrl: omb.url, debounceMs: 20 });
    const { url } = await startHub({ reconciler });
    for (const hb of fleet) expect((await api(url, "POST", "/api/nodes/heartbeat", hb)).status).toBe(200);
    await waitFor(() => omb.groups.length === 3 && omb.bots.length === 3, 5000, "OpenMausBot sync");
    await reconciler.flush();
    expect(omb.bots.map((b) => b.name).sort()).toEqual(["coder", "lead", "qa"]);
    expect(omb.groups.every((g) => g.setup?.defaultResponder?.kind === "auto")).toBe(true);
    await omb.close();
  });
});
