// Mirrors what the fleet reports into OpenMausBot: every Hermes profile becomes a bot, every repo a room.
// Create-only and name-keyed, so re-running against the same fleet converges to zero writes. Existing bots and
// rooms (matched case-insensitively by name) are never patched or duplicated.
import { existsSync } from "node:fs";
import type { HermesProfile, NodeHeartbeat, RepoInfo } from "./protocol.ts";

export const DEFAULT_OMB_URL = "http://127.0.0.1:8799";

export interface ReconcilerOptions {
  /** null disables syncing entirely. */
  ombUrl: string | null;
  token?: string;
  debounceMs?: number;
  requestTimeoutMs?: number;
  /** Bot that answers a repo room when no member is a better fit; first synced bot when absent. */
  fallbackBotName?: string;
  log?: (message: string) => void;
}

export type SyncResult =
  | { kind: "disabled" }
  | { kind: "unchanged" }
  | { kind: "unreachable"; error: string }
  | { kind: "synced"; createdBots: string[]; createdGroups: string[]; errors: string[] };

interface OmbBot { id: string; name: string }
interface OmbGroup { id: string; name: string; dm?: boolean }

export interface Reconciler {
  /** Feed the current online fleet; a sync runs after the debounce window. */
  observe(heartbeats: readonly NodeHeartbeat[]): void;
  /** Sync now against the last observed fleet (skips when nothing changed since the last full sync). */
  sync(): Promise<SyncResult>;
  /** Resolves once any pending or in-flight sync has settled. */
  flush(): Promise<SyncResult | null>;
  close(): void;
}

/** Resolve OMB_URL: unset → loopback default, "" / "off" → disabled. */
export function ombUrlFromEnv(env: NodeJS.ProcessEnv = process.env): string | null {
  const raw = env.OMB_URL;
  if (raw === undefined) return DEFAULT_OMB_URL;
  const trimmed = raw.trim();
  return trimmed === "" || trimmed.toLowerCase() === "off" ? null : trimmed.replace(/\/+$/, "");
}

export const roomNameForRepo = (repo: string) => `repo: ${repo}`;

const key = (name: string) => name.trim().toLowerCase();

/** Desired state, deduplicated by name. Sorted so the fingerprint is stable across heartbeat order. */
export function desiredState(heartbeats: readonly NodeHeartbeat[]) {
  const profiles = new Map<string, HermesProfile>();
  const repos = new Map<string, { repo: RepoInfo; nodes: string[] }>();
  for (const hb of [...heartbeats].sort((a, b) => a.nodeId.localeCompare(b.nodeId))) {
    for (const profile of hb.hermesProfiles) if (!profiles.has(key(profile.name))) profiles.set(key(profile.name), profile);
    for (const repo of hb.repos) {
      const entry = repos.get(key(repo.name)) ?? { repo, nodes: [] };
      entry.nodes.push(hb.nodeId);
      repos.set(key(repo.name), entry);
    }
  }
  const byName = <T>(map: Map<string, T>) => [...map.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, v]) => v);
  return { profiles: byName(profiles), repos: byName(repos) };
}

export function createReconciler(options: ReconcilerOptions): Reconciler {
  const debounceMs = options.debounceMs ?? 2000;
  const timeoutMs = options.requestTimeoutMs ?? 5000;
  const log = options.log ?? (() => {});
  let fleet: readonly NodeHeartbeat[] = [];
  let lastSyncedFingerprint: string | null = null;
  let timer: NodeJS.Timeout | null = null;
  let inFlight: Promise<SyncResult> | null = null;
  let rerun = false;
  let lastUnreachable = false;

  async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { accept: "application/json" };
    if (body !== undefined) headers["content-type"] = "application/json";
    if (options.token) headers.authorization = `Bearer ${options.token}`;
    const res = await fetch(`${options.ombUrl}${path}`, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`${method} ${path} → ${res.status}: ${text.slice(0, 300)}`);
    return (text ? JSON.parse(text) : {}) as T;
  }

  async function runSync(): Promise<SyncResult> {
    if (!options.ombUrl) return { kind: "disabled" };
    const desired = desiredState(fleet);
    const fingerprint = JSON.stringify([
      desired.profiles.map((p) => [key(p.name), p.soulHash]),
      desired.repos.map((r) => key(r.repo.name)),
    ]);
    if (fingerprint === lastSyncedFingerprint) return { kind: "unchanged" };

    let listing: { bots?: OmbBot[]; groups?: OmbGroup[] };
    try {
      listing = await call("GET", "/api/bots?messages=0");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!lastUnreachable) log(`reconciler: OpenMausBot unreachable at ${options.ombUrl} (${message}); skipping`);
      lastUnreachable = true;
      return { kind: "unreachable", error: message };
    }
    lastUnreachable = false;

    const bots = new Map((listing.bots ?? []).map((bot) => [key(bot.name), bot]));
    const groups = new Map((listing.groups ?? []).filter((g) => !g.dm).map((group) => [key(group.name), group]));
    const createdBots: string[] = [];
    const createdGroups: string[] = [];
    const errors: string[] = [];

    for (const profile of desired.profiles) {
      if (bots.has(key(profile.name))) continue;
      try {
        const made = await call<{ bot: OmbBot }>("POST", "/api/bots", {
          useDefaults: false, name: profile.name, description: `Hermes profile ${profile.name} (swarm-hub)`,
        });
        bots.set(key(profile.name), made.bot);
        createdBots.push(profile.name);
        if (profile.soul.trim()) await call("PATCH", `/api/bots/${encodeURIComponent(made.bot.id)}/profile`, { soul: profile.soul });
      } catch (error) {
        errors.push(`bot ${profile.name}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    const memberIds = desired.profiles.map((p) => bots.get(key(p.name))?.id).filter((id): id is string => Boolean(id));
    const fallbackBotId = (options.fallbackBotName && bots.get(key(options.fallbackBotName))?.id) || memberIds[0];
    for (const { repo, nodes } of desired.repos) {
      const name = roomNameForRepo(repo.name);
      if (groups.has(key(name))) continue;
      if (!memberIds.length || !fallbackBotId) {
        errors.push(`room ${name}: no bots to seat`);
        continue;
      }
      const members = memberIds.includes(fallbackBotId) ? memberIds : [fallbackBotId, ...memberIds];
      try {
        const made = await call<{ group: OmbGroup }>("POST", "/api/groups", { name, memberIds: members });
        groups.set(key(name), made.group);
        createdGroups.push(name);
        await call("PATCH", `/api/groups/${encodeURIComponent(made.group.id)}/setup`, {
          action: "complete",
          cwd: existsSync(repo.path) ? repo.path : null,
          bulletin: [
            `Repo **${repo.name}** discovered by swarm-hub.`,
            `Nodes: ${nodes.join(", ")}`,
            repo.remoteUrl ? `Remote: ${repo.remoteUrl}` : null,
            repo.branch ? `Branch: ${repo.branch}` : null,
          ].filter(Boolean).join("\n"),
          defaultResponder: { kind: "auto", fallbackBotId },
        });
      } catch (error) {
        errors.push(`room ${name}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    if (!errors.length) lastSyncedFingerprint = fingerprint;
    if (createdBots.length || createdGroups.length) log(`reconciler: created bots [${createdBots.join(", ")}] rooms [${createdGroups.join(", ")}]`);
    for (const error of errors) log(`reconciler: ${error}`);
    return { kind: "synced", createdBots, createdGroups, errors };
  }

  // One sync at a time; a request that arrives mid-sync runs once more afterwards with the newest fleet.
  function sync(): Promise<SyncResult> {
    if (inFlight) {
      rerun = true;
      return inFlight;
    }
    inFlight = runSync().finally(() => {
      inFlight = null;
      if (rerun) {
        rerun = false;
        void sync();
      }
    });
    return inFlight;
  }

  return {
    observe(heartbeats) {
      fleet = heartbeats;
      if (!options.ombUrl) return;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        timer = null;
        void sync();
      }, debounceMs);
      timer.unref();
    },
    sync,
    async flush() {
      if (timer) {
        clearTimeout(timer);
        timer = null;
        return sync();
      }
      let last: SyncResult | null = null;
      while (inFlight) last = await inFlight;
      return last;
    },
    close() {
      if (timer) clearTimeout(timer);
      timer = null;
    },
  };
}
