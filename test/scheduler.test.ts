import { describe, expect, it } from "vitest";
import { JobSpec, type JobSpecInput } from "../src/protocol.ts";
import { jobRequiredTags, nodeTags, pickNode, rankNodes, type NodeCandidate } from "../src/scheduler.ts";
import { heartbeat } from "./helpers.ts";

const spec = (input: Partial<JobSpecInput> = {}) => JobSpec.parse({ prompt: "do it", cli: "shell", ...input });

const mac: NodeCandidate = {
  nodeId: "mac", online: true, freeSlots: 2,
  heartbeat: heartbeat({ nodeId: "mac", os: "darwin", tags: ["sdk:xcode", "sdk:node"], repos: [{ name: "app", path: "/p/app", remoteUrl: null, branch: "main" }] }),
};
const linux: NodeCandidate = {
  nodeId: "linux", online: true, freeSlots: 2,
  heartbeat: heartbeat({
    nodeId: "linux", os: "linux", arch: "x64", tags: ["sdk:docker", "sdk:android"],
    clis: { claude: { available: true, version: "1" }, codex: { available: true, version: "1" }, agy: { available: false, version: null }, hermes: { available: false, version: null } },
    resources: { freeDiskGb: 20, freeRamGb: 8, cpuCount: 4 },
  }),
};
const fleet = [mac, linux];

describe("capability matching", () => {
  it("routes each job to the only node whose tags satisfy it", () => {
    expect(pickNode(spec({ requiredTags: ["sdk:xcode"] }), fleet)).toBe("mac");
    expect(pickNode(spec({ requiredTags: ["sdk:android", "sdk:docker"] }), fleet)).toBe("linux");
    expect(pickNode(spec({ requiredTags: ["sdk:xcode", "sdk:docker"] }), fleet)).toBeNull();
  });

  it("derives os:, arch:, cli: and repo: tags from the heartbeat", () => {
    expect(nodeTags(linux.heartbeat)).toEqual(new Set(["sdk:docker", "sdk:android", "os:linux", "arch:x64", "cli:shell", "cli:claude", "cli:codex"]));
    expect(pickNode(spec({ requiredTags: ["os:linux"] }), fleet)).toBe("linux");
    expect(pickNode(spec({ cli: "codex" }), fleet)).toBe("linux");
    expect(pickNode(spec({ cli: "agy" }), fleet)).toBeNull();
  });

  it("only sends a repo job without repoUrl to a node that has the repo checked out", () => {
    expect(jobRequiredTags(spec({ repo: "app" }))).toContain("repo:app");
    expect(pickNode(spec({ repo: "app", preferredNodeId: "linux" }), fleet)).toBe("mac");
    expect(pickNode(spec({ repo: "app", repoUrl: "https://example.com/app.git", preferredNodeId: "linux" }), fleet)).toBe("linux");
  });

  it("enforces minDiskGb", () => {
    expect(rankNodes(spec({ minDiskGb: 50 }), fleet).map((n) => n.nodeId)).toEqual(["mac"]);
    expect(pickNode(spec({ minDiskGb: 500 }), fleet)).toBeNull();
  });

  it("skips offline nodes and nodes with no free slots", () => {
    expect(pickNode(spec(), [{ ...mac, online: false }, { ...linux, freeSlots: 0 }])).toBeNull();
    expect(pickNode(spec(), [{ ...mac, freeSlots: 0 }, linux])).toBe("linux");
  });

  it("gives preferredNodeId priority over more free slots", () => {
    expect(pickNode(spec(), [{ ...mac, freeSlots: 1 }, { ...linux, freeSlots: 4 }])).toBe("linux");
    expect(pickNode(spec({ preferredNodeId: "mac" }), [{ ...mac, freeSlots: 1 }, { ...linux, freeSlots: 4 }])).toBe("mac");
  });

  it("breaks ties by free slots, then free disk, then node id", () => {
    expect(rankNodes(spec(), [{ ...linux, freeSlots: 3 }, mac]).map((n) => n.nodeId)).toEqual(["linux", "mac"]);
    expect(rankNodes(spec(), [linux, mac]).map((n) => n.nodeId)).toEqual(["mac", "linux"]); // mac has 100 GB vs 20 GB
    const twin = { ...mac, nodeId: "aaa" };
    expect(rankNodes(spec(), [mac, twin]).map((n) => n.nodeId)).toEqual(["aaa", "mac"]);
  });
});
