// Pure capability matcher: given a job and a fleet snapshot, which node should run it?
import { AGENT_CLIS, type JobSpec, type NodeHeartbeat } from "./protocol.ts";

export interface NodeCandidate {
  nodeId: string;
  online: boolean;
  freeSlots: number;
  heartbeat: NodeHeartbeat;
}

/** Explicit tags plus the ones every heartbeat implies: os:, arch:, cli: (available CLIs + shell), repo:. */
export function nodeTags(hb: NodeHeartbeat): Set<string> {
  const tags = new Set(hb.tags);
  tags.add(`os:${hb.os}`);
  tags.add(`arch:${hb.arch}`);
  tags.add("cli:shell");
  for (const cli of AGENT_CLIS) if (hb.clis[cli].available) tags.add(`cli:${cli}`);
  for (const repo of hb.repos) tags.add(`repo:${repo.name}`);
  return tags;
}

/** Tags a node must carry to run the job. A repo with no repoUrl can only run where it is already checked out. */
export function jobRequiredTags(spec: JobSpec): string[] {
  const tags = new Set(spec.requiredTags);
  tags.add(`cli:${spec.cli}`);
  if (spec.repo && !spec.repoUrl) tags.add(`repo:${spec.repo}`);
  return [...tags];
}

export function isEligible(spec: JobSpec, node: NodeCandidate): boolean {
  if (!node.online || node.freeSlots <= 0) return false;
  if (node.heartbeat.resources.freeDiskGb < spec.minDiskGb) return false;
  const have = nodeTags(node.heartbeat);
  return jobRequiredTags(spec).every((tag) => have.has(tag));
}

/** Eligible nodes, best first: preferred node, then most free slots, then most free disk, then nodeId for determinism. */
export function rankNodes(spec: JobSpec, nodes: readonly NodeCandidate[]): NodeCandidate[] {
  const preferred = (n: NodeCandidate) => (n.nodeId === spec.preferredNodeId ? 1 : 0);
  return nodes
    .filter((node) => isEligible(spec, node))
    .sort((a, b) =>
      preferred(b) - preferred(a)
      || b.freeSlots - a.freeSlots
      || b.heartbeat.resources.freeDiskGb - a.heartbeat.resources.freeDiskGb
      || a.nodeId.localeCompare(b.nodeId));
}

export function pickNode(spec: JobSpec, nodes: readonly NodeCandidate[]): string | null {
  return rankNodes(spec, nodes)[0]?.nodeId ?? null;
}
