import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

/** True when the module at `moduleUrl` is the process entry point (node src/x.ts). */
export function isMain(moduleUrl: string): boolean {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return realpathSync(entry) === realpathSync(fileURLToPath(moduleUrl));
  } catch {
    return false;
  }
}

export function intFromEnv(value: string | undefined, fallback: number): number {
  const parsed = value === undefined || value === "" ? Number.NaN : Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

export const errorMessage = (error: unknown) => (error instanceof Error ? error.message : String(error));
