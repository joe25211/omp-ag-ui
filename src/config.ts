import { chmodSync, mkdirSync, realpathSync, statSync } from "node:fs";
import { resolve } from "node:path";

export type Config = {
  hostname: string;
  port: number;
  token: string;
  corsOrigin?: string;
  cwd: string;
  sessionDir: string;
  provider?: string;
  modelId?: string;
  tools: string[];
};

const supportedTools: Record<string, true> = { read: true, grep: true, glob: true, edit: true, write: true, bash: true };
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const token = env.AGUI_TOKEN ?? "";
  if (!/^[\x21-\x7e]+$/.test(token)) throw new Error("AGUI_TOKEN must be nonblank and header-safe");
  const rawPort = env.AGUI_PORT ?? "8789";
  const port = Number(rawPort);
  if (!/^\d+$/.test(rawPort) || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("AGUI_PORT must be an integer from 1 to 65535");
  }
  let corsOrigin: string | undefined;
  if (env.AGUI_CORS_ORIGIN !== undefined) {
    const url = new URL(env.AGUI_CORS_ORIGIN);
    if (!(["http:", "https:"].includes(url.protocol)) || url.username || url.password || url.href !== `${url.origin}/`) {
      throw new Error("AGUI_CORS_ORIGIN must be one exact HTTP(S) origin");
    }
    corsOrigin = url.origin;
  }
  const cwd = realpathSync(resolve(env.OMP_CWD ?? process.cwd()));
  if (!statSync(cwd).isDirectory()) throw new Error("OMP_CWD must be an existing directory");
  const sessionDir = resolve(env.OMP_SESSION_DIR ?? resolve(process.cwd(), ".omp-ag-ui"));
  mkdirSync(sessionDir, { recursive: true, mode: 0o700 });
  chmodSync(sessionDir, 0o700);
  const provider = env.OMP_PROVIDER?.trim() || undefined;
  const modelId = env.OMP_MODEL?.trim() || undefined;
  if (!!provider !== !!modelId) throw new Error("OMP_PROVIDER and OMP_MODEL must be set together");
  const tools = [...new Set((env.OMP_TOOLS ?? "read,grep,glob").split(",").map(name => name.trim()).filter(Boolean))];
  for (const name of tools) if (!supportedTools[name]) throw new Error(`Unsupported OMP_TOOLS name: ${name}`);
  return { hostname: env.AGUI_HOST ?? "127.0.0.1", port, token, corsOrigin, cwd, sessionDir, provider, modelId, tools };
}
