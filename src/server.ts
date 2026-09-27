import { timingSafeEqual } from "node:crypto";
import { EventType, RunAgentInputSchema, type BaseEvent, type RunAgentInput } from "@ag-ui/core";
import { EventEncoder } from "@ag-ui/encoder";
import { loadConfig, type Config } from "./config";
import { createOmpResources, runOmpTurn, type OmpResources, type RunOutcome } from "./omp";
import { RunStore } from "./store";

const BODY_LIMIT = 1024 * 1024;
const HEARTBEAT_MS = 15_000;
const encoder = new EventEncoder();
const textEncoder = new TextEncoder();
const heartbeat = textEncoder.encode(": keep-alive\n\n");

type Owner = {
  controller: AbortController;
  task: Promise<void>;
  wake?: () => void;
};

function json(status: number, code: string, message: string, headers: HeadersInit = {}): Response {
  return Response.json({ error: { code, message } }, { status, headers });
}

function authorized(header: string | null, token: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  const candidate = Buffer.from(header.slice(7));
  const expected = Buffer.from(token);
  return candidate.length === expected.length && timingSafeEqual(candidate, expected);
}

function normalize(body: unknown): RunAgentInput | undefined {
  if (!body || typeof body !== "object" || Array.isArray(body)) return undefined;
  const candidate = { ...body } as Record<string, unknown>;
  if (!("tools" in candidate)) candidate.tools = [];
  if (!("context" in candidate)) candidate.context = [];
  if (!("state" in candidate)) candidate.state = {};
  if (!("forwardedProps" in candidate)) candidate.forwardedProps = {};
  const result = RunAgentInputSchema.safeParse(candidate);
  return result.success ? result.data : undefined;
}

/** Own one SQLite writer and every native session until shutdown has actually drained. */
export function startServer(config: Config, resources: OmpResources): { url: URL; close(): Promise<void> } {
  const store = new RunStore(config);
  const owners = new Map<string, Owner>();
  const readers = new Set<() => void>();
  let shuttingDown = false;
  let closing: Promise<void> | undefined;
  const token = config.token;
  function logFailure(label: string, error: unknown): void {
    const detail = error instanceof Error
      ? `${error.name}: ${error.message.replaceAll(token, "[redacted]").slice(0, 500)}`
      : "Unknown error";
    console.error(label, detail);
  }

  function fatal(error: unknown): void {
    logFailure("Journal failure; stopping adapter", error);
    void close();
  }

  function stream(request: Request, input: RunAgentInput, owner: Owner | undefined, cors: HeadersInit): Response {
    let cursor = 0;
    let finished = false;
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let waiter: (() => void) | undefined;
    const onAbort = () => stop(true);
    function wake(): void {
      if (timer) clearTimeout(timer);
      timer = undefined;
      if (owner?.wake === wake) owner.wake = undefined;
      const resolve = waiter;
      waiter = undefined;
      resolve?.();
    }
    function cleanup(): void {
      wake();
      readers.delete(closeReader);
      request.signal.removeEventListener("abort", onAbort);
    }
    function stop(abortOwner: boolean): void {
      if (finished) return;
      finished = true;
      cleanup();
      if (abortOwner) owner?.controller.abort();
      try { controller?.close(); } catch { /* A cancelled stream is already closed. */ }
    }
    function closeReader(): void { stop(false); }
    const body = new ReadableStream<Uint8Array>({
      start(current) {
        controller = current;
        readers.add(closeReader);
        request.signal.addEventListener("abort", onAbort, { once: true });
        if (request.signal.aborted) onAbort();
      },
      async pull(current) {
        while (!finished) {
          let row;
          try {
            row = store.readEvent(input.threadId, input.runId, cursor);
            if (!row) {
              // Subscribe first, then SELECT again: a commit between the first SELECT and
              // subscription cannot leave a waiting reader asleep forever.
              await new Promise<void>(resolve => {
                waiter = resolve;
                if (owner) owner.wake = wake;
                try { row = store.readEvent(input.threadId, input.runId, cursor); }
                catch (error) { fatal(error); wake(); return; }
                if (row) wake();
                else timer = setTimeout(wake, HEARTBEAT_MS);
              });
              if (finished) return;
              row ??= store.readEvent(input.threadId, input.runId, cursor);
              if (!row) {
                current.enqueue(heartbeat);
                return;
              }
            }
            cursor = row.seq;
            current.enqueue(textEncoder.encode(encoder.encodeSSE(row.event)));
            if (row.event.type === EventType.RUN_FINISHED || row.event.type === EventType.RUN_ERROR) stop(false);
            return;
          } catch (error) {
            fatal(error);
            finished = true;
            cleanup();
            current.error(new Error("Journal read failed."));
            return;
          }
        }
      },
      cancel() { stop(true); },
    }, { highWaterMark: 1 });
    return new Response(body, {
      headers: {
        ...cors,
        "Content-Type": `${encoder.getContentType()}; charset=utf-8`,
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      },
    });
  }

  let server: Bun.Server<unknown>;
  try {
    server = Bun.serve({
      hostname: config.hostname,
      port: config.port,
      maxRequestBodySize: BODY_LIMIT,
      development: false,
      async fetch(request, srv) {
        const url = new URL(request.url);
        if (url.pathname !== "/agent/run") return json(404, "NOT_FOUND", "Not found.");
        const origin = request.headers.get("Origin");
        if (origin && origin !== config.corsOrigin && origin !== url.origin) {
          return json(403, "ORIGIN_NOT_ALLOWED", "Origin is not allowed.");
        }
        const cors: Record<string, string> = origin
          ? { "Access-Control-Allow-Origin": origin, Vary: "Origin" } : {};
        if (request.method === "OPTIONS") {
          return new Response(null, { status: 204, headers: {
            ...cors, "Access-Control-Allow-Methods": "POST", "Access-Control-Allow-Headers": "Content-Type, Authorization",
          } });
        }
        if (request.method !== "POST") {
          return json(405, "METHOD_NOT_ALLOWED", "Method not allowed.", { ...cors, Allow: "POST, OPTIONS" });
        }
        if (shuttingDown) return json(503, "SHUTTING_DOWN", "Adapter is shutting down.", cors);
        if (!authorized(request.headers.get("Authorization"), token)) {
          return json(401, "UNAUTHORIZED", "Bearer token is required.", cors);
        }
        if (request.headers.get("Content-Type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
          return json(415, "UNSUPPORTED_MEDIA_TYPE", "JSON content is required.", cors);
        }
        const length = Number(request.headers.get("Content-Length"));
        if (length > BODY_LIMIT) return json(413, "PAYLOAD_TOO_LARGE", "Request body exceeds 1 MiB.", cors);
        let body: unknown;
        try {
          const bytes = await request.arrayBuffer();
          if (bytes.byteLength > BODY_LIMIT) return json(413, "PAYLOAD_TOO_LARGE", "Request body exceeds 1 MiB.", cors);
          body = JSON.parse(new TextDecoder().decode(bytes));
        } catch {
          if (shuttingDown) return json(503, "SHUTTING_DOWN", "Adapter is shutting down.", cors);
          return json(400, "INVALID_INPUT", "Request body must be valid JSON.", cors);
        }
        const input = normalize(body);
        if (!input) return json(400, "INVALID_INPUT", "Invalid AG-UI run input.", cors);
        // No await from this final shutdown check through admission, owner registration and scheduling.
        if (shuttingDown) return json(503, "SHUTTING_DOWN", "Adapter is shutting down.", cors);
        let admission;
        try { admission = store.admit(input); }
        catch (error) { fatal(error); return json(503, "SHUTTING_DOWN", "Adapter is shutting down.", cors); }
        if (admission.kind === "reject") return json(admission.status, admission.code, admission.message, cors);
        srv.timeout(request, 0);
        if (admission.kind === "replay") return stream(request, input, undefined, cors);
        const owner: Owner = { controller: new AbortController(), task: Promise.resolve() };
        const key = JSON.stringify([input.threadId, input.runId]);
        owners.set(key, owner);
        const response = stream(request, input, owner, cors);
        owner.task = Promise.resolve().then(async () => {
          let outcome: RunOutcome;
          try {
            outcome = await runOmpTurn(config, resources, input, store, owner.controller.signal, event => {
              try { store.append(input.threadId, input.runId, event); }
              catch (error) { fatal(error); throw error; }
              owner.wake?.();
            });
          } catch (error) {
            logFailure("Native run failed", error);
            outcome = { status: "error", code: "OMP_ERROR", message: "omp failed to complete the run." };
          }
          const terminal: BaseEvent = outcome.status === "success"
            ? { type: EventType.RUN_FINISHED, threadId: input.threadId, runId: input.runId }
            : outcome.status === "cancelled"
              ? { type: EventType.RUN_ERROR, code: "CANCELLED", message: "Run cancelled." }
              : { type: EventType.RUN_ERROR, code: outcome.code, message: outcome.message };
          try { store.append(input.threadId, input.runId, terminal); owner.wake?.(); }
          catch (error) { fatal(error); }
          finally { owners.delete(key); }
        });
        return response;
      },
      error(error) {
        logFailure("Server request failed", error);
        return json(500, "INTERNAL_ERROR", "Internal server error.");
      },
    });
  } catch (error) {
    store.close();
    throw error;
  }

  function close(): Promise<void> {
    if (closing) return closing;
    shuttingDown = true;
    const active = [...owners.values()];
    const stopping = server.stop(false);
    for (const owner of active) owner.controller.abort();
    closing = (async () => {
      await Promise.allSettled(active.map(owner => owner.task));
      for (const reader of [...readers]) reader();
      await server.stop(true);
      await stopping;
      store.close();
      resources.authStorage.close();
    })();
    return closing;
  }
  return { url: server.url, close };
}

if (import.meta.main) {
  const config = loadConfig();
  const resources = await createOmpResources(config);
  const app = startServer(config, resources);
  console.error(`AG-UI listening on ${app.url}`);
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.on(signal, () => { void app.close().then(() => process.exit(0), () => process.exit(1)); });
  }
}
