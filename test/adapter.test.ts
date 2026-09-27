import { afterEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HttpAgent, type AgentSubscriber, type RunAgentResult } from "@ag-ui/client";
import { EventType as E, type BaseEvent, type RunAgentInput } from "@ag-ui/core";
import { AuthStorage, ModelRegistry, SessionManager } from "@oh-my-pi/pi-coding-agent";
import { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { createAssistantMessageEventStream, type AssistantMessageEventStream, type AssistantMessage, type Context, type Model, type SimpleStreamOptions } from "@oh-my-pi/pi-ai";
import { unregisterCustomApis } from "@oh-my-pi/pi-ai/api-registry";
import { startServer } from "../src/server";
import { RunStore } from "../src/store";
import type { Config } from "../src/config";

const SOURCE = "omp-agui-tests";
type Stream = AssistantMessageEventStream;
type Provider = (stream: Stream, model: Model, context: Context, signal: AbortSignal | undefined) => Promise<void> | void;
type WireEvent = BaseEvent & { messageId?: string; toolCallId?: string; parentMessageId?: string; toolCallName?: string; delta?: string; content?: string; code?: string };
const cleanups: Array<() => Promise<void>> = [];
type ClientRun = { agent: HttpAgent; events: WireEvent[]; promise: Promise<RunAgentResult> };
type Fixture = {
  root: string;
  config: Config;
  resources: { model: Model; modelRegistry: ModelRegistry };
  readonly calls: number;
  readonly url: string;
  setProvider(provider: Provider): void;
  close(): Promise<void>;
  restart(): Promise<void>;
  request(value: unknown, init?: RequestInit): Promise<Response>;
  run(value: RunAgentInput, subscriber?: AgentSubscriber): ClientRun;
};

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function bounded<T>(promise: Promise<T>, milliseconds = 10_000): Promise<T> {
  // Real socket/SDK deadlines bound a hung integration; they never schedule expected behavior.
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out after ${milliseconds}ms`)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}
async function until(predicate: () => boolean | Promise<boolean>, milliseconds = 5_000) {
  const deadline = Date.now() + milliseconds;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error("Condition did not become true");
    await Bun.sleep(10);
  }
}
function assistant(model: Model, content: AssistantMessage["content"], stopReason: AssistantMessage["stopReason"] = "stop"): AssistantMessage {
  return { role: "assistant", api: model.api, provider: model.provider, model: model.id, content, stopReason, timestamp: Date.now(),
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
}
function finish(stream: Stream, message: AssistantMessage) {
  if (message.content.some(block => block.type === "toolCall")) {
    stream.push({ type: "start", partial: structuredClone(message) });
    for (const [contentIndex, block] of message.content.entries()) {
      if (block.type !== "toolCall") continue;
      stream.push({ type: "toolcall_start", contentIndex, partial: structuredClone(message) });
      stream.push({ type: "toolcall_end", contentIndex, toolCall: block, partial: structuredClone(message) });
    }
  }
  if (message.stopReason === "aborted" || message.stopReason === "error") {
    stream.push({ type: "error", reason: message.stopReason, error: message });
  } else stream.push({ type: "done", reason: message.stopReason, message });
  stream.end();
}
function delta(stream: Stream, message: AssistantMessage, text: string) {
  const block = message.content[0];
  if (!block || block.type !== "text") throw new Error("Expected text block");
  block.text += text;
  stream.push({ type: "text_delta", contentIndex: 0, delta: text, partial: structuredClone(message) });
}
const answer: Provider = (stream, model) => finish(stream, assistant(model, [{ type: "text", text: "answer" }]));
function input(text = "hello", threadId: string = crypto.randomUUID()): RunAgentInput {
  return { threadId, runId: crypto.randomUUID(), messages: [{ id: crypto.randomUUID(), role: "user", content: text }], tools: [], context: [], state: {}, forwardedProps: {} };
}
async function fixture(provider: Provider = answer, tools: string[] = []): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "omp-agui-test-"));
  const cwd = join(root, "work");
  await mkdir(cwd, { mode: 0o700 });
  const config: Config = { hostname: "127.0.0.1", port: 0, token: crypto.randomUUID(), cwd, sessionDir: join(root, "sessions"), tools, corsOrigin: "https://allowed.example" };
  let calls = 0;
  let app: { url: URL; close(): Promise<void> } | undefined;
  async function start() {
    const authStorage = await AuthStorage.create(":memory:");
    const modelRegistry = new ModelRegistry(authStorage, undefined, { ignoreLocalModelConfig: true, cacheDbPath: join(root, "models.sqlite") });
    modelRegistry.registerProvider("agui-test", { api: "agui-test-stream", baseUrl: "http://127.0.0.1", apiKey: "test",
      streamSimple(model: Model, context: Context, options?: SimpleStreamOptions) {
        calls++;
        const stream = createAssistantMessageEventStream();
        void Promise.resolve().then(() => provider(stream, model, context, options?.signal)).catch(error => {
          const message = assistant(model, [], "error");
          message.errorMessage = String(error);
          finish(stream, message);
        });
        return stream;
      },
      models: [{ id: "test", name: "Test", reasoning: false, input: ["text"], supportsTools: true, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024 }],
    }, SOURCE);
    const model = modelRegistry.find("agui-test", "test");
    if (!model) throw new Error("Test model registration failed");
    app = startServer(config, { authStorage, modelRegistry, model });
    return { model, modelRegistry };
  }
  const resources = await start();
  const f: Fixture = {
    root, config, resources,
    get calls() { return calls; },
    get url() { return new URL("agent/run", app!.url).href; },
    setProvider(next: Provider) { provider = next; },
    async close() { await app?.close(); },
    async restart() { await app?.close(); await start(); },
    request(value: unknown, init: RequestInit = {}) {
      return fetch(f.url, { method: "POST", headers: { Authorization: `Bearer ${config.token}`, "Content-Type": "application/json" }, body: JSON.stringify(value), ...init });
    },
    run(value: RunAgentInput, subscriber: AgentSubscriber = {}) {
      const events: WireEvent[] = [];
      const agent = new HttpAgent({ url: f.url, headers: { Authorization: `Bearer ${config.token}` }, threadId: value.threadId, initialMessages: structuredClone(value.messages), initialState: value.state });
      const promise = agent.runAgent({ runId: value.runId, tools: value.tools, context: value.context, forwardedProps: value.forwardedProps }, {
        ...subscriber,
        onEvent(params) { events.push(structuredClone(params.event)); return subscriber.onEvent?.(params); },
      });
      // Keep intentionally cancelled clients from causing unhandled-rejection noise.
      void promise.catch(() => {});
      return { agent, events, promise };
    },
  };
  cleanups.push(async () => { await f.close(); unregisterCustomApis(SOURCE); await rm(root, { recursive: true, force: true }); });
  return f;
}
function terminal(events: WireEvent[]) {
  const endings = events.filter(event => event.type === E.RUN_FINISHED || event.type === E.RUN_ERROR);
  expect(endings).toHaveLength(1);
  expect(events.at(-1)).toEqual(endings[0]);
  return endings[0]!;
}
async function completed(f: Fixture, value: RunAgentInput) {
  const run = f.run(value);
  await bounded(run.promise.catch(error => { if (!run.events.some(event => event.type === E.RUN_ERROR)) throw error; }));
  terminal(run.events);
  return run;
}
async function replayAfterAbort(f: Fixture, value: RunAgentInput) {
  const deadline = Date.now() + 5_000;
  while (true) {
    const response = await f.request(value);
    if (response.status !== 409) {
      expect(response.status).toBe(200);
      await response.body?.cancel();
      return completed(f, value);
    }
    const body = await response.json() as { error: { code: string } };
    expect(["RUN_ACTIVE", "THREAD_BUSY"]).toContain(body.error.code);
    if (Date.now() >= deadline) throw new Error("Cancelled run failed to drain within five seconds");
    await Bun.sleep(10);
  }
}
async function expectRejected(f: Fixture, value: unknown, status: number, code: string, init?: RequestInit) {
  const response = await f.request(value, init);
  expect(response.status).toBe(status);
  expect(await response.json()).toMatchObject({ error: { code } });
}
function text(events: WireEvent[]) { return events.filter(event => event.type === E.TEXT_MESSAGE_CONTENT).map(event => event.delta).join(""); }
function userContents(context: Context) {
  return context.messages.filter(message => message.role === "user").map(message => typeof message.content === "string" ? message.content : message.content.filter(block => block.type === "text").map(block => block.text).join(""));
}
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

describe.serial("real SDK HTTP adapter", () => {
  test("delivers incremental text before completion and one complete-only lifecycle", async () => {
    const release = gate();
    const received = gate();
    const f = await fixture(async (stream, model) => {
      const message = assistant(model, [{ type: "text", text: "" }]);
      stream.push({ type: "start", partial: message });
      message.content.push({ type: "thinking", thinking: "private-reasoning-must-not-leak" });
      stream.push({ type: "thinking_delta", contentIndex: 1, delta: "private-reasoning-must-not-leak", partial: structuredClone(message) });
      delta(stream, message, "alpha");
      await release.promise;
      delta(stream, message, " beta");
      finish(stream, message);
    });
    const run = f.run(input(), { onTextMessageContentEvent({ event }) { if (event.delta === "alpha") received.resolve(); } });
    let settled = false;
    void run.promise.then(() => { settled = true; }, () => { settled = true; });
    try { await bounded(received.promise); expect(settled).toBe(false); expect(text(run.events)).toBe("alpha"); }
    finally { release.resolve(); }
    await bounded(run.promise);
    expect(text(run.events)).toBe("alpha beta");
    expect(terminal(run.events).type).toBe(E.RUN_FINISHED);
    expect(run.events.some(event => event.type.includes("REASONING"))).toBe(false);
    expect(JSON.stringify(run.events)).not.toContain("private-reasoning-must-not-leak");
    f.setProvider((stream, model) => finish(stream, assistant(model, [{ type: "text", text: "complete-only" }])));
    const complete = await completed(f, input());
    const lifecycle = complete.events.filter(event => event.type.startsWith("TEXT_MESSAGE"));
    expect(lifecycle.map(event => event.type)).toEqual([E.TEXT_MESSAGE_START, E.TEXT_MESSAGE_CONTENT, E.TEXT_MESSAGE_END]);
    expect(new Set(lifecycle.map(event => event.messageId)).size).toBe(1);
    expect(complete.agent.messages.filter(message => message.role === "assistant").map(message => message.content)).toEqual(["complete-only"]);
  }, 30_000);

  test("pairs real read success and failure, keeps tool-only parents, and waits for recovery", async () => {
    const nonce = crypto.randomUUID();
    let path = "";
    const f = await fixture((stream, model, context) => {
      const results = context.messages.filter(message => message.role === "toolResult");
      if (results.length === 0) finish(stream, assistant(model, [{ type: "toolCall", id: "first", name: "read", arguments: { path } }], "toolUse"));
      else if (results.length === 1) finish(stream, assistant(model, [{ type: "text", text: "try missing" }, { type: "toolCall", id: "missing", name: "read", arguments: { path: `${path}.missing` } }], "toolUse"));
      else finish(stream, assistant(model, [{ type: "text", text: "recovered" }]));
    }, ["read"]);
    path = join(f.config.cwd, "fixture.txt");
    await writeFile(path, nonce);
    const run = await completed(f, input());
    expect(f.calls).toBe(3);
    expect(terminal(run.events).type).toBe(E.RUN_FINISHED);
    for (const id of ["first", "missing"]) {
      const events = run.events.filter(event => event.toolCallId?.endsWith(`:${id}`));
      expect(events.map(event => event.type)).toEqual([E.TOOL_CALL_START, E.TOOL_CALL_ARGS, E.TOOL_CALL_END, E.TOOL_CALL_RESULT]);
      expect(JSON.parse(events[1]!.delta!).path).toBe(id === "first" ? path : `${path}.missing`);
      expect(events[0]!.toolCallName).toBe("read");
      expect(run.agent.messages.some(message => message.id === events[0]!.parentMessageId && message.role === "assistant" && message.toolCalls?.some(call => call.id === events[0]!.toolCallId))).toBe(true);
      expect(events[3]!.content).toContain(id === "first" ? nonce : "Error: ");
    }
    expect(text(run.events)).toBe("try missingrecovered");
    expect(run.events.findIndex(event => event.delta === "recovered")).toBeGreaterThan(run.events.findLastIndex(event => event.type === E.TOOL_CALL_RESULT));
  }, 30_000);
});

describe.serial("cancellation and persisted ownership", () => {
  test("client abort reaches provider before terminal replay and releases the thread", async () => {
    const aborted = gate();
    const f = await fixture(async (stream, model, _context, signal) => {
      const message = assistant(model, [{ type: "text", text: "" }]);
      stream.push({ type: "start", partial: message });
      delta(stream, message, "first");
      await new Promise<void>(resolve => {
        if (signal?.aborted) resolve();
        else signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      aborted.resolve();
      finish(stream, { ...message, stopReason: "aborted" });
    });
    const original = input();
    const run = f.run(original, { onTextMessageContentEvent({ agent }) { agent.abortRun(); } });
    await bounded(aborted.promise);
    await run.promise.catch(() => {});
    const replay = await replayAfterAbort(f, original);
    expect(terminal(replay.events).code).toBe("CANCELLED");
    expect(text(replay.events)).toBe("first");
    expect(f.calls).toBe(1);
    f.setProvider(answer);
    expect(terminal((await completed(f, input("new turn", original.threadId))).events).type).toBe(E.RUN_FINISHED);
  }, 30_000);

  for (const stage of ["construction", "pre-prompt flush", "before native marker"] as const) {
    test(`cancel during ${stage} never enters provider and preserves accepted user`, async () => {
      const entered = gate();
      const release = gate();
      const f = await fixture();
      const original = input(`accepted-${stage}`);
      const originalHydrate = ModelRegistry.prototype.hydrateCredentialScopedModelCaches;
      const originalFlush = SessionManager.prototype.flush;
      const originalOpen = SessionManager.open;
      let held = false;
      if (stage === "construction") {
        ModelRegistry.prototype.hydrateCredentialScopedModelCaches = async function (this: ModelRegistry, ...args) {
          if (!held) { held = true; entered.resolve(); await release.promise; }
          return originalHydrate.apply(this, args);
        };
      } else if (stage === "pre-prompt flush") {
        SessionManager.prototype.flush = async function (this: SessionManager, ...args) {
          const marker = this.getBranch().findLast(entry => entry.type === "custom" && entry.customType === "agui.run");
          if (!held && marker) { held = true; entered.resolve(); await release.promise; }
          return originalFlush.apply(this, args);
        };
      } else {
        SessionManager.open = async function (this: typeof SessionManager, ...args) {
          if (!held) { held = true; entered.resolve(); await release.promise; }
          return originalOpen.apply(this, args);
        };
      }
      const run = f.run(original);
      try {
        await bounded(entered.promise);
        run.agent.abortRun();
        await run.promise.catch(() => {});
      } finally {
        release.resolve();
        ModelRegistry.prototype.hydrateCredentialScopedModelCaches = originalHydrate;
        SessionManager.prototype.flush = originalFlush;
        SessionManager.open = originalOpen;
      }
      expect(terminal((await replayAfterAbort(f, original)).events).code).toBe("CANCELLED");
      expect(f.calls).toBe(0);
      await f.restart();
      let seen: string[] = [];
      f.setProvider((stream, model, context) => { seen = userContents(context); answer(stream, model, context, undefined); });
      const next = input("after cancellation", original.threadId);
      expect(terminal((await completed(f, next)).events).type).toBe(E.RUN_FINISHED);
      expect(seen.filter(value => value.includes(`accepted-${stage}`))).toHaveLength(1);
    }, 30_000);
  }

  for (const shutdown of [false, true]) {
    test(`${shutdown ? "shutdown" : "client abort"} kills an actual bash child before releasing ownership`, async () => {
      let pid: number | undefined;
      const f = await fixture((stream, model, context) => {
        if (context.messages.some(message => message.role === "toolResult")) return answer(stream, model, context, undefined);
        finish(stream, assistant(model, [{ type: "toolCall", id: "sleep", name: "bash", arguments: {
          command: "bash -c 'echo $$ > cancel.pid; exec sleep 60'",
        } }], "toolUse"));
      }, ["bash"]);
      const original = input();
      const run = f.run(original);
      try {
        // This platform integration polls the OS-owned PID file, not a guessed startup delay.
        await until(async () => {
          try { pid = Number(await readFile(join(f.config.cwd, "cancel.pid"), "utf8")); return Number.isInteger(pid) && pid! > 1; }
          catch { return false; }
        });
        const stoppedAt = Date.now();
        if (shutdown) await bounded(f.close(), 5_000);
        else run.agent.abortRun();
        await run.promise.catch(() => {});
        if (shutdown) await f.restart();
        const replay = await replayAfterAbort(f, original);
        expect(terminal(replay.events).code).toBe("CANCELLED");
        let code: unknown;
        try { process.kill(pid!, 0); } catch (error) { code = (error as NodeJS.ErrnoException).code; }
        expect(code).toBe("ESRCH");
        pid = undefined;
        expect(Date.now() - stoppedAt).toBeLessThan(5_000);
        const results = replay.events.filter(event => event.type === E.TOOL_CALL_RESULT);
        expect(results).toHaveLength(1);
        expect(results[0]!.content).toMatch(/Error:|cancel|abort/i);
        f.setProvider(answer);
        expect(terminal((await completed(f, input("after bash", original.threadId))).events).type).toBe(E.RUN_FINISHED);
      } finally {
        if (pid) { try { process.kill(pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } }
      }
    }, 30_000);
  }

  test("restart preserves native user/assistant/tool context once and imports history without executing tools", async () => {
    let snapshots: Context[] = [];
    const nonce = crypto.randomUUID();
    const f = await fixture((stream, model, context) => {
      // Provider-facing tool schemas carry non-cloneable internals; the assertions consume messages only.
      snapshots.push({ messages: structuredClone(context.messages) });
      if (!context.messages.some(message => message.role === "toolResult")) {
        finish(stream, assistant(model, [{ type: "toolCall", id: "read", name: "read", arguments: { path: "nonce.txt" } }], "toolUse"));
      } else finish(stream, assistant(model, [{ type: "text", text: nonce }]));
    }, ["read"]);
    await writeFile(join(f.config.cwd, "nonce.txt"), nonce);
    const first = input(`remember ${nonce}`);
    await completed(f, first);
    await f.restart();
    snapshots = [];
    const next = input("recall", first.threadId);
    const recalled = await completed(f, next);
    expect(terminal(recalled.events).type).toBe(E.RUN_FINISHED);
    const context = snapshots[0]!;
    expect(userContents(context).filter(value => value.includes(nonce))).toHaveLength(1);
    expect(context.messages.filter(message => message.role === "toolResult")).toHaveLength(1);
    expect(context.messages.filter(message => message.role === "assistant" && message.content.some(block => block.type === "text" && block.text === nonce))).toHaveLength(1);
    const full = input("full continuation", first.threadId);
    full.messages = [...first.messages, ...next.messages, ...full.messages];
    await completed(f, full);
    await expectRejected(f, { ...input("reused", first.threadId), messages: first.messages }, 409, "DUPLICATE_USER_MESSAGE");
    const changed = input("append", first.threadId);
    if (first.messages[0]?.role !== "user") throw new Error("Expected first user message");
    changed.messages = [{ ...first.messages[0]!, content: "edited" }, ...next.messages, ...full.messages.slice(-1), ...changed.messages];
    await expectRejected(f, changed, 409, "HISTORY_CONFLICT");
    const imported = input("import followup");
    imported.messages.unshift(
      { id: "old-user", role: "user", content: "historic user" },
      { id: "old-assistant", role: "assistant", content: "historic assistant", toolCalls: [{ id: "historic-call", type: "function", function: { name: "read", arguments: '{"path":"must-not-execute"}' } }] },
      { id: "old-result", role: "tool", toolCallId: "historic-call", content: "historic result" },
    );
    snapshots = [];
    const importedRun = await completed(f, imported);
    expect(importedRun.events.filter(event => event.type === E.TOOL_CALL_START)).toEqual([]);
    expect(snapshots[0]!.messages.filter(message => message.role === "toolResult")).toMatchObject([{ toolCallId: "historic-call", content: [{ type: "text", text: "historic result" }] }]);
  }, 30_000);

  test("replay ignores renewed opaque credentials and mutable cwd/tools without stealing another owner", async () => {
    const f = await fixture();
    const first = input("first");
    first.tools = [{ name: "read", description: "frontend", parameters: {} }];
    first.forwardedProps = { openbotRun: "old" };
    const baseline = await completed(f, first);
    await completed(f, input("second", first.threadId));
    const entered = gate();
    const release = gate();
    let ownerAborted = false;
    f.setProvider(async (stream, model, _context, signal) => {
      signal?.addEventListener("abort", () => { ownerAborted = true; release.resolve(); }, { once: true });
      entered.resolve();
      await release.promise;
      finish(stream, assistant(model, [{ type: "text", text: "third" }]));
    });
    const third = input("third", first.threadId);
    const active = f.run(third);
    try {
      await bounded(entered.promise);
      const calls = f.calls;
      expect((await completed(f, { ...first, forwardedProps: { openbotRun: "renewed" } })).events).toEqual(baseline.events);
      await expectRejected(f, { ...third, forwardedProps: { openbotRun: "renewed" } }, 409, "RUN_ACTIVE");
      await expectRejected(f, { ...first, messages: input("different").messages }, 409, "RUN_INPUT_CONFLICT");
      await expectRejected(f, input("busy", first.threadId), 409, "THREAD_BUSY");
      const replay = await f.request(first);
      await replay.body?.cancel();
      expect(ownerAborted).toBe(false);
      expect(f.calls).toBe(calls);
    } finally { release.resolve(); }
    await bounded(active.promise);
    expect((await completed(f, first)).events).toEqual(baseline.events);
    f.config.cwd = join(f.root, "other-work");
    await mkdir(f.config.cwd);
    f.config.tools = ["read"];
    await f.restart();
    const calls = f.calls;
    expect((await completed(f, first)).events).toEqual(baseline.events);
    expect(f.calls).toBe(calls);
    await expectRejected(f, input("changed cwd", first.threadId), 409, "THREAD_CWD_CHANGED");
    const collision = input();
    collision.tools = first.tools;
    await expectRejected(f, collision, 422, "TOOL_NAME_CONFLICT");
    f.setProvider(answer);
    const isolated = await completed(f, input("isolated thread"));
    expect(terminal(isolated.events).type).toBe(E.RUN_FINISHED);
  }, 30_000);
});

describe.serial("crash recovery and HTTP safety", () => {
  test("startup closes uncertain journal lifecycles once and repairs a native tool tail without execution", async () => {
    const f = await fixture(answer, ["read"]);
    await f.close();
    const original = input("accepted before crash");
    const store = new RunStore(f.config);
    expect(store.admit(original)).toEqual({ kind: "run" });
    const file = store.thread(original.threadId).sessionFile;
    const manager = await SessionManager.open(file, undefined, undefined, { initialCwd: f.config.cwd, suppressBreadcrumb: true });
    await manager.appendEntriesAtomically(() => {
      manager.appendCustomEntry("agui.bootstrap", { threadId: original.threadId });
      manager.appendCustomEntry("agui.run", { runId: original.runId, userMessageId: original.messages[0]!.id, text: "accepted before crash" });
      manager.appendMessage({ role: "user", content: [{ type: "text", text: "accepted before crash" }], timestamp: Date.now() });
      manager.appendMessage(assistant(f.resources.model, [{ type: "toolCall", id: "dangling", name: "read", arguments: { path: "must-not-read" } }], "toolUse"));
    });
    await manager.close();
    store.markNativeReady(original.threadId);
    store.append(original.threadId, original.runId, { type: E.TEXT_MESSAGE_START, messageId: "open-text", role: "assistant" });
    store.append(original.threadId, original.runId, { type: E.TEXT_MESSAGE_CONTENT, messageId: "open-text", delta: "partial" });
    for (const id of ["open-call", "ended-call", "result-call"]) {
      store.append(original.threadId, original.runId, { type: E.TOOL_CALL_START, toolCallId: id, toolCallName: "read", parentMessageId: "open-text" });
      store.append(original.threadId, original.runId, { type: E.TOOL_CALL_ARGS, toolCallId: id, delta: '{"path":"unknown"}' });
      if (id !== "open-call") store.append(original.threadId, original.runId, { type: E.TOOL_CALL_END, toolCallId: id });
      if (id === "result-call") store.append(original.threadId, original.runId, { type: E.TOOL_CALL_RESULT, toolCallId: id, messageId: "known-result", role: "tool", content: "known" });
    }
    store.close();
    await f.restart();
    const recovered = await completed(f, original);
    expect(terminal(recovered.events).code).toBe("SERVER_RESTARTED");
    expect(recovered.events.filter(event => event.type === E.TEXT_MESSAGE_END)).toHaveLength(1);
    for (const id of ["open-call", "ended-call", "result-call"]) {
      expect(recovered.events.filter(event => event.type === E.TOOL_CALL_END && event.toolCallId === id)).toHaveLength(1);
      const results = recovered.events.filter(event => event.type === E.TOOL_CALL_RESULT && event.toolCallId === id);
      expect(results).toHaveLength(1);
      if (id === "result-call") expect(results[0]!.content).toBe("known");
      else expect(results[0]!.content).toContain("execution outcome unknown; do not retry automatically");
    }
    expect(recovered.agent.messages.filter(message => message.role === "tool").map(message => message.content).join("\n")).toContain("execution outcome unknown");
    await f.restart();
    expect((await completed(f, original)).events).toEqual(recovered.events);
    expect(f.calls).toBe(0);
    let observed: Context | undefined;
    f.setProvider((stream, model, context) => {
      // Schema objects in `context.tools` are not structured-cloneable; messages are.
      observed = { messages: structuredClone(context.messages) };
      answer(stream, model, context, undefined);
    });
    const next = await completed(f, input("new explicit turn", original.threadId));
    expect(next.events.filter(event => event.type === E.TOOL_CALL_START)).toEqual([]);
    expect(terminal(next.events).type).toBe(E.RUN_FINISHED);
    expect(observed!.messages.filter(message => message.role === "toolResult")).toMatchObject([
      { toolCallId: "dangling", isError: true, content: [{ type: "text", text: "Adapter restarted or stopped before completion was recorded; execution outcome unknown; do not retry automatically." }] },
    ]);
    expect(f.calls).toBe(1);
  }, 30_000);

  for (const mode of ["durable bootstrap", "missing marker", "missing transcript", "corrupt transcript"] as const) {
    test(`bootstrap recovery fails closed appropriately: ${mode}`, async () => {
      const f = await fixture();
      await f.close();
      const original = input("initial final user");
      original.messages.unshift({ id: "import-user", role: "user", content: "imported exactly once" });
      const store = new RunStore(f.config);
      expect(store.admit(original)).toEqual({ kind: "run" });
      const file = store.thread(original.threadId).sessionFile;
      if (mode !== "missing transcript" && mode !== "corrupt transcript") {
        const manager = await SessionManager.open(file, undefined, undefined, { initialCwd: f.config.cwd, suppressBreadcrumb: true });
        await manager.appendEntriesAtomically(() => {
          manager.appendMessage({ role: "user", content: [{ type: "text", text: "imported exactly once" }], timestamp: Date.now() });
          if (mode === "durable bootstrap") manager.appendCustomEntry("agui.bootstrap", { threadId: original.threadId });
        });
        await manager.close();
      }
      if (mode !== "durable bootstrap") store.markNativeReady(original.threadId);
      if (mode === "corrupt transcript") await writeFile(file, "this is not a session\n");
      store.close();
      await f.restart();
      let seen: string[] = [];
      f.setProvider((stream, model, context) => { seen = userContents(context); answer(stream, model, context, undefined); });
      const next = await completed(f, input("next accepted", original.threadId));
      if (mode === "durable bootstrap") {
        expect(terminal(next.events).type).toBe(E.RUN_FINISHED);
        expect(seen.filter(value => value.includes("imported exactly once"))).toHaveLength(1);
        expect(seen.filter(value => value.includes("initial final user"))).toHaveLength(1);
      } else {
        expect(terminal(next.events).code).toBe("SESSION_ERROR");
        expect(f.calls).toBe(0);
      }
    }, 30_000);
  }

  test("a second process cannot become the journal writer", async () => {
    const f = await fixture();
    const child = Bun.spawn([process.execPath, "--eval", `import { RunStore } from ${JSON.stringify(join(import.meta.dir, "../src/store.ts"))}; try { const s = new RunStore(JSON.parse(process.argv[1])); s.close(); process.exit(7); } catch { process.exit(0); }`, JSON.stringify(f.config)], { stdout: "pipe", stderr: "pipe" });
    expect(await bounded(child.exited)).toBe(0);
    expect(terminal((await completed(f, input())).events).type).toBe(E.RUN_FINISHED);
  }, 30_000);

  test("rejects unsafe HTTP inputs before invoking any provider", async () => {
    const f = await fixture(answer, ["read"]);
    await expectRejected(f, input(), 401, "UNAUTHORIZED", { headers: { Authorization: "Bearer wrong", "Content-Type": "application/json" } });
    await expectRejected(f, null, 400, "INVALID_INPUT");
    await expectRejected(f, input(), 400, "INVALID_INPUT", { body: "{" });
    await expectRejected(f, input(), 415, "UNSUPPORTED_MEDIA_TYPE", { headers: { Authorization: `Bearer ${f.config.token}`, "Content-Type": "text/plain" } });
    // Bun rejects a body over maxRequestBodySize before the handler runs, so this 413 has no
    // adapter JSON body to assert; the adapter's PAYLOAD_TOO_LARGE shape is unreachable at the bound.
    const oversized = await f.request(input(), { body: JSON.stringify({ padding: "x".repeat(1024 * 1024) }) });
    expect(oversized.status).toBe(413);
    await oversized.body?.cancel().catch(() => {});
    await expectRejected(f, input(), 403, "ORIGIN_NOT_ALLOWED", { headers: { Authorization: `Bearer ${f.config.token}`, "Content-Type": "application/json", Origin: "https://denied.example" } });
    const preflight = await fetch(f.url, { method: "OPTIONS", headers: { Origin: f.config.corsOrigin! } });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("Access-Control-Allow-Origin")).toBe(f.config.corsOrigin!);
    expect(preflight.headers.get("Access-Control-Allow-Headers")).toBe("Content-Type, Authorization");
    const allowedError = await fetch(f.url, { method: "POST", headers: { Origin: f.config.corsOrigin! } });
    expect(allowedError.status).toBe(401);
    expect(allowedError.headers.get("Vary")).toBe("Origin");
    expect(allowedError.headers.get("Access-Control-Allow-Origin")).toBe(f.config.corsOrigin!);
    const collision = input();
    collision.tools = [{ name: "read", description: "frontend", parameters: {} }];
    await expectRejected(f, collision, 422, "TOOL_NAME_CONFLICT");
    const omittedDefaults = input();
    await expectRejected(f, { ...omittedDefaults, tools: null }, 400, "INVALID_INPUT");
    await expectRejected(f, { ...omittedDefaults, context: null }, 400, "INVALID_INPUT");
    await expectRejected(f, { ...omittedDefaults, threadId: "x".repeat(257) }, 422, "INVALID_INPUT");
    await expectRejected(f, { ...input(), resume: [{ interruptId: "unsupported", status: "resolved" }] }, 422, "UNSUPPORTED_RESUME");
    for (const messages of [
      [{ id: "orphan", role: "tool", toolCallId: "absent", content: "orphan" }],
      [{ id: "bad-args", role: "assistant", toolCalls: [{ id: "call", type: "function", function: { name: "read", arguments: "[]" } }] }],
      [{ id: "unresolved", role: "assistant", toolCalls: [{ id: "call", type: "function", function: { name: "read", arguments: "{}" } }] }],
    ]) {
      const invalid = input();
      await expectRejected(f, { ...invalid, messages: [...messages, ...invalid.messages] }, 422, "INVALID_HISTORY");
    }
    await expectRejected(f, input("   "), 422, "TEXT_REQUIRED");
    const duplicate = input();
    await expectRejected(f, { ...duplicate, messages: [...duplicate.messages, ...duplicate.messages] }, 422, "INVALID_INPUT");
    expect(f.calls).toBe(0);
  }, 30_000);

  test("blocked frontend hallucinations expose no executable calls and resolved provider failures end once", async () => {
    const f = await fixture((stream, model) => finish(stream, assistant(model, [{ type: "toolCall", id: "bad", name: "computer_read", arguments: {} }], "toolUse")));
    const hallucinated = await completed(f, input());
    expect(terminal(hallucinated.events).code).toBe("TOOL_NOT_ALLOWED");
    expect(hallucinated.events.filter(event => event.type.startsWith("TOOL_CALL"))).toEqual([]);
    for (const stopReason of ["error", "length"] as const) {
      f.setProvider((stream, model) => finish(stream, assistant(model, [{ type: "text", text: "partial" }], stopReason)));
      const failure = await completed(f, input());
      expect(terminal(failure.events).code).toBe(stopReason === "error" ? "OMP_ERROR" : "OUTPUT_LIMIT");
      expect(failure.events.some(event => event.type === E.RUN_FINISHED)).toBe(false);
    }
  }, 30_000);

  test("quiet native work outlives Bun idle timeout and emits a real heartbeat", async () => {
    const release = gate();
    const f = await fixture(async (stream, model, _context, signal) => {
      signal?.addEventListener("abort", release.resolve, { once: true });
      await release.promise;
      finish(stream, assistant(model, [{ type: "text", text: "after heartbeat" }]));
    });
    const response = await f.request(input());
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let bytes = "";
    const start = Date.now();
    try {
      // Deliberately use the real platform clock: fake timers cannot test Bun's socket idle timeout.
      while (!bytes.includes(": keep-alive\n\n")) {
        const chunk = await bounded(reader.read(), 20_000);
        expect(chunk.done).toBe(false);
        bytes += decoder.decode(chunk.value);
      }
      expect(Date.now() - start).toBeGreaterThanOrEqual(14_000);
      release.resolve();
      while (true) {
        const chunk = await bounded(reader.read());
        if (chunk.done) break;
        bytes += decoder.decode(chunk.value);
      }
      expect(bytes).toContain("after heartbeat");
      expect(bytes.match(/"type":"RUN_FINISHED"/g)).toHaveLength(1);
    } finally { release.resolve(); await reader.cancel(); }
  }, 30_000);

  test("large events survive a slow reader and unread replay does not hold shutdown open", async () => {
    // Random content cannot be mistaken for the provider loop guard's repeated-runaway shape.
    const payload = randomBytes(1024 * 1024 + 13).toString("base64");
    const produced = gate();
    const f = await fixture((stream, model) => {
      const message = assistant(model, [{ type: "text", text: "" }]);
      stream.push({ type: "start", partial: message });
      delta(stream, message, payload);
      finish(stream, message);
      produced.resolve();
    });
    const original = input();
    const response = await f.request(original);
    await bounded(produced.promise);
    // Demand starts only after provider completion; the real HTTP reader must preserve the whole event.
    const bytes = await response.text();
    const events = bytes.split("\n\n").filter(part => part.startsWith("data:")).map(part => JSON.parse(part.slice(5)) as WireEvent);
    expect(text(events)).toBe(payload);
    expect(terminal(events).type).toBe(E.RUN_FINISHED);
    const replay = await f.request(original);
    await bounded(f.close());
    await replay.body?.cancel().catch(() => {});
  }, 30_000);

  test("shutdown rejects a body-parsing loser before admission while awaiting provider abort completion", async () => {
    const entered = gate();
    const abortSeen = gate();
    const releaseAbort = gate();
    const parsing = gate();
    const releaseBody = gate();
    const f = await fixture(async (stream, model, _context, signal) => {
      entered.resolve();
      await new Promise<void>(resolve => {
        if (signal?.aborted) resolve();
        else signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      await releaseAbort.promise;
      finish(stream, assistant(model, [], "aborted"));
    });
    const owner = f.run(input());
    await bounded(entered.promise);
    const loser = input("must not be admitted");
    const originalRead = Request.prototype.arrayBuffer;
    Request.prototype.arrayBuffer = async function (this: Request) {
      const bytes = await originalRead.call(this);
      if (new TextDecoder().decode(bytes).includes(loser.runId)) { parsing.resolve(); await releaseBody.promise; }
      return bytes;
    };
    // The agent loop detaches a parked provider stream from its abort race, so hold the SDK's own
    // abort completion: that is the teardown step shutdown must actually await.
    const originalAbort = AgentSession.prototype.abort;
    AgentSession.prototype.abort = async function (this: AgentSession, ...args: Parameters<typeof originalAbort>) {
      abortSeen.resolve();
      await releaseAbort.promise;
      return originalAbort.apply(this, args);
    };
    let closing: Promise<void> | undefined;
    try {
      const responsePromise = f.request(loser);
      await bounded(parsing.promise);
      closing = f.close();
      await bounded(abortSeen.promise);
      let closed = false;
      void closing.then(() => { closed = true; });
      releaseBody.resolve();
      const response = await bounded(responsePromise);
      expect(response.status).toBe(503);
      expect(await response.json()).toMatchObject({ error: { code: "SHUTTING_DOWN" } });
      expect(f.calls).toBe(1);
      expect(closed).toBe(false);
    } finally {
      releaseBody.resolve();
      releaseAbort.resolve();
      Request.prototype.arrayBuffer = originalRead;
      AgentSession.prototype.abort = originalAbort;
      await closing;
      await owner.promise.catch(() => {});
    }
    const store = new RunStore(f.config);
    try { expect(() => store.thread(loser.threadId)).toThrow(); }
    finally { store.close(); }
  }, 30_000);
});
