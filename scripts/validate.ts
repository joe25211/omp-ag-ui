#!/usr/bin/env bun
/**
 * Live endpoint validation for the omp AG-UI adapter, using the official
 * @ag-ui/client HttpAgent exactly as an external consumer would.
 *
 *   AGUI_TOKEN=<token> bun run validate [--url http://127.0.0.1:8789/agent/run]
 *
 * Prerequisites: a running adapter started with the same AGUI_TOKEN and a live
 * model, and OMP_CWD set to this repository root (the README scenario reads
 * README.md from disk and compares the heading independently).
 *
 * Prints observations, terminal codes, and timings, and exits non-zero on any
 * unmet assertion. The bearer token is never printed. runAgent() resolving is
 * never treated as success: outcomes come from observed protocol events.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  EventType,
  type BaseEvent,
  type RunAgentInput,
  type RunErrorEvent,
  type TextMessageContentEvent,
  type TextMessageStartEvent,
  type ToolCallArgsEvent,
  type ToolCallEndEvent,
  type ToolCallResultEvent,
  type ToolCallStartEvent,
} from "@ag-ui/core";
import { HttpAgent, type AgentSubscriber } from "@ag-ui/client";

type ChatMessages = RunAgentInput["messages"];

type Capture = {
  threadId: string;
  runId: string;
  requestStart: number;
  events: BaseEvent[];
  deltas: string[];
  text: string;
  toolStarts: Array<{ toolCallId: string; toolCallName: string }>;
  toolArgs: Array<{ toolCallId: string; delta: string }>;
  toolEnds: string[];
  toolResults: Array<{ toolCallId: string; content: string }>;
  firstDeltaAt?: number;
  terminal?: { type: string; code?: string; message?: string; synthetic: boolean };
  terminalAt?: number;
  clientAbort: boolean;
  httpError?: { status?: number; code?: string; message: string };
  agent: HttpAgent;
};

const failures: string[] = [];
let checks = 0;

function heading(title: string): void {
  console.log(`\n== ${title} ==`);
}
function note(message: string): void {
  console.log(`  · ${message}`);
}
function check(ok: boolean, label: string, detail?: string): boolean {
  checks++;
  console.log(`  ${ok ? "PASS" : "FAIL"} ${label}${!ok && detail ? ` — ${detail}` : ""}`);
  if (!ok) failures.push(label);
  return ok;
}
function timed(ms: number): string {
  return `${(ms / 1000).toFixed(1)}s`;
}
function words(text: string): number {
  const trimmed = text.trim();
  return trimmed ? trimmed.split(/\s+/).length : 0;
}
function snippet(text: string, limit = 160): string {
  const flat = text.replace(/\s+/g, " ").trim();
  return flat.length > limit ? `${flat.slice(0, limit)}…` : flat;
}
function payloadCode(payload: unknown): string | undefined {
  if (!payload || typeof payload !== "object") return undefined;
  const code = (payload as { error?: { code?: unknown } }).error?.code;
  return typeof code === "string" ? code : undefined;
}
function terminalLabel(capture: Capture): string {
  const terminal = capture.terminal;
  if (!terminal) return capture.httpError ? `HTTP ${capture.httpError.status ?? "?"} ${capture.httpError.code ?? ""}` : "no terminal event observed";
  if (terminal.type === EventType.RUN_FINISHED) return "RUN_FINISHED";
  const origin = terminal.synthetic ? "client-synthesized on local abort" : "recorded in adapter journal";
  return `RUN_ERROR code=${terminal.code ?? "(none)"} (${origin})`;
}
function eventIds(capture: Capture): string {
  const ids: string[] = [];
  for (const event of capture.events) {
    if (event.type === EventType.TEXT_MESSAGE_START) ids.push(`text:${(event as TextMessageStartEvent).messageId}`);
    else if (event.type === EventType.TOOL_CALL_START) ids.push(`tool:${(event as ToolCallStartEvent).toolCallId}`);
    else if (event.type === EventType.TOOL_CALL_RESULT) ids.push(`result:${(event as ToolCallResultEvent).toolCallId}`);
  }
  return ids.join(" ") || "(none)";
}

function userMessage(id: string, text: string): ChatMessages[number] {
  return { id, role: "user", content: text };
}

type RunSpec = {
  threadId: string;
  runId: string;
  messages: ChatMessages;
  abortOnFirstDelta?: boolean;
};

async function runOnce(url: string, token: string, spec: RunSpec): Promise<Capture> {
  const agent = new HttpAgent({
    url,
    threadId: spec.threadId,
    initialMessages: spec.messages,
    headers: { Authorization: `Bearer ${token}` },
  });
  const capture: Capture = {
    threadId: spec.threadId,
    runId: spec.runId,
    requestStart: performance.now(),
    events: [], deltas: [], text: "", toolStarts: [], toolArgs: [], toolEnds: [], toolResults: [],
    clientAbort: false,
    agent,
  };
  const abortNow = (): void => {
    if (capture.clientAbort) return;
    capture.clientAbort = true;
    agent.abortRun();
  };
  const subscriber: AgentSubscriber = {
    onEvent: ({ event }) => {
      capture.events.push(event);
      if (event.type === EventType.TEXT_MESSAGE_CONTENT) {
        const { delta } = event as TextMessageContentEvent;
        if (delta.length > 0) {
          capture.deltas.push(delta);
          capture.text += delta;
          capture.firstDeltaAt ??= performance.now();
          if (spec.abortOnFirstDelta) abortNow();
        }
      } else if (event.type === EventType.TOOL_CALL_START) {
        const call = event as ToolCallStartEvent;
        capture.toolStarts.push({ toolCallId: call.toolCallId, toolCallName: call.toolCallName });
      } else if (event.type === EventType.TOOL_CALL_ARGS) {
        const args = event as ToolCallArgsEvent;
        capture.toolArgs.push({ toolCallId: args.toolCallId, delta: args.delta });
      } else if (event.type === EventType.TOOL_CALL_END) {
        capture.toolEnds.push((event as ToolCallEndEvent).toolCallId);
      } else if (event.type === EventType.TOOL_CALL_RESULT) {
        const result = event as ToolCallResultEvent;
        capture.toolResults.push({ toolCallId: result.toolCallId, content: result.content });
      } else if (event.type === EventType.RUN_FINISHED) {
        capture.terminal = { type: event.type, synthetic: false };
        capture.terminalAt = performance.now();
      } else if (event.type === EventType.RUN_ERROR) {
        const error = event as RunErrorEvent;
        capture.terminal = {
          type: event.type,
          code: error.code,
          message: error.message,
          // The client emits a local RUN_ERROR wrapping the fetch AbortError; the
          // adapter journals none. rawEvent only ever appears on that synthetic one.
          synthetic: "rawEvent" in event,
        };
        capture.terminalAt = performance.now();
      }
    },
  };
  try {
    await agent.runAgent({ runId: spec.runId, tools: [], context: [], forwardedProps: {} }, subscriber);
  } catch (error) {
    const failure = error as { status?: number; payload?: unknown; message?: string };
    capture.httpError = {
      status: failure.status,
      code: payloadCode(failure.payload),
      message: failure.message ?? String(error),
    };
  }
  return capture;
}

type Ctx = { url: string; token: string };

async function scenarioStreaming(ctx: Ctx): Promise<{ capture: Capture; messages: ChatMessages }> {
  heading("1. streaming explanation (>=200 words, multiple deltas)");
  const threadId = `val-stream-${crypto.randomUUID()}`;
  const runId = `val-stream-run-${crypto.randomUUID()}`;
  const messages: ChatMessages = [
    userMessage(`val-stream-user-${crypto.randomUUID()}`,
      "Explain in at least 200 words why Server-Sent Events suit streaming model output. Plain prose, no lists."),
  ];
  note(`thread=${threadId} run=${runId}`);
  const capture = await runOnce(ctx.url, ctx.token, { threadId, runId, messages });
  if (!check(!capture.httpError, "run accepted", capture.httpError?.message)) return { capture, messages };
  check(capture.terminal?.type === EventType.RUN_FINISHED, "terminal is RUN_FINISHED", terminalLabel(capture));
  if (capture.firstDeltaAt !== undefined) {
    note(`first delta at ${timed(capture.firstDeltaAt - capture.requestStart)} after request start`);
    check(capture.deltas.length >= 2, "multiple nonempty text deltas before the terminal", `${capture.deltas.length} delta(s)`);
  } else {
    check(false, "text deltas observed before the terminal");
  }
  if (capture.terminalAt !== undefined) note(`terminal ${capture.terminal?.type} at ${timed(capture.terminalAt - capture.requestStart)}`);
  note(`assembled ${words(capture.text)} words; deltas=${capture.deltas.length}`);
  check(words(capture.text) >= 200, "assembled explanation is at least 200 words", `${words(capture.text)} words`);
  note(`text: ${snippet(capture.text)}`);
  return { capture, messages };
}

async function scenarioRead(ctx: Ctx): Promise<{ capture: Capture; messages: ChatMessages } | undefined> {
  heading("2. native read tool: args, result, and final answer");
  const readme = join(import.meta.dir, "..", "README.md");
  let localHeading: string | undefined;
  try {
    localHeading = readFileSync(readme, "utf8").split("\n").find(line => line.startsWith("#"));
  } catch {
    localHeading = undefined;
  }
  if (!check(localHeading !== undefined, "README.md heading readable locally (OMP_CWD must be this repository root)")) return undefined;
  const threadId = `val-read-${crypto.randomUUID()}`;
  const runId = `val-read-run-${crypto.randomUUID()}`;
  const messages: ChatMessages = [
    userMessage(`val-read-user-${crypto.randomUUID()}`, "Use the read tool to read README.md and quote its first heading."),
  ];
  note(`thread=${threadId} run=${runId}`);
  note(`local heading: ${localHeading}`);
  const capture = await runOnce(ctx.url, ctx.token, { threadId, runId, messages });
  if (!check(!capture.httpError, "run accepted", capture.httpError?.message)) return undefined;
  check(capture.terminal?.type === EventType.RUN_FINISHED, "terminal is RUN_FINISHED", terminalLabel(capture));
  check(capture.toolStarts.length === 1 && capture.toolStarts[0].toolCallName === "read",
    "exactly one read tool call", capture.toolStarts.map(call => call.toolCallName).join(",") || "none");
  const args = capture.toolArgs.map(entry => entry.delta).join("");
  let parsed: unknown;
  try {
    parsed = JSON.parse(args);
  } catch {
    parsed = undefined;
  }
  note(`call args: ${snippet(args, 200)}`);
  const paths = parsed && typeof parsed === "object" ? Object.values(parsed).filter(value => typeof value === "string") : [];
  check(paths.some(value => (value as string).includes("README")), "read arguments target README", snippet(args, 120));
  check(capture.toolEnds.length === 1 && capture.toolEnds[0] === capture.toolStarts[0]?.toolCallId, "tool call start/args/end triad is complete");
  const result = capture.toolResults[0];
  check(capture.toolResults.length === 1 && result.toolCallId === capture.toolStarts[0]?.toolCallId,
    "exactly one tool result paired with the call");
  check(result !== undefined && result.content.includes(localHeading as string), "tool result contains the real README heading",
    snippet(result?.content ?? "no result"));
  note(`result: ${snippet(result?.content ?? "", 200)}`);
  check(capture.text.trim().length > 0, "final assistant text present after the tool call", snippet(capture.text));
  note(`final text: ${snippet(capture.text)}`);
  return { capture, messages };
}

async function scenarioNonce(ctx: Ctx): Promise<void> {
  heading("3. multi-turn nonce recall (fresh agent, delta-only second request)");
  const threadId = `val-nonce-${crypto.randomUUID()}`;
  const nonce = crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  const firstRunId = `val-nonce-run1-${crypto.randomUUID()}`;
  const first = await runOnce(ctx.url, ctx.token, {
    threadId,
    runId: firstRunId,
    messages: [userMessage(`val-nonce-user1-${crypto.randomUUID()}`, `Remember ${nonce}; reply ACK.`)],
  });
  note(`thread=${threadId} nonce=${nonce}`);
  if (!check(!first.httpError, "first turn accepted", first.httpError?.message)) return;
  check(first.terminal?.type === EventType.RUN_FINISHED, "first turn terminal is RUN_FINISHED", terminalLabel(first));
  note(`first turn reply: ${snippet(first.text)}`);

  const secondRunId = `val-nonce-run2-${crypto.randomUUID()}`;
  const second = await runOnce(ctx.url, ctx.token, {
    threadId,
    runId: secondRunId,
    messages: [userMessage(`val-nonce-user2-${crypto.randomUUID()}`, "What nonce did I ask you to remember? Reply only with it.")],
  });
  note(`second request sent 1 message only (no first-turn resend); run=${secondRunId}`);
  if (!check(!second.httpError, "second turn accepted", second.httpError?.message)) return;
  check(second.terminal?.type === EventType.RUN_FINISHED, "second turn terminal is RUN_FINISHED", terminalLabel(second));
  const recalled = second.text.trim().replace(/[`"'\s.]/g, "");
  note(`second turn reply: ${snippet(second.text)}`);
  check(recalled === nonce, "recalled nonce matches the first turn", `expected ${nonce}, got ${recalled || "(empty)"}`);
}

async function scenarioAbort(ctx: Ctx): Promise<void> {
  heading("4. abort reaches omp: replay CANCELLED, then a new turn");
  const threadId = `val-abort-${crypto.randomUUID()}`;
  const runId = `val-abort-run-${crypto.randomUUID()}`;
  const prompt = "Produce a numbered list of 10000 detailed items, one per line. Do not stop early.";
  const messages: ChatMessages = [userMessage(`val-abort-user-${crypto.randomUUID()}`, prompt)];
  note(`thread=${threadId} run=${runId}`);
  const aborted = await runOnce(ctx.url, ctx.token, { threadId, runId, messages, abortOnFirstDelta: true });
  check(aborted.clientAbort, "client aborted on the first text delta");
  note(`synthetic local abort: HttpAgent.abortRun() at ${aborted.terminalAt !== undefined ? timed(aborted.terminalAt - aborted.requestStart) : "?"}; ` +
    `local terminal=${terminalLabel(aborted)} (not adapter evidence)`);
  note(`text received before abort: ${snippet(aborted.text, 80)}`);

  const deadline = performance.now() + 5000;
  let replay: Capture | undefined;
  let attempts = 0;
  let lastBusy = "";
  while (performance.now() < deadline && replay === undefined) {
    attempts++;
    const attempt = await runOnce(ctx.url, ctx.token, { threadId, runId, messages });
    if (!attempt.httpError) {
      replay = attempt;
      break;
    }
    lastBusy = `HTTP ${attempt.httpError.status ?? "?"} ${attempt.httpError.code ?? attempt.httpError.message}`;
    if (attempt.httpError.status !== 409 || (attempt.httpError.code !== "RUN_ACTIVE" && attempt.httpError.code !== "THREAD_BUSY")) {
      check(false, "replay retry failed for a non-busy reason", lastBusy);
      return;
    }
    await Bun.sleep(150);
  }
  note(`replay attempts: ${attempts}${lastBusy ? ` (last busy response: ${lastBusy})` : ""}`);
  if (!check(replay !== undefined, "original run replayed within 5s of the abort")) return;
  const recorded = replay as Capture;
  note(`recorded terminal (adapter journal, observed via replay): ${terminalLabel(recorded)}`);
  check(recorded.events[0]?.type === EventType.RUN_STARTED, "replay starts at RUN_STARTED", recorded.events[0]?.type ?? "no events");
  check(recorded.terminal?.type === EventType.RUN_ERROR && recorded.terminal.code === "CANCELLED",
    "recorded terminal is RUN_ERROR code=CANCELLED", terminalLabel(recorded));
  if (aborted.terminal && !aborted.terminal.synthetic) {
    note(`warning: the live client saw ${terminalLabel(aborted)} before the abort; the journal is authoritative`);
  }

  const second = await runOnce(ctx.url, ctx.token, { threadId, runId, messages });
  if (!check(!second.httpError && second.terminal?.code === "CANCELLED", "second replay returns the same frozen terminal", terminalLabel(second))) return;
  check(JSON.stringify(second.events) === JSON.stringify(recorded.events), "replay is frozen: identical events, no fresh provider output");

  const nextRunId = `val-abort-next-${crypto.randomUUID()}`;
  const next = await runOnce(ctx.url, ctx.token, {
    threadId,
    runId: nextRunId,
    messages: [userMessage(`val-abort-next-user-${crypto.randomUUID()}`, "Reply with the single word: recovered.")],
  });
  note(`new turn run=${nextRunId}`);
  check(!next.httpError && next.terminal?.type === EventType.RUN_FINISHED, "a new turn on the thread succeeds after cancellation", terminalLabel(next));
  check(next.text.trim().length > 0, "new turn produced assistant text", snippet(next.text));
}

async function scenarioReplay(ctx: Ctx, source: { capture: Capture; messages: ChatMessages }): Promise<void> {
  heading("5. completed-run replay (fresh agent, original messages)");
  const replay = await runOnce(ctx.url, ctx.token, { threadId: source.capture.threadId, runId: source.capture.runId, messages: source.messages });
  note(`thread=${source.capture.threadId} run=${source.capture.runId}`);
  if (!check(!replay.httpError, "replay accepted", replay.httpError?.message)) return;
  note(`original ids: ${eventIds(source.capture)}`);
  note(`replay ids:   ${eventIds(replay)}`);
  check(JSON.stringify(replay.events) === JSON.stringify(source.capture.events),
    "replayed event stream is identical to the original (same IDs, content, and order)");
  check(replay.text === source.capture.text, "replayed text matches the original");
  const roles = (agent: HttpAgent): string => agent.messages.map(message => message.role).join(",");
  note(`original client messages: ${roles(source.capture.agent)}`);
  note(`replay client messages:   ${roles(replay.agent)}`);
  check(replay.agent.messages.filter(message => message.role === "user").length === 1,
    "replay client holds exactly one user message (no duplicated turns)");
  check(roles(replay.agent) === roles(source.capture.agent), "replay client message roles match the original run");
}

async function scenarioWrongToken(ctx: Ctx): Promise<void> {
  heading("6. wrong bearer token");
  const body: RunAgentInput = {
    threadId: `val-auth-${crypto.randomUUID()}`,
    runId: `val-auth-run-${crypto.randomUUID()}`,
    messages: [userMessage(`val-auth-user-${crypto.randomUUID()}`, "This request must never reach a model.")],
    tools: [], context: [], state: {}, forwardedProps: {},
  };
  const response = await fetch(ctx.url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer wrong-${crypto.randomUUID()}` },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  check(response.status === 401, "wrong token returns 401", `status ${response.status}`);
  check(payloadCode(parsed) === "UNAUTHORIZED", "error code is UNAUTHORIZED", snippet(text, 160));
  check(!(response.headers.get("content-type") ?? "").includes("text/event-stream"), "rejection is not an SSE stream");
}

function parseArgs(argv: string[]): { url: string; help: boolean } {
  let url = "http://127.0.0.1:8789/agent/run";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--url") {
      const value = argv[++i];
      if (!value) throw new Error("--url requires a value");
      url = value;
    } else if (argv[i] === "--help" || argv[i] === "-h") {
      return { url, help: true };
    } else {
      throw new Error(`Unknown argument: ${argv[i]}`);
    }
  }
  if (!url.startsWith("http")) throw new Error(`--url must be an http(s) URL, got ${url}`);
  return { url, help: false };
}

async function step<T>(label: string, fn: () => Promise<T>): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    checks++;
    failures.push(label);
    console.log(`  FAIL ${label} — unexpected error: ${message}`);
    return undefined;
  }
}

let args: { url: string; help: boolean };
try {
  args = parseArgs(process.argv.slice(2));
} catch (error) {
  console.error(`${error instanceof Error ? error.message : String(error)}\nusage: AGUI_TOKEN=<token> bun run validate [--url http://127.0.0.1:8789/agent/run]`);
  process.exit(2);
}
if (args.help) {
  console.log("usage: AGUI_TOKEN=<token> bun run validate [--url http://127.0.0.1:8789/agent/run]");
  process.exit(0);
}
const token = process.env.AGUI_TOKEN ?? "";
if (!token) {
  console.error("AGUI_TOKEN is required: the adapter's bearer token (never printed).");
  process.exit(2);
}
const ctx: Ctx = { url: args.url, token };
console.log(`omp AG-UI validation → ${args.url} (bearer token loaded from AGUI_TOKEN, not printed)`);

const streamingResult = await step("1. streaming explanation", () => scenarioStreaming(ctx));

const readResult = await step("2. read tool", () => scenarioRead(ctx));

await step("3. nonce recall", () => scenarioNonce(ctx));
await step("4. abort and replay", () => scenarioAbort(ctx));

const replaySource = readResult ?? streamingResult;
if (replaySource !== undefined) {
  await step("5. completed-run replay", () => scenarioReplay(ctx, replaySource));
} else {
  checks++;
  failures.push("5. completed-run replay");
  console.log("  FAIL 5. completed-run replay — no completed run available to replay");
}

await step("6. wrong bearer token", () => scenarioWrongToken(ctx));

console.log(`\n${checks - failures.length}/${checks} checks passed`);
if (failures.length > 0) {
  console.log("FAILURES:");
  for (const failure of failures) console.log(`  - ${failure}`);
  process.exit(1);
}
console.log("validation complete: all checks passed");