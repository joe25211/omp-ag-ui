import {
  AgentRegistry, SessionManager, Settings, createAgentSession,
  discoverAuthStorage, ModelRegistry, type AuthStorage,
} from "@oh-my-pi/pi-coding-agent";
import type { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { EventType, type BaseEvent, type RunAgentInput } from "@ag-ui/core";
import type { AssistantMessage, Model, ToolCall } from "@oh-my-pi/pi-ai";
import { userText, type RunStore, type StoredUser, type ThreadState } from "./store";
import type { Config } from "./config";

export type OmpResources = {
  authStorage: AuthStorage;
  modelRegistry: ModelRegistry;
  model: Model;
};

export async function createOmpResources(config: Config): Promise<OmpResources> {
  process.env.PI_NO_TITLE = "1";
  const authStorage = await discoverAuthStorage();
  try {
    const modelRegistry = new ModelRegistry(authStorage);
    await modelRegistry.refresh();
    const available = modelRegistry.getAvailable("chat");
    if (!available.length) throw new Error("No authenticated models available");
    const model = config.provider && config.modelId
      ? available.find(item => item.provider === config.provider && item.id === config.modelId)
      : available[0];
    if (!model) throw new Error(`Configured model ${config.provider}/${config.modelId} is unavailable`);
    if (!config.provider) console.error(`Selected model ${model.provider}/${model.id}`);
    return { authStorage, modelRegistry, model };
  } catch (error) {
    authStorage.close();
    throw error;
  }
}

export type RunOutcome =
  | { status: "success" }
  | { status: "cancelled" }
  | { status: "error"; code: string; message: string };

const failureMessages = {
  SESSION_ERROR: "Session storage failed.",
  OMP_ERROR: "omp failed to complete the run.",
  OUTPUT_LIMIT: "Model output limit reached.",
  EMPTY_RESPONSE: "omp produced no assistant response.",
  TOOL_NOT_ALLOWED: "omp requested a tool outside the configured allowlist.",
} as const;

function nativeUser(text: string) {
  return { role: "user" as const, content: [{ type: "text" as const, text }], timestamp: Date.now() };
}

function importHistory(manager: SessionManager, input: RunAgentInput, model: Model): void {
  const calls = new Map<string, ToolCall>();
  const finalUser = input.messages.findLastIndex(message => message.role === "user");
  for (const message of input.messages.slice(0, finalUser)) {
    if (message.role === "user") {
      manager.appendMessage(nativeUser(userText(message.content)));
    } else if (message.role === "assistant") {
      const content: AssistantMessage["content"] = [];
      if (message.content) content.push({ type: "text", text: message.content });
      for (const call of message.toolCalls ?? []) {
        const block: ToolCall = {
          type: "toolCall", id: call.id, name: call.function.name,
          arguments: JSON.parse(call.function.arguments),
        };
        calls.set(block.id, block);
        content.push(block);
      }
      manager.appendMessage({
        role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: message.toolCalls?.length ? "toolUse" : "stop", timestamp: Date.now(),
        usage: {
          input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
      });
    } else if (message.role === "tool") {
      const call = calls.get(message.toolCallId);
      if (!call) throw new Error("Imported tool result has no matching call");
      manager.appendMessage({
        role: "toolResult", toolCallId: call.id, toolName: call.name,
        content: [{ type: "text", text: message.content }],
        isError: message.error !== undefined, timestamp: Date.now(),
      });
      calls.delete(call.id);
    }
  }
}

async function repairToolTail(manager: SessionManager): Promise<void> {
  const pending = new Map<string, ToolCall>();
  for (const entry of manager.getBranch()) {
    if (entry.type !== "message") continue;
    if (entry.message.role === "assistant") {
      for (const block of entry.message.content) {
        if (block.type === "toolCall") pending.set(block.id, block);
      }
    } else if (entry.message.role === "toolResult") {
      pending.delete(entry.message.toolCallId);
    }
  }
  if (!pending.size) return;
  await manager.appendEntriesAtomically(() => {
    for (const call of pending.values()) {
      manager.appendMessage({
        role: "toolResult", toolCallId: call.id, toolName: call.name, isError: true,
        content: [{
          type: "text",
          text: "Adapter restarted or stopped before completion was recorded; execution outcome unknown; do not retry automatically.",
        }],
        timestamp: Date.now(),
      });
    }
  });
  await manager.flush();
}

function markerData(data: unknown): Record<string, unknown> | undefined {
  return data !== null && typeof data === "object" && !Array.isArray(data)
    ? data as Record<string, unknown> : undefined;
}

async function reconcileUsers(
  manager: SessionManager,
  users: StoredUser[],
): Promise<void> {
  const marked = new Map<string, { hasUser: boolean; isTail: boolean }>();
  let current: { hasUser: boolean; isTail: boolean } | undefined;
  for (const entry of manager.getBranch()) {
    if (entry.type === "custom" && entry.customType === "agui.run") {
      if (current) current.isTail = false;
      const runId = markerData(entry.data)?.runId;
      current = { hasUser: false, isTail: true };
      if (typeof runId === "string") marked.set(runId, current);
    } else if (entry.type === "message" && entry.message.role === "user" && current) {
      current.hasUser = true;
    }
  }
  const missing = users.filter(user => user.runId !== null && !marked.get(user.runId)?.hasUser);
  if (!missing.length) return;
  await manager.appendEntriesAtomically(() => {
    for (const user of missing) {
      // A superseded empty marker needs a new segment for the recovered user.
      if (!marked.get(user.runId!)?.isTail) {
        manager.appendCustomEntry("agui.run", {
          runId: user.runId, userMessageId: user.id, text: user.text,
        });
      }
      manager.appendMessage(nativeUser(user.text));
    }
  });
  await manager.flush();
}

export async function runOmpTurn(
  config: Config,
  resources: OmpResources,
  input: RunAgentInput,
  store: RunStore,
  signal: AbortSignal,
  emit: (event: BaseEvent) => void,
): Promise<RunOutcome> {
  process.env.PI_NO_TITLE = "1";
  let manager: SessionManager | undefined;
  let session: AgentSession | undefined;
  let unsubscribe: (() => void) | undefined;
  let abortPromise: Promise<void> | undefined;
  let cancelled = signal.aborted;
  let failure: Extract<RunOutcome, { status: "error" }> | undefined;
  const observed: { stopReason?: AssistantMessage["stopReason"] } = {};
  let promptDispatched = false;
  let stage: "SESSION_ERROR" | "OMP_ERROR" = "SESSION_ERROR";
  let thread: ThreadState | undefined;
  let bootstrapReady = false;
  let assistant: { id: string; started: boolean; ended: boolean; sentDelta: boolean } | undefined;
  const announced = new Set<string>();
  const results = new Set<string>();
  const allowed = new Set(config.tools);

  function latch(code: keyof typeof failureMessages, error?: unknown): void {
    failure ??= { status: "error", code, message: failureMessages[code] };
    if (error instanceof Error) {
      console.error(`Native run ${code}`, error.name, error.message.replaceAll(config.token, "[redacted]").slice(0, 500));
    }
  }
  function stop(): void {
    if (!session || abortPromise) return;
    try {
      session.beginDispose();
    } catch (error) {
      latch("OMP_ERROR", error);
    }
    abortPromise = session.abort();
    // Mark rejection handled immediately; the owner still explicitly awaits it.
    void abortPromise.catch(error => latch("OMP_ERROR", error));
  }
  function cancel(): void {
    cancelled = true;
    stop();
  }
  signal.addEventListener("abort", cancel, { once: true });

  function send(event: BaseEvent): void {
    try {
      emit(event);
    } catch (error) {
      latch("SESSION_ERROR", error);
      stop();
      throw error;
    }
  }
  function closeText(): void {
    if (assistant?.started && !assistant.ended) {
      assistant.ended = true;
      send({ type: EventType.TEXT_MESSAGE_END, messageId: assistant.id });
    }
  }
  function text(delta: string): void {
    if (!delta || !assistant) return;
    if (!assistant.started) {
      assistant.started = true;
      send({ type: EventType.TEXT_MESSAGE_START, messageId: assistant.id, role: "assistant" });
    }
    send({ type: EventType.TEXT_MESSAGE_CONTENT, messageId: assistant.id, delta });
  }

  try {
    thread = store.thread(input.threadId);
    manager = await SessionManager.open(thread.sessionFile, undefined, undefined, {
      initialCwd: config.cwd, suppressBreadcrumb: true, throwIfMissing: thread.nativeReady,
    });
    if (manager.getCwd() !== config.cwd) throw new Error("Native session cwd mismatch");
    const branch = manager.getBranch();
    const bootstrapped = branch.some(entry => entry.type === "custom"
      && entry.customType === "agui.bootstrap" && markerData(entry.data)?.threadId === input.threadId);
    if (!bootstrapped) {
      if (thread.nativeReady || branch.some(entry => entry.type === "message")) {
        throw new Error("Missing native bootstrap marker");
      }
      const bootstrapManager = manager;
      await manager.appendEntriesAtomically(() => {
        importHistory(bootstrapManager, thread!.initialInput, resources.model);
        bootstrapManager.appendCustomEntry("agui.bootstrap", { threadId: input.threadId });
      });
    }
    await manager.flush();
    if (!thread.nativeReady) store.markNativeReady(input.threadId);
    bootstrapReady = true;
    await repairToolTail(manager);
    await reconcileUsers(manager, thread.users.filter(user => user.runId !== input.runId));

    const appendSystemPrompt = [
      ...input.messages.filter(message => message.role === "system" || message.role === "developer")
        .map(message => "content" in message && typeof message.content === "string" ? message.content : ""),
      ...input.context.map(context => `${context.description}:\n${context.value}`),
    ].filter(Boolean).join("\n\n");
    stage = "OMP_ERROR";
    ({ session } = await createAgentSession({
      ...resources, cwd: config.cwd, sessionManager: manager,
      settings: Settings.isolated({
        "async.enabled": false, "bash.autoBackground.enabled": false, "bash.direnv": "off",
        "compaction.enabled": false, "retry.enabled": false, "retry.modelFallback": false,
        "memory.backend": "off", "advisor.enabled": false, "autolearn.enabled": false,
        "goal.enabled": false, "plan.enabled": false, "tools.xdev": false,
      }),
      agentRegistry: new AgentRegistry(), bindProcessState: false, hasUI: false,
      cacheWarming: false, restrictToolNames: true, toolNames: config.tools,
      enableMCP: false, enableLsp: false, enableIrc: false, disableExtensionDiscovery: true,
      contextFiles: [], skills: [], rules: [], promptTemplates: [], slashCommands: [],
      appendSystemPrompt,
    }));
    if (cancelled || failure) stop();
    const active = session.getActiveToolNames();
    if (active.length !== allowed.size || active.some(name => !allowed.has(name))) {
      latch("TOOL_NOT_ALLOWED");
      stop();
    }
    unsubscribe = session.subscribe(event => {
      if (failure) return;
      try {
        if (event.type === "message_start" && event.message.role === "assistant") {
          closeText();
          assistant = { id: crypto.randomUUID(), started: false, ended: false, sentDelta: false };
        } else if (event.type === "message_update" && event.assistantMessageEvent.type === "text_delta") {
          const delta = event.assistantMessageEvent.delta;
          if (delta && assistant) {
            assistant.sentDelta = true;
            text(delta);
          }
        } else if (event.type === "message_end" && event.message.role === "assistant") {
          observed.stopReason = event.message.stopReason;
          assistant ??= { id: crypto.randomUUID(), started: false, ended: false, sentDelta: false };
          if (!assistant.sentDelta) {
            const complete = event.message.content
              .filter(block => block.type === "text").map(block => block.text).join("");
            if (complete) {
              console.error(`Complete-only assistant output for run ${input.runId}`);
              text(complete);
            }
          }
          closeText();
          const calls = event.message.content.filter(block => block.type === "toolCall");
          if (calls.some(call => !allowed.has(call.name))) {
            latch("TOOL_NOT_ALLOWED");
            stop();
            return;
          }
          for (const call of calls) {
            if (announced.has(call.id)) continue;
            const toolCallId = `${input.runId}:${call.id}`;
            send({
              type: EventType.TOOL_CALL_START, toolCallId,
              toolCallName: call.name, parentMessageId: assistant.id,
            });
            announced.add(call.id);
            send({ type: EventType.TOOL_CALL_ARGS, toolCallId, delta: JSON.stringify(call.arguments) });
            send({ type: EventType.TOOL_CALL_END, toolCallId });
          }
        } else if (event.type === "message_end" && event.message.role === "toolResult") {
          const result = event.message;
          if (!announced.has(result.toolCallId) || results.has(result.toolCallId)) return;
          if (result.content.some(block => block.type !== "text")) {
            console.error(`Skipped non-text tool result for run ${input.runId}`);
          }
          const content = result.content.filter(block => block.type === "text")
            .map(block => block.text).join("\n");
          send({
            type: EventType.TOOL_CALL_RESULT, messageId: crypto.randomUUID(),
            toolCallId: `${input.runId}:${result.toolCallId}`, role: "tool",
            content: `${result.isError ? "Error: " : ""}${content}`,
          });
          results.add(result.toolCallId);
        }
      } catch (error) {
        latch("OMP_ERROR", error);
        stop();
      }
    });
    stage = "SESSION_ERROR";
    const user = thread.users.find(user => user.runId === input.runId);
    if (!user) throw new Error("Accepted user missing from ledger");
    const currentManager = manager;
    await manager.appendEntriesAtomically(() => {
      currentManager.appendCustomEntry("agui.run", {
        runId: input.runId, userMessageId: user.id, text: user.text,
      });
    });
    await manager.flush();
    // Nothing asynchronous may separate this latch check from entering prompt.
    if (!cancelled && !failure) {
      stage = "OMP_ERROR";
      promptDispatched = await session.prompt(user.text, { expandPromptTemplates: false });
    }
  } catch (error) {
    latch(stage, error);
    stop();
  } finally {
    if (session) {
      try {
        session.beginDispose();
        if (abortPromise) await abortPromise;
        await session.waitForIdle();
        if (session.isStreaming || session.hasPendingAsyncWork()) {
          throw new Error("Native session did not become idle");
        }
      } catch (error) {
        latch("OMP_ERROR", error);
        stop();
        // A failed drain is not completion; still explicitly settle the abort.
        try {
          if (abortPromise) await abortPromise;
          await session.waitForIdle();
        } catch (drainError) {
          latch("OMP_ERROR", drainError);
        }
      }
    }
    try {
      if (manager && thread && bootstrapReady) await reconcileUsers(manager, thread.users);
      closeText();
    } catch (error) {
      latch("SESSION_ERROR", error);
    }
    unsubscribe?.();
    signal.removeEventListener("abort", cancel);
    // Cancellation can arrive while persistence repair is awaiting its flush.
    try {
      if (abortPromise) await abortPromise;
      if (session) await session.waitForIdle();
    } catch (error) {
      latch("OMP_ERROR", error);
    }
    try {
      if (manager) await manager.flush();
    } catch (error) {
      latch("SESSION_ERROR", error);
    }
    try {
      if (session) await session.dispose();
      else if (manager) await manager.close();
    } catch (error) {
      latch("SESSION_ERROR", error);
    }
  }
  if (failure) return failure;
  if (cancelled || observed.stopReason === "aborted") return { status: "cancelled" };
  const code = observed.stopReason === "error" ? "OMP_ERROR"
    : observed.stopReason === "length" ? "OUTPUT_LIMIT"
    : !promptDispatched || !observed.stopReason ? "EMPTY_RESPONSE" : undefined;
  return code ? { status: "error", code, message: failureMessages[code] } : { status: "success" };
}
