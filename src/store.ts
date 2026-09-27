import { Database, type Statement } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { chmodSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { EventType, type BaseEvent, type RunAgentInput, type UserMessage } from "@ag-ui/core";
import type { Config } from "./config";

export type Admission =
  | { kind: "run" }
  | { kind: "replay" }
  | { kind: "reject"; status: 409 | 422; code: string; message: string };

export type StoredUser = { id: string; text: string; runId: string | null };

export type ThreadState = {
  sessionFile: string;
  nativeReady: boolean;
  initialInput: RunAgentInput;
  users: StoredUser[];
};

type ThreadRow = { thread_id: string; cwd: string; initial_run_id: string; users_json: string; native_ready: number };
type RunRow = { input_json: string; status: string };
type EventRow = { seq: number; event_json: string };

const MAX_ID_LENGTH = 256;
const SYNTHETIC_TOOL_RESULT =
  "Error: Adapter stopped before a tool result was recorded; execution outcome unknown; do not retry automatically.";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS threads (
  thread_id TEXT PRIMARY KEY,
  cwd TEXT NOT NULL,
  initial_run_id TEXT NOT NULL,
  users_json TEXT NOT NULL,
  native_ready INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS runs (
  thread_id TEXT NOT NULL REFERENCES threads(thread_id),
  run_id TEXT NOT NULL,
  input_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running','done')),
  PRIMARY KEY (thread_id, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_thread ON runs(thread_id) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS events (
  thread_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  PRIMARY KEY (thread_id, run_id, seq),
  FOREIGN KEY (thread_id, run_id) REFERENCES runs(thread_id, run_id)
);
`;

function deny(status: 409 | 422, code: string, message: string): Admission {
  return { kind: "reject", status, code, message };
}

/** `BaseEvent` is the open base schema, so journaled IDs only narrow at runtime. */
function eventField(event: BaseEvent, key: "messageId" | "toolCallId"): string {
  const value: unknown = event[key];
  if (typeof value !== "string" || !value) throw new Error(`Journal event ${event.type} has no usable ${key}.`);
  return value;
}

/** Text of an AG-UI user message: string content, or text parts concatenated in order. */
export function userText(content: UserMessage["content"]): string {
  if (typeof content === "string") return content;
  let text = "";
  for (const part of content) if (part.type === "text") text += part.text;
  return text;
}

function comparableInput(input: RunAgentInput): RunAgentInput {
  // Run identity is the recorded JSON: OpenBot remints its expiring forwardedProps.openbotRun on
  // every remote request, and a key that is absent and a key that is present-but-undefined are the
  // same field on the wire.
  return JSON.parse(JSON.stringify({ ...input, forwardedProps: undefined })) as RunAgentInput;
}

function validateImportedHistory(input: RunAgentInput): Admission | undefined {
  const seenCalls = new Set<string>();
  const pendingCalls = new Set<string>();
  for (const message of input.messages) {
    if (message.role === "assistant") {
      for (const call of message.toolCalls ?? []) {
        if (!call.id || seenCalls.has(call.id)) {
          return deny(422, "INVALID_HISTORY", `Imported history repeats or omits tool call ID "${call.id}".`);
        }
        seenCalls.add(call.id);
        if (!call.function.name) return deny(422, "INVALID_HISTORY", `Tool call "${call.id}" has no name.`);
        let args: unknown;
        try {
          args = JSON.parse(call.function.arguments);
        } catch {
          return deny(422, "INVALID_HISTORY", `Tool call "${call.id}" has arguments that are not JSON.`);
        }
        if (typeof args !== "object" || args === null || Array.isArray(args)) {
          return deny(422, "INVALID_HISTORY", `Tool call "${call.id}" has arguments that are not a JSON object.`);
        }
        pendingCalls.add(call.id);
      }
    } else if (message.role === "tool") {
      if (!pendingCalls.delete(message.toolCallId)) {
        return deny(422, "INVALID_HISTORY", `Tool result "${message.id}" does not match a preceding tool call.`);
      }
    }
  }
  if (pendingCalls.size) {
    return deny(422, "INVALID_HISTORY", `Imported history leaves ${pendingCalls.size} tool call(s) without results.`);
  }
  return undefined;
}

/**
 * Durable journal for accepted runs: one SQLite file plus one native transcript per thread.
 *
 * The connection runs in SQLite EXCLUSIVE locking mode, so a second process pointed at the same
 * session directory fails at startup instead of rewriting another process's active runs. Records
 * are never pruned; the operator archives the stopped session directory.
 */
export class RunStore {
  #db: Database;
  #closed = false;
  #cwd: string;
  #sessionDir: string;
  #tools: Set<string>;
  #selectThread: Statement;
  #insertThread: Statement;
  #markNativeReady: Statement;
  #selectRun: Statement;
  #selectRunning: Statement;
  #insertRun: Statement;
  #updateUsers: Statement;
  #selectStale: Statement;
  #updateStatus: Statement;
  #insertEvent: Statement;
  #selectEvent: Statement;
  #selectEvents: Statement;

  constructor(config: Pick<Config, "sessionDir" | "cwd" | "tools">) {
    this.#cwd = config.cwd;
    this.#sessionDir = config.sessionDir;
    this.#tools = new Set(config.tools);
    mkdirSync(config.sessionDir, { recursive: true, mode: 0o700 });
    chmodSync(config.sessionDir, 0o700);
    const journalPath = join(config.sessionDir, "runs.sqlite");
    this.#db = new Database(journalPath, { create: true });
    try {
      this.#db.exec("PRAGMA locking_mode = EXCLUSIVE");
      this.#db.exec("PRAGMA journal_mode = WAL");
      this.#db.exec("PRAGMA synchronous = FULL");
      this.#db.exec("PRAGMA foreign_keys = ON");
      this.#db.exec("BEGIN EXCLUSIVE");
      try {
        this.#db.exec(SCHEMA);
        this.#db.exec("COMMIT");
      } catch (error) {
        this.#db.exec("ROLLBACK");
        throw error;
      }
      chmodSync(journalPath, 0o600);
      this.#selectThread = this.#db.query("SELECT * FROM threads WHERE thread_id = ?");
      this.#insertThread = this.#db.query(
        "INSERT INTO threads (thread_id, cwd, initial_run_id, users_json, native_ready) VALUES (?, ?, ?, ?, ?)",
      );
      this.#markNativeReady = this.#db.query("UPDATE threads SET native_ready = 1 WHERE thread_id = ?");
      this.#selectRun = this.#db.query("SELECT input_json, status FROM runs WHERE thread_id = ? AND run_id = ?");
      this.#selectRunning = this.#db.query("SELECT run_id FROM runs WHERE thread_id = ? AND status = 'running'");
      this.#insertRun = this.#db.query("INSERT INTO runs (thread_id, run_id, input_json, status) VALUES (?, ?, ?, ?)");
      this.#updateUsers = this.#db.query("UPDATE threads SET users_json = ? WHERE thread_id = ?");
      this.#selectStale = this.#db.query("SELECT thread_id, run_id FROM runs WHERE status = 'running'");
      this.#updateStatus = this.#db.query("UPDATE runs SET status = 'done' WHERE thread_id = ? AND run_id = ?");
      this.#insertEvent = this.#db.query(
        `INSERT INTO events (thread_id, run_id, seq, event_json)
         SELECT ?, ?, COALESCE(MAX(seq), 0) + 1, ? FROM events WHERE thread_id = ? AND run_id = ?`,
      );
      this.#selectEvent = this.#db.query(
        "SELECT seq, event_json FROM events WHERE thread_id = ? AND run_id = ? AND seq > ? ORDER BY seq LIMIT 1",
      );
      this.#selectEvents = this.#db.query(
        "SELECT seq, event_json FROM events WHERE thread_id = ? AND run_id = ? ORDER BY seq",
      );
      for (const row of this.#selectStale.all() as { thread_id: string; run_id: string }[]) {
        this.append(row.thread_id, row.run_id, {
          type: EventType.RUN_ERROR,
          code: "SERVER_RESTARTED",
          message:
            "Adapter restarted before run completion; tool execution outcome may be unknown. The run will not be re-executed.",
        });
      }
    } catch (error) {
      this.#db.close();
      throw error;
    }
  }

  /**
   * Validate and record a run, or explain why it cannot run: an identical recorded run replays,
   * anything else is rejected before any SDK work happens.
   */
  admit(input: RunAgentInput): Admission {
    for (const [label, id] of [
      ["threadId", input.threadId],
      ["runId", input.runId],
    ] as const) {
      if (!id || id.length > MAX_ID_LENGTH) {
        return deny(422, "INVALID_INPUT", `${label} must be nonempty and at most ${MAX_ID_LENGTH} characters.`);
      }
    }
    const messageIds = new Set<string>();
    for (const message of input.messages) {
      if (!message.id || message.id.length > MAX_ID_LENGTH || messageIds.has(message.id)) {
        return deny(
          422,
          "INVALID_INPUT",
          `Message ID "${message.id}" must be nonempty, at most ${MAX_ID_LENGTH} characters, and unique.`,
        );
      }
      messageIds.add(message.id);
    }
    if (input.resume?.length) return deny(422, "UNSUPPORTED_RESUME", "Run resumption is not supported.");
    const texts = this.#userTexts(input);
    const users = input.messages
      .filter(message => message.role === "user")
      .map(message => ({ id: message.id, text: texts.get(message.id) ?? "" }));
    let finalUser: UserMessage | undefined;
    for (let index = input.messages.length - 1; index >= 0; index--) {
      const message = input.messages[index]!;
      if (message.role === "activity" || message.role === "reasoning") continue;
      if (message.role === "user") finalUser = message;
      break;
    }
    if (!finalUser) {
      return deny(422, "TEXT_REQUIRED", "The final message must be a user message with nonblank text content.");
    }
    const finalText = texts.get(finalUser.id) ?? "";
    if (!finalText.trim()) {
      return deny(422, "TEXT_REQUIRED", "The final message must be a user message with nonblank text content.");
    }

    const recordedRun = this.#selectRun.get(input.threadId, input.runId) as RunRow | null;
    if (recordedRun) {
      const recorded = JSON.parse(recordedRun.input_json) as RunAgentInput;
      if (!isDeepStrictEqual(comparableInput(input), comparableInput(recorded))) {
        return deny(409, "RUN_INPUT_CONFLICT", "This run ID was already recorded with different input.");
      }
      return recordedRun.status === "running"
        ? deny(409, "RUN_ACTIVE", "This run is still active on the adapter.")
        : { kind: "replay" };
    }

    const threadRow = this.#selectThread.get(input.threadId) as ThreadRow | null;
    if (!threadRow) {
      const invalidHistory = validateImportedHistory(input);
      if (invalidHistory) return invalidHistory;
    } else {
      if (this.#selectRunning.get(input.threadId)) {
        return deny(409, "THREAD_BUSY", "This thread already has an active run.");
      }
      if (threadRow.cwd !== this.#cwd) {
        return deny(409, "THREAD_CWD_CHANGED", "This thread was created with a different working directory.");
      }
    }

    const collision = input.tools.find(tool => this.#tools.has(tool.name));
    if (collision) {
      return deny(
        422,
        "TOOL_NAME_CONFLICT",
        `Frontend tool "${collision.name}" conflicts with a configured omp tool.`,
      );
    }

    const ledger: StoredUser[] = threadRow ? (JSON.parse(threadRow.users_json) as StoredUser[]) : [];
    if (threadRow) {
      if (ledger.some(user => user.id === finalUser.id)) {
        return deny(409, "DUPLICATE_USER_MESSAGE", `User message ${finalUser.id} was already accepted.`);
      }
      if (users.length !== 1) {
        const prefixMatches =
          users.length === ledger.length + 1 &&
          users
            .slice(0, -1)
            .every((user, index) => user.id === ledger[index]!.id && user.text === ledger[index]!.text);
        if (!prefixMatches) {
          return deny(409, "HISTORY_CONFLICT", "Supplied user history does not match this thread.");
        }
      }
    }
    const accepted: StoredUser[] = users.map((user, index) => ({
      ...user,
      runId: index === users.length - 1 ? input.runId : (ledger[index]?.runId ?? null),
    }));
    // The incoming prefix is identical to the ledger prefix when one was supplied, so only the
    // newly accepted final turn is appended either way.
    const nextUsers: StoredUser[] = threadRow ? [...ledger, accepted[accepted.length - 1]!] : accepted;

    if (!threadRow) {
      const threadDir = this.#threadDir(input.threadId);
      mkdirSync(threadDir, { recursive: true, mode: 0o700 });
      chmodSync(threadDir, 0o700);
    }
    this.#transaction(() => {
      if (threadRow) this.#updateUsers.run(JSON.stringify(nextUsers), input.threadId);
      else this.#insertThread.run(input.threadId, this.#cwd, input.runId, JSON.stringify(nextUsers), 0);
      this.#insertRun.run(input.threadId, input.runId, JSON.stringify(input), "running");
      this.#appendEvent(input.threadId, input.runId, {
        type: EventType.RUN_STARTED,
        threadId: input.threadId,
        runId: input.runId,
      });
    });
    return { kind: "run" };
  }

  thread(threadId: string): ThreadState {
    const threadRow = this.#selectThread.get(threadId) as ThreadRow | null;
    if (!threadRow) throw new Error(`Unknown thread ${threadId}.`);
    const initialRun = this.#selectRun.get(threadId, threadRow.initial_run_id) as RunRow | null;
    if (!initialRun) throw new Error(`Thread ${threadId} has no recorded initial run.`);
    return {
      sessionFile: this.#sessionFile(threadId),
      nativeReady: threadRow.native_ready !== 0,
      initialInput: JSON.parse(initialRun.input_json) as RunAgentInput,
      users: JSON.parse(threadRow.users_json) as StoredUser[],
    };
  }

  markNativeReady(threadId: string): void {
    if (this.#markNativeReady.run(threadId).changes === 0) throw new Error(`Unknown thread ${threadId}.`);
  }

  /** Append one protocol event, balancing open text/tool lifecycles when the event is terminal. */
  append(threadId: string, runId: string, event: BaseEvent): void {
    const run = this.#selectRun.get(threadId, runId) as RunRow | null;
    if (!run) throw new Error(`Run ${runId} of thread ${threadId} is not recorded.`);
    if (run.status !== "running") throw new Error(`Run ${runId} of thread ${threadId} is already finished.`);
    const terminal = event.type === EventType.RUN_FINISHED || event.type === EventType.RUN_ERROR;
    this.#transaction(() => {
      if (terminal) this.#balance(threadId, runId);
      this.#appendEvent(threadId, runId, event);
      if (terminal) this.#updateStatus.run(threadId, runId);
    });
  }

  readEvent(threadId: string, runId: string, afterSeq: number): { seq: number; event: BaseEvent } | undefined {
    const row = this.#selectEvent.get(threadId, runId, afterSeq) as EventRow | null;
    return row ? { seq: row.seq, event: JSON.parse(row.event_json) as BaseEvent } : undefined;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }

  /**
   * Close every lifecycle the journal left open so a replay never leaves a client waiting forever:
   * text streams end, announced tools end, and tools without a recorded result get an explicit
   * uncertainty result — never a fabricated outcome.
   */
  #balance(threadId: string, runId: string): void {
    const openText = new Set<string>();
    const tools = new Map<string, { ended: boolean; result: boolean }>();
    for (const row of this.#selectEvents.all(threadId, runId) as EventRow[]) {
      const event = JSON.parse(row.event_json) as BaseEvent;
      switch (event.type) {
        case EventType.TEXT_MESSAGE_START:
          openText.add(eventField(event, "messageId"));
          break;
        case EventType.TEXT_MESSAGE_END:
          openText.delete(eventField(event, "messageId"));
          break;
        case EventType.TOOL_CALL_START:
          tools.set(eventField(event, "toolCallId"), { ended: false, result: false });
          break;
        case EventType.TOOL_CALL_END: {
          const tool = tools.get(eventField(event, "toolCallId"));
          if (tool) tool.ended = true;
          break;
        }
        case EventType.TOOL_CALL_RESULT: {
          const tool = tools.get(eventField(event, "toolCallId"));
          if (tool) tool.result = true;
          break;
        }
      }
    }
    for (const messageId of openText) {
      this.#appendEvent(threadId, runId, { type: EventType.TEXT_MESSAGE_END, messageId });
    }
    for (const [toolCallId, tool] of tools) {
      if (!tool.ended) this.#appendEvent(threadId, runId, { type: EventType.TOOL_CALL_END, toolCallId });
      if (!tool.result) {
        this.#appendEvent(threadId, runId, {
          type: EventType.TOOL_CALL_RESULT,
          messageId: randomUUID(),
          toolCallId,
          role: "tool",
          content: SYNTHETIC_TOOL_RESULT,
        });
      }
    }
  }

  #appendEvent(threadId: string, runId: string, event: BaseEvent): void {
    this.#insertEvent.run(threadId, runId, JSON.stringify(event), threadId, runId);
  }

  #transaction<T>(work: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }

  #userTexts(input: RunAgentInput): Map<string, string> {
    const texts = new Map<string, string>();
    for (const message of input.messages) {
      if (message.role !== "user") continue;
      const text = userText(message.content);
      if (typeof message.content !== "string" && message.content.some(part => part.type !== "text")) {
        console.warn(
          `Skipping non-text content parts in user message ${message.id} of thread ${input.threadId} run ${input.runId}`,
        );
      }
      texts.set(message.id, text);
    }
    return texts;
  }

  #threadDir(threadId: string): string {
    return join(this.#sessionDir, "threads", createHash("sha256").update(threadId).digest("hex"));
  }

  #sessionFile(threadId: string): string {
    return join(this.#threadDir(threadId), "session.jsonl");
  }
}
