# omp AG-UI adapter

A Bun HTTP/SSE bridge from [omp](https://github.com/can1357/oh-my-pi) SDK sessions to [AG-UI](https://docs.ag-ui.com/) 0.0.59. It exposes `POST /agent/run` for CopilotKit OpenBot and other AG-UI 0.0.59 clients; it does not modify omp. Runtime: Bun ≥1.3.14, `@oh-my-pi/pi-coding-agent@18.3.5`, `@ag-ui/core@0.0.59`, `@ag-ui/encoder@0.0.59`. The validation script uses `@ag-ui/client@0.0.59`.

**Security boundary:** omp tools execute on the **adapter host**, outside OpenBot's computer/governance gateway. OpenBot displays their calls and results; it does not approve or sandbox them. Even an empty `OMP_TOOLS` list does **not** disable omp's native `@file` prompt expansion, which can read host files. Treat every bearer-token holder as an operator with the configured host capabilities. Isolate the process and working directory at the OS/container level, and terminate TLS outside this server. The bearer token is shared deployment access, not per-user authorization.

## Run

```sh
bun install --frozen-lockfile
export AGUI_TOKEN="$(openssl rand -hex 32)"
export OMP_CWD="$PWD"
# Set both only when you want a particular available/authenticated model:
export OMP_PROVIDER=opencode-go
export OMP_MODEL=deepseek-v4.1-flash
bun run start
```

Supply provider credentials through omp's existing login/credential store or provider environment. The explicit model above was verified in one environment, not a universal credential requirement. Without either selector, the server selects and logs the first authenticated *chat* model; a catalogued model can still fail on its first provider request (the SDK's availability check is not an endpoint health check). An explicit unavailable selector fails startup instead of silently falling back. Default listener: `127.0.0.1:8789`; port 8789 avoids an occupied local 8787. Keep `.env` and `.omp-ag-ui/` private; both are gitignored. `PI_NO_TITLE=1` is set by the adapter before sessions are constructed.

| Environment | Default | Meaning |
|---|---|---|
| `AGUI_HOST` | `127.0.0.1` | Bind address; external binding is operator-controlled. |
| `AGUI_PORT` | `8789` | Integer 1–65535. |
| `AGUI_TOKEN` | required | Nonblank header-safe bearer token; never logged. |
| `AGUI_CORS_ORIGIN` | unset | One exact HTTP(S) browser origin; no wildcard. OpenBot's server-side connection needs no CORS setting. |
| `OMP_CWD` | launch cwd | Existing directory; resolved to its real path. All native tools run here. |
| `OMP_SESSION_DIR` | `<launch cwd>/.omp-ag-ui` | Private SQLite journal and native transcript directory. |
| `OMP_PROVIDER`, `OMP_MODEL` | unset | Both or neither; exact available chat model selector. |
| `OMP_TOOLS` | `read,grep,glob` | Comma-separated, trimmed/deduplicated allowlist. Empty disables registered tools. Supported: `read,grep,glob,edit,write,bash`. |

`read`, `grep`, and `glob` are conservative defaults, **not** a filesystem sandbox. Opting in to `edit`/`write` modifies files; `bash` can execute destructive commands. All opted-in tools run headlessly without per-call approval. Frontend-supplied AG-UI `tools` are not installed or called; a name collision with a configured native tool rejects a *new* run. Client `state` is ignored and `forwardedProps` is opaque, not authentication. System/developer text and context entries shape the current turn's system prompt; private reasoning, attached images/audio/video/documents, and shared UI state are not transported. Non-text user parts and non-text native results are skipped, not fetched.

## Request and official client

```sh
curl -N http://127.0.0.1:8789/agent/run \
  -H "Authorization: Bearer $AGUI_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"threadId":"example-thread","runId":"example-run","messages":[{"id":"example-user","role":"user","content":"Reply with hello"}],"tools":[],"context":[],"state":{},"forwardedProps":{}}'
```

```ts
import { HttpAgent } from "@ag-ui/client";

const agent = new HttpAgent({
  url: "http://127.0.0.1:8789/agent/run",
  headers: { Authorization: `Bearer ${process.env.AGUI_TOKEN}` },
  threadId: "a-private-thread-id",
  initialMessages: [{ id: "one-user", role: "user", content: "Reply with hello" }],
});
agent.subscribe({
  onTextMessageContentEvent: ({ event }) => process.stdout.write(event.delta),
  onRunErrorEvent: ({ event }) => console.error(event.code, event.message),
});
await agent.runAgent({ runId: "one-run" }); // Resolution alone does not imply success: inspect terminal events.
```

`RUN_STARTED` is durable before model setup; assistant text follows actual SDK `text_delta` events, not an artificial chunker. Assistant text has START/CONTENT/END lifecycle. Tool calls expose native names, complete JSON argument strings at `TOOL_CALL_ARGS` (not token-streamed arguments), and actual text/error results; tool-only assistant messages use the call's parent ID without fake text. A provider emitting only a completed text message produces one coarse CONTENT at message end, **not** measured token-time streaming. Tool errors may be handled by omp within the same run. The adapter does not expose thinking, raw provider data, or frontend tool execution.

## Persistent turns, replay, and cancellation

Each accepted `(threadId, runId)` is recorded once before SDK work. A thread's native omp JSONL transcript lives at `threads/<sha256(threadId)>/session.jsonl` under `OMP_SESSION_DIR`; `runs.sqlite` journals ordered AG-UI events. The first run imports text-only prior user/assistant/paired-tool messages once, with zero accounting for imported assistant messages; imported tools are **not** executed. Later turns reuse the native conversation, accept either the exact complete user prefix plus one new user or only the new user, and ignore client-side assistant/tool projections. Reusing a final user ID or editing the user prefix rejects the new turn. An explicit retry needs both a **new run ID and a new user-message ID**.

The same run ID and normalized input (all parsed fields except opaque `forwardedProps`) replays the recorded events/IDs from `RUN_STARTED` to the terminal without invoking omp again, even after later turns or changed configuration. An active matching run rejects with `RUN_ACTIVE`; different input with the same ID rejects with `RUN_INPUT_CONFLICT`. There is **no SSE cursor, `Last-Event-ID`, or automatic reconnect** in AG-UI 0.0.59/OpenBot. For a deliberate replay, reset the client to its pre-run message baseline or create a fresh `HttpAgent` with the original input messages; replaying into an already-assembled client appends duplicate text. OpenBot does not automatically reconnect to fill a disconnected stream.

`HttpAgent.abortRun()` or a disconnected live response latches cancellation and aborts the native session; teardown waits for native abort and idle before recording `RUN_ERROR/CANCELLED` and releasing the thread. A disconnected client cannot receive its terminal event, but the exact input can replay it later. Shutdown stops admission, cancels owners, waits for native work and terminal transactions, then closes sockets and the store. Restart closes unfinished journal runs with `SERVER_RESTARTED` and explicit uncertainty for an announced tool whose result was never recorded; it never reruns that tool. Before a *new* turn, an unmatched native tool-call tail gets an error result stating that execution outcome is unknown. No recovery result asserts that a command did or did not execute.

One writer owns a session directory at a time. Journal/native records grow indefinitely: stop the server before archiving or removing the **private** `OMP_SESSION_DIR`; there is no TTL/pruning. Context compaction, automatic retry, and model fallback are disabled. Model context is finite, and a failed run does not automatically retry or silently switch models.

## OpenBot coworker

Use a separate OpenBot project/deployment rather than reusing production credentials. In `/agents`, create a **private** coworker with name `OMP`, title `OMP Agent`, role description `Coding agent using its own configured host tools.`, endpoint `http://127.0.0.1:8789/agent/run`, authorization header name `Authorization`, and value `Bearer <AGUI_TOKEN>`. The endpoint must be reachable from the OpenBot **server** (its network namespace matters). Test connection with a real one-word response before saving. Native tool names such as `read` are rendered as OpenBot server-side tool lines; OpenBot's frontend tools remain separate. OpenBot forwards Stop to its remote agent, but its own event-stall watchdog ignores SSE heartbeat comments. No managed-agent header or separate authentication path is supported by this adapter.

## Evidence and checks

The initial **live SDK-only spike** used the explicitly selected `opencode-go/deepseek-v4.1-flash` with a private fixture. It observed 168 nonempty text deltas, a native `read` whose result contained the fixture nonce, reopening the exact native file and recalling the nonce without a second read, and abort after the first text delta with no subsequent deltas; `isStreaming` and pending async work were false after abort/idle. It also observed that the first model in the available catalog failed its real request with an OAuth 403 despite being listed as available. Complete-only provider behavior remains a source-supported fallback, not a live-provider spike measurement. The real HTTP smoke observed 401 for an incorrect bearer, `RUN_STARTED`, assistant text, one `RUN_FINISHED`, EOF, and byte-identical replay.

Run `bun run typecheck`, `bun test test/adapter.test.ts`, then with valid credentials `bun run validate --url http://127.0.0.1:8789/agent/run`. The latter uses official `HttpAgent` and exits nonzero if any terminal, tool, resume, abort, replay, or authorization assertion fails.

On this checkout, `bun run typecheck` passed and the serial real-SDK suite passed **21/21** cases. The official-client live script passed **34/34** assertions: 250 explanation deltas before `RUN_FINISHED`, native README `read`, exact delta-only nonce recall, recorded `CANCELLED` replay after client abort, stable completed-run replay, and 401 for a wrong token. Raw `curl -N` received `RUN_STARTED`, `hello` text lifecycle, one `RUN_FINISHED`, and EOF.

An isolated pinned OpenBot deployment in a separate Intelligence project registered a private OMP coworker. Its connection probe observed the full `RUN_STARTED` → text → `RUN_FINISHED` sequence. The channel showed incremental text, a settled `read` card/result containing this README's heading, and nonce recall across turns. In a separate OMP channel, a browser Stop click at `2026-09-27T21:35:42.995Z` during visible text was followed by that **same run's** journaled `CANCELLED` at `21:35:43.637Z` (0.642 s); the original input was replayable and a new channel turn answered. Private screenshot and ID/timestamp evidence live under gitignored `.validation/`. Earlier attempts that ended before the click do not count as Stop proof.

Platform caveats observed in this OpenBot revision: its frontend requests a single-route runtime-info envelope while the server exposes multi-route mode (browser console 404), although direct `GET /api/copilotkit/info` returned 200 and the OMP runtime ID matched the saved coworker. With computer services deliberately disabled, the optional computer-control route returned 404 and its banner said input was paused; chat sending, Stop, and subsequent turns worked. Bun rejects bodies over the configured 1 MiB `maxRequestBodySize` **before** the handler; that platform-generated 413 has an empty body, while handler-reached errors use `{ "error": { "code", "message" } }`.
