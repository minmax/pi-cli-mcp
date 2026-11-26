# pi-cli-mcp

MCP server that delegates coding tasks to your **locally installed** [pi](https://pi.dev) CLI.

It wraps the real `pi` binary instead of bundling its own copy of the agent, so every call inherits
your `~/.pi/agent/settings.json` — provider, models, thinking level, extensions, `AGENTS.md` /
`CLAUDE.md` discovery. Nothing about your model stack is duplicated here, and the server does not
drift when you upgrade pi.

Use it when your primary agent (Claude Code, Cursor, any MCP client) should hand work to pi: a
second opinion from a different model, an investigation you want kept out of the main context
window, or parallel work.

## Install

```bash
npx -y pi-cli-mcp     # no install
npm install -g pi-cli-mcp    # or global
```

Requires Node ≥ 22 and a working `pi` on `PATH` (`npm i -g @earendil-works/pi-coding-agent`).

### Claude Code

```bash
claude mcp add-json pi -s user '{
  "type": "stdio",
  "command": "npx",
  "args": ["-y", "pi-cli-mcp"],
  "timeout": 3600000
}'
claude mcp list | grep '^pi:'      # expect: ✔ Connected
```

The generous `timeout` matters: the server applies no run deadline by default, and a real
delegated task can run for minutes.

### Any other MCP client

```json
{
  "mcpServers": {
    "pi": { "command": "npx", "args": ["-y", "pi-cli-mcp"] }
  }
}
```

Keep the server name short (`pi`): it becomes part of the tool names your model sees.

## Tools

| Tool | Purpose |
|---|---|
| `pi` | Start a pi session. Returns `[session: <uuid>]`, the answer, and stats. |
| `pi_reply` | Continue a session that is not executing — including one killed by a timeout. |
| `pi_models` | List reachable models (provider, id, context, max output, thinking, images). |
| `pi_send` | Send a message into a turn that is running right now (`rpc` transport only). |
| `pi_running` | List turns executing right now and reachable by `pi_send`. |
| `pi_sessions` | List known sessions, newest first, with their working directory. |

### `pi`

| Argument | Notes |
|---|---|
| `prompt` | Required. Must be self-contained — pi cannot see your conversation. |
| `cwd` | Absolute path. pi reads `AGENTS.md` / `CLAUDE.md` from here. |
| `model` | e.g. `bifrost/minimax/MiniMax-M3`, `sonnet`, `provider/id:thinking`. |
| `thinking` | `off` … `max`. No-op on models without thinking support — check `pi_models`. |
| `timeout_ms` | Wall clock for this run. Off unless you set it — the task decides whether it needs a deadline. |
| `tools` | Allowlist, e.g. `read,grep,find,ls` for a read-only run. |
| `no_tools` | Pure reasoning over the prompt text. |
| `system_prompt_append` | Extra text appended to pi's system prompt. |

```js
pi({
  prompt: "Map how retries are wired in src/http.rs. Report call sites only.",
  cwd: "/abs/path/to/repo",
  tools: "read,grep,find,ls"
})
```

> **pi has no permission system.** With its default tools it edits files and runs shell commands as
> your user inside `cwd`. Pass `tools` or `no_tools` whenever the task is analysis. Use
> `PI_MCP_WRAP` if you want a sandbox.

## What comes back

Only pi's final answer plus aggregate stats — never the transcript, tool arguments or tool output:

```
[session: 0927adc5-a840-4b68-93ca-5ca344c9fafb]

Created note.md containing "hello" and updated target.txt to read "new content".

---
pi: bifrost/minimax/MiniMax-M3 · 5 turns · 4 tool calls: bash, read, write, edit · 11k in / 276 out · 9.8s
pi wrote: note.md, target.txt
```

- **"Final answer" is defined by `stopReason`**, not by position: the last assistant message that
  settled — the last one whose `stopReason` is not `toolUse`, which is how pi marks tool-call steps.
  Mid-run narration is dropped even when a preamble shares a message with a tool call. If the
  settled message has no text, that is reported as a broken run rather than silently reaching back
  for an earlier preamble. If nothing settled at all, the last text produced is returned, labelled
  as such.
- **The answer is never truncated.** Set `PI_MCP_MAX_OUTPUT` if you want a cap. Only diagnostics
  are bounded.
- **`pi wrote:` appears only when pi actually wrote files**, so it doubles as a side-effect check.
- **Bad `stopReason` fails the call, fail-closed.** `stop` / `length` are success; `error`,
  `aborted`, a missing `stopReason`, and anything outside the known vocabulary are reported as
  errors with the answer still attached. The `stopReason` that is validated belongs to the message
  being returned, not to whichever event arrived last. pi can exit 0 on a turn that did not settle
  cleanly, so the exit code alone is not trusted.
- **Raw stdout is never returned as an answer.** If the event stream does not match the expected
  contract, the response says so and describes the shape of what arrived (message count,
  `stopReason` values, tool-call count, byte count) — never the transcript itself, which would leak
  narration, tool arguments and tool results.

## Sessions

`pi` returns a session id; `pi_reply` continues it. The conversation lives in pi's own session
files, so follow-ups keep working across restarts of this server — the session → directory map is
persisted in `~/.local/state/pi-mcp/sessions.json`.

Concurrent replies to one session are serialized: two pi processes writing one session file would
corrupt it. If an id is unknown, pi starts a fresh conversation and the answer carries an explicit
`[warning: no existing session …]` rather than pretending to continue.

**Cross-process caveat.** The session mutex is process-local. If you run two MCP clients against
two server processes and both reply to the *same* session id at the same time, nothing serializes
them. The state file is written with re-read-then-merge, so sessions learned by one process are not
erased by the other, but the underlying pi session file has no such protection. In practice one
client owns a session; if you need a hard guarantee, keep one server process.

## Transports

Two ways to drive pi, behind one interface. Everything downstream — the event
accumulator, answer selection, the stats line, failure reporting — is shared, so
the choice changes only how pi is launched and what is possible during a run.

| | `rpc` (default) | `print` |
|---|---|---|
| command | `pi --mode rpc` | `pi -p --mode json` |
| process | stays up, reads JSONL commands on stdin | one per turn, exits when done |
| mid-run message | `pi_send` | impossible: pi reads nothing while working |
| interrupted by deadline or cancel | pi's own `abort` first, signals only as fallback | SIGTERM, then SIGKILL |
| prompt delivery | inside the command, no argv limit | argv, with a temp file for long or dash-leading prompts |

`rpc` is the default because it is a superset: the same event stream and the same answer, plus a
running turn stays reachable and an interrupted one is ended in-protocol. Pick per call with
`transport`, or set the default with `PI_MCP_TRANSPORT=print`.

### Reaching a running turn

`pi --mode rpc` accepts commands while it works. That is the whole reason the
transport exists, and it is exposed as pi's commands, unchanged:

A call blocks until the turn settles, so reaching it needs a second caller — or a client that stops
waiting. Claude Code, for one, moves a tool call to the background after about two minutes, and from
that point the turn is reachable from the same conversation:

```js
pi({ prompt: "long task…", cwd: "/repo" })   // moved to the background by the client

pi_running()
// 1 running:
// 5aef3387-…  8.0s  /repo

pi_send({ session: "5aef3387-…", message: "stop and report what you have" })
// Sent steer to session 5aef3387-… (running for 8.1s).
```

The answer appears in the call that is still waiting on that turn:

```
[session: 5aef3387-…]

STEERED_LIVE

---
pi: bifrost/agnes/agnes-2.5-flash · 3 turns · 3 tool calls: bash×3 · 23k in / 207 out · 46.6s
```

`command` selects which pi command to pass: `steer` (default, interrupts the
current turn), `follow_up` (queues for after it), `abort` (stops it).

### Ending a turn early

A deadline or a cancellation ends an rpc turn with pi's own `abort` first, and
only signals if that does not take within `PI_MCP_ABORT_GRACE_MS`. The reason is
in pi's own shutdown path: on SIGTERM it deliberately skips `flushRawStdout()`,
so signalling straight away can cost the tail of the event stream — including the
answer pi was in the middle of writing. After `abort` the turn closes through the
normal path and its report arrives. Print mode has no stdin to talk to, so there
it is SIGTERM then SIGKILL as before.

The report says which happened: `the turn was aborted` means it closed itself and
the events are complete, `the process was killed` means it was cut off.

No message is ever sent into a running turn on this server's own initiative:
`pi_send` fires only when you call it.

## When a run dies

A run killed by its deadline, by cancellation, or by a non-zero exit is not a
dead end — pi keeps the conversation in its own session file, so the work is
parked rather than lost. The failure carries what is needed to pick it back up:

```
[session: 0927adc5-…]

[error: pi timed out after 1800000 ms and was killed]

last thing pi said:
43 tests pass, now showing the failure

progress before it died:
pi: bifrost/zai/glm-5.3 · 9 turns · 14 tool calls: bash×6, read×5, edit×3 · 61k in / 4.2k out · 1800.0s
pi wrote: tests/test_upstream.py, conftest.py

The session is intact and resumable — pi still has every turn above.
To continue where it stopped:
  pi_reply({ session: "0927adc5-…", prompt: "..." })
Raise the limit for the next leg with timeout_ms if the task needs longer.
```

The session is recorded **before** the run starts, not after it succeeds, so a
killed run is still listed by `pi_sessions` and still resumable. The files line
comes from pi's own tool calls — this server does not inspect the filesystem.

`timeout_ms` exists because the right deadline belongs to the task. By default
the server sets no deadline at all — pi runs until it finishes. Give the task a
wall clock when it needs one, or install a server-wide default with
`PI_MCP_TIMEOUT_MS`; a global limit would otherwise kill long work at an
arbitrary point, while per-call it is a decision, and the report above makes
the decision recoverable either way.

### stderr

stderr is diagnostics, and only the tail is forwarded (`PI_MCP_STDERR_LIMIT`).
By the protocol, events belong on stdout, so a stderr line that *parses* as an
event is a channel violation: those lines are counted by type and reported as
`[6 protocol event line(s) on stderr, suppressed: message_start×3, message_end×3]`
rather than pasted in. The classification comes from parsing the line once and
reusing that parse for the tally — the payloads, prompts included, are never
forwarded. `PI_MCP_STDERR_KEEP_EVENTS=1` turns the guard off and forwards stderr
verbatim.

## Cancellation

MCP `notifications/cancelled` kills pi with `SIGTERM`, escalating to `SIGKILL` after a grace period.
Children go with it: pi runs in its own process group and the whole tree is signalled, so an
interrupted `sleep 120` does not survive even if pi fails to forward the signal.

Cancellation registers before the call queues for a concurrency slot or a session lock, so a call
that is cancelled while still waiting never starts pi at all.

Shutdown — stdin EOF, `SIGTERM`, `SIGINT`, `SIGHUP`, or a closed stdout — terminates every
running pi tree before exiting, and a tree is only dead once its process group is confirmed
empty: the leader exiting proves nothing (a descendant can ignore SIGTERM with the leader's
pipes already closed), so groups are probed to `ESRCH` after the SIGKILL. Detached children have
no other parent to clean them up. If a group cannot be confirmed empty in time, the async store
lock is deliberately left in place and the next supervisor says so — remove it by hand once the
leftovers are gone.

## Asynchronous executions (opt-in)

Set `PI_MCP_ASYNC_STATE_DIR` to an absolute path and the server gains five tools after the six
synchronous ones: `pi_start`, `pi_status`, `pi_wait`, `pi_result`, `pi_cancel`. Unset, the server exposes
exactly the original six and behaves identically to before.

`pi_start` validates like the `pi` tool, durably records the job, and returns `{execution_id,
session_id, state: "queued"}` before a model is reached or a process spawned. The execution belongs
to the server, not to the call: poll `pi_status` for state, page `pi_result` for ordered progress
and the final answer, or use `pi_cancel` to stop the execution. Queued jobs are withdrawn without
spawning; running ones are aborted with the same signal escalation as synchronous cancellation,
and `pi_cancel` waits for the process tree to be reaped. Async runs always use the `rpc` transport —
passing `transport` is rejected — because a background execution must stay steerable and abortable
while it runs.

`pi_wait` returns the same payload as `pi_status`. It returns immediately if the execution is
already terminal or `timeout_ms` is `0`; otherwise it waits for terminal state or until the
specified duration elapses, whichever comes first. `timeout_ms` is an optional integer from `0`
to `20000`, defaulting to `5000` when omitted. A timeout returns the latest status, not an error.
Cancelling the wait request does not cancel the execution.

Cleanup is verified at turn completion, not only at shutdown. A turn is not over when the leader
exits: its whole process group must be confirmed empty first, so a descendant that ignores SIGTERM
receives the SIGKILL escalation and the turn settles — the result is published, the session lane
frees, and `pi_cancel` returns `cancelled` — only after the group is gone. If the group cannot be
confirmed empty within the window, the turn fails closed: the execution lands on `failed` with a
`failure_reason` saying so instead of reporting a clean cancel or success — and the session is
quarantined. Returning from the run would free the session lane while a leftover may still be in
it, so until the sweep confirms the group empty, `pi_start` and `pi_reply` on that session are
refused (`session_quarantined`); the quarantine lifts by itself the moment the group is confirmed
gone. If the supervisor dies first, the shutdown escalation owns the group and holds the store
lock until it is empty, so a restarted supervisor cannot hand the session over either.

When Pi fails to start or exits before settlement, `pi_result` distinguishes a spawn error, an
abnormal exit, and a clean exit without `agent_settled`. The final's optional `process_diagnostics`
contains the spawn error code, exit code or signal when known, and `rpc_prompt_acknowledged`.
It never contains raw stderr or RPC payloads. A missing acknowledgement does not prove the prompt
was never acted on; the server does not automatically retry.

Progress events are a safe projection — short notes and provisional answer snapshots, clipped and
bounded — never the transcript. Pages are exclusive per execution: advance to `next_cursor` after
processing, deduplicate on `(execution_id, seq)`, drain `has_more` before stopping. Every page is
bounded by the bytes actually written: the whole JSON-RPC frame, UTF-8 bytes with the real JSON
escaping, the payload counted in both its representations. A cursor past retention is reported as
`cursor_expired` with the page resuming at the oldest retained event; event loss inside a page
window is marked by a `gap` event instead of being silently dropped. The settled answer rides any
page it fits on, and always the first page a cursorless call sees — one call recovers the final
without replaying the sequence. No page is exempt from the byte cap, the last one included: a final
too large for the frame is clipped to fit and marked `final.page_clipped` (the stored final stays
whole; recover it without widening anything with `pi_result(final_only=true)`, which pages just
the retained final by UTF-8 bytes — the clipped page's `recovery_hint` says exactly that). A
terminal record without an answer reports `final: null` on every page. The cap is enforced on the
exact frame as it is written, newline included, and nothing oversized is ever emitted: when even an
empty page cannot fit — a cap set below the envelope, or a request id so large its echo alone
overflows it — `pi_result` refuses with the bounded typed error `page_too_small` instead of
pretending the caller got a page. One exemption, stated rather than smuggled: the JSON-RPC request
id is echoed as the protocol requires and is the caller's choice, so it is not counted against the
cap. With a request id too large to fit beside a page — larger than the cap itself, even — the
refusal frame carries that id and a few hundred bytes of our own content, and that is all; the
refusal message says which side of the frame is over.

Optional `idempotency_key` makes retries safe: same key + same request returns the original
execution (even after a restart, even if the session map has since evicted the entry); same key +
different request is rejected as `idempotency_conflict`.

**Restart semantics.** Durable state lives only in the state directory (records, projections,
finals — never prompts or credentials; the directory is created `0700` and records are `0600`). A
supervisor that dies leaves orphan executions behind; on startup they are reconciled as
`interrupted` with their committed partials intact, and nothing is ever auto-replayed — a lost prompt
may have edited files. The session survives, so `pi_reply` can continue the conversation.

**One supervisor per state directory.** Ownership is an exclusive lock directory. A supervisor
going away cleanly holds the lock until its parked writes are done *and* its pi process groups are
reaped, so a successor starting inside the cleanup window is refused. A supervisor that was
killed outright leaves the lock behind: confirm the old owner and its pi children are gone, remove
`<state-dir>/lock` by hand, and restart — startup refuses to open a store it cannot prove it owns,
and every async tool then reports `store_unavailable` while the six synchronous tools keep
working.

**Stable upstream required.** Async executions are only as durable as the process that owns them:
this is a stdio server, so a client that disconnects closes its stdin unless a gateway (MCP Proxy or
similar) keeps the server process alive across client connections. Point the gateway at one
server and one state directory; two servers on the same state directory are never supported, and
the lock is what enforces it.

The first store failure — a refused rename, a failed fsync — fails the whole async subsystem
closed: live async children are reaped, queued plans are never dispatched, and every async tool
reports `store_unavailable` rather than claim a state that was not committed. The synchronous tools
are untouched by any of it.

### Completion hook (opt-in, off by default)

The adapter sends nothing on its own. If you want to be told when a background execution finishes,
set `PI_MCP_COMPLETION_HOOK` to the absolute path of an executable you own. It is invoked at most
once, at the boundary where an execution's nonterminal → terminal transition has become durable —
after the record is written, fsynced and renamed — with exactly two arguments: the execution id and
the terminal state (`succeeded`, `failed`, `cancelled`, or `interrupted`, the last also when
startup recovery reconciles an orphaned run). At most once, not exactly once: an invocation can be
skipped (the in-flight bound below) or lost (a crash between the commit and the spawn), and
neither case is ever retried. It is spawned directly, no shell, detached in its own
process group, with an **empty environment**: nothing inherited from the server or its gateway
reaches it, so put every path, URL and credential the wrapper needs inside the wrapper (or a
private `0600` file it reads) — never in the environment.

What it is *not*:

- **Not a notification channel.** The adapter knows nothing about Apprise, URLs or credentials;
all delivery logic lives in your wrapper. The server never reads or logs the wrapper's output —
only a bounded one-line status (a failed start, a nonzero exit, a timeout).
- **Not durable.** Delivery is best-effort: at most one attempt per durably committed qualifying
transition. A crash between the durable commit and the invocation loses the notification — the
next startup sees an already-terminal record and stays silent. No exit status confirms delivery.
- **Not replayed.** Same-state retention commits, repeated `pi_status`/`pi_wait`/`pi_result`
reads, repeated cancels, already-terminal records loaded at startup, and failed commits (before
*or after* the atomic rename) never invoke the hook.

Bounds and shutdown:

- Each invocation gets `PI_MCP_COMPLETION_HOOK_TIMEOUT_MS` (default `10000`; values below `100` fall
back to the default with a one-line warning; values above `600000` are capped — larger ones would
overflow Node's timers and fire immediately instead of never);
past it the whole process group is SIGTERMed and then SIGKILLed — descendants included, so a
wrapper that hands off to Apprise and hangs cannot linger. A run is only forgotten once probing
its group is ESRCH (every member gone); a SIGKILL that cannot be confirmed inside the bounded
window is reported on stderr, never assumed to have worked.
- At most 8 invocations run at once; further qualifying transitions are skipped with a logged
note (skipped means never attempted — the at-most-one rule holds either way).
- At shutdown, in-flight hooks get the same grace window as pi's own trees
(`PI_MCP_KILL_GRACE_MS`) to finish delivering; what is still running at the end of it is
force-reaped, and both confirmations — every pi tree and every hook group — are awaited
together within the same bounded window (`PI_MCP_TREE_GONE_MS`) before the server exits; only the pi trees gate the store handover,
so an unconfirmed pi group keeps the lock in place (the next supervisor is told to remove it by
hand once the leftovers are gone), while an unconfirmed hook group is only logged — the lock is
still handed over, and the last delivery is not trusted. Nothing is replayed. The
`interrupted` commit the shutdown itself makes is attempted too, and gets that same window to
deliver — whether it lands depends on the wrapper finishing inside it. Nothing is promised after a
SIGKILL of the server itself: only a clean shutdown reaps hooks.
- A hook failure never delays a tool result, never mutates a record, and can never fail a commit:
the invocation happens strictly after the state is durable.

Example: an Apprise wrapper (illustrative only — no real destination or credentials). The adapter
knows nothing about Apprise, so the wrapper owns every detail. A complete setup is two files,
plus one dry run to prove where a tag lands.

The private config, `0600`, readable only by the account the server runs under —
`/usr/local/etc/pi-mcp/apprise.conf`:

```sh
# Apprise TEXT config. Tag syntax is a PREFIX: tag=url. A trailing ", tag" after the
# URL attaches nothing, matches no tag, and fails silently — keep the tag before the '='.
pi-completion=pover://<PushoverToken>@<PushoverUser>/
# Untagged lines are outside the hook's reach: --tag pi-completion never selects them.
desktop=macosx
```

The wrapper, `0700` — `/usr/local/lib/pi-mcp/notify.sh`:

```sh
#!/bin/sh
# /usr/local/lib/pi-mcp/notify.sh — invoked as: notify.sh <execution-id> <state>
# The environment is EMPTY: every path is baked in here, nothing is inherited.
CONFIG=/usr/local/etc/pi-mcp/apprise.conf   # 0600, private: Apprise URLs/credentials live
                                            # here — never in argv, never in the server's env
[ "$#" -eq 2 ] || exit 64                   # the contract is exactly two arguments
[ "$2" = succeeded ] || [ "$2" = failed ] || [ "$2" = cancelled ] || [ "$2" = interrupted ] || exit 64
exec /usr/local/bin/apprise --config "$CONFIG" --tag pi-completion \
    --title "pi execution: $2" --body "execution $1 finished with state $2"
# All four states are delivered; filter on $2 inside the case above for a subset.
# exec preserves apprise's exit status: a nonzero exit is logged by the server as one
# bounded line (the code, never the output), and delivery is never retried either way.
```

Prove the destination locally before wiring it in — a dry run sends nothing and prints exactly
what a tag would reach:

```console
$ apprise --dry-run --disable-async --tag pi-completion
… pover://… (tags: pi-completion)

$ apprise --dry-run --disable-async            # no tag: only the untagged lines
… macosx

$ apprise --dry-run --disable-async --tag all # everything the config can reach
```

Exactly one destination is the goal: the tagged line above pins the hook to a single recipient,
and the dry run is how you check it stayed pinned. The argument guards make accidental invocations
harmless too — the `exit 64` paths run before apprise is ever reached, so a stray call with the
wrong shape never sends anything.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `PI_MCP_BIN` | `pi` | Path to the pi binary. |
| `PI_MCP_MODEL` | pi's setting | Default model for every call. |
| `PI_MCP_THINKING` | pi's setting | Default thinking level. |
| `PI_MCP_TIMEOUT_MS` | unset | Server-wide default wall clock; unset means no deadline. `timeout_ms` overrides it per call. |
| `PI_MCP_MAX_TIMEOUT_MS` | `86400000` | Ceiling on what `timeout_ms` may ask for. |
| `PI_MCP_MAX_CONCURRENT` | `100` | Concurrent pi processes. |
| `PI_MCP_MAX_OUTPUT` | unset | Cap on the answer. Unset means no truncation. |
| `PI_MCP_STDERR_LIMIT` | `1500` | stderr tail included in the response. |
| `PI_MCP_STDERR_KEEP_EVENTS` | unset | `1` forwards stderr verbatim, event lines included. |
| `PI_MCP_MAX_CAPTURE` | `16000000` | Read-buffer guard against a runaway stream. |
| `PI_MCP_MAX_LINE` | `8000000` | Longest single event line from pi before it is dropped. |
| `PI_MCP_MAX_FRAME` | `8000000` | Longest single JSON-RPC frame from the client. |
| `PI_MCP_MAX_SESSIONS` | `1000` | Remembered sessions before the oldest is dropped. |
| `PI_MCP_KILL_GRACE_MS` | `5000` | SIGTERM → SIGKILL grace period. |
| `PI_MCP_TREE_GONE_MS` | `5000` | How long shutdown waits, after SIGKILL, to confirm each pi process group is empty before handing over the async store lock. Unconfirmed groups keep the lock in place. |
| `PI_MCP_ABORT_GRACE_MS` | `5000` | How long `abort` gets before signals (rpc only). |
| `PI_MCP_STATE` | `~/.local/state/pi-mcp/sessions.json` | Session → cwd map. |
| `PI_MCP_WRAP` | unset | Command prefix, e.g. `sandbox-exec -f profile.sb`. |
| `PI_MCP_TRANSPORT` | `rpc` | Default transport: `rpc` or `print`. |
| `PI_MCP_ASYNC_STATE_DIR` | unset | Absolute path; set to enable `pi_start`/`pi_status`/`pi_wait`/`pi_result`/`pi_cancel`. Private durable store, `0700`. |
| `PI_MCP_ASYNC_MAX_QUEUED` | `32` | Pending async executions across all sessions. |
| `PI_MCP_ASYNC_MAX_QUEUED_PER_SESSION` | `8` | Pending async executions per session. |
| `PI_MCP_ASYNC_MAX_QUEUED_BYTES` | `16777216` | UTF-8 budget for queued plans held in memory (prompt + overrides). |
| `PI_MCP_ASYNC_MAX_RECORDS` | `1024` | Stored executions before the oldest terminal records are pruned. |
| `PI_MCP_ASYNC_MAX_STORE_BYTES` | `67108864` (64 MiB) | Store size cap. Admission reserves each live execution's full serialized growth — event ring + final + scaffolding, doubled for the atomic-rename tmp write — against it, so accepted work always fits. |
| `PI_MCP_ASYNC_PENDING_EVENTS` | `512` | Unflushed events per execution; overflow becomes a marked `gap` event. |
| `PI_MCP_ASYNC_MAX_EVENTS` | `256` | Retained events per execution before the oldest are dropped (detectable via `oldest_retained_seq`). |
| `PI_MCP_ASYNC_MAX_EXEC_EVENT_BYTES` | `262144` | Retained event bytes per execution. |
| `PI_MCP_ASYNC_PAGE_EVENTS` | `50` | Default `pi_result` page size (events). |
| `PI_MCP_ASYNC_PAGE_BYTES` | `131072` | Cap on the exact `pi_result` frame as written to the wire: UTF-8 bytes, both response representations, envelope, and the trailing newline included — no page exempt, the final clipped and marked `page_clipped` (with a `recovery_hint` pointing at `final_only`) when it must be; a cap that cannot carry even an empty page is refused with the bounded typed error `page_too_small`. The echoed request id is the one exemption: JSON-RPC requires it and the caller chose it, so it is not counted — a refusal beside an oversized id carries the id plus bounded content and nothing else. |
| `PI_MCP_ASYNC_EVENT_TEXT_LIMIT` | `32768` | Clip, in UTF-8 bytes, on a single projection event (kept at or below an eighth of the page budget). |
| `PI_MCP_ASYNC_MAX_FINAL_BYTES` | `1048576` | Cap on the stored final, measured as its serialized UTF-8 bytes — JSON escaping included, not raw characters. |
| `PI_MCP_ASYNC_FLUSH_MS` | `2000` | Interval for committing progress projections. |
| `PI_MCP_ASYNC_PARTIAL_RETENTION_MS` | `86400000` | How long committed events are kept after the execution ends. |
| `PI_MCP_ASYNC_FINAL_RETENTION_MS` | `604800000` | How long the final answer and the idempotency key are kept. |
| `PI_MCP_ASYNC_TOMBSTONE_RETENTION_MS` | `604800000` | Extra grace for expired keys before the record is deleted. |
| `PI_MCP_COMPLETION_HOOK` | unset | Absolute path to an operator-owned executable, invoked at most once with `(execution_id, terminal_state)` after the durable terminal transition of an async execution. Unset (or not absolute): nothing is ever invoked. Best-effort, empty environment, output never read. |
| `PI_MCP_COMPLETION_HOOK_TIMEOUT_MS` | `10000` | Grace before a hook's whole process group is terminated (SIGTERM → SIGKILL after 500 ms, then a bounded group-empty confirmation), descendants included. Values below `100` fall back to the default; hard-capped at `600000`: larger values would overflow Node's timers and fire after ~1 ms instead. |

## Design

- **Process per call.** pi's own session files are the source of truth, which is what makes
  follow-ups survive a restart of this server.
- **`pi -p --mode json`.** The json event stream is what yields turns, tool calls, token usage and
  cost — no scraping of human-readable output.
- **No dependencies.** Newline-delimited JSON-RPC 2.0 is spoken directly; installing this package
  pulls nothing else in.
- **Long or dash-leading prompts** are passed as an `@file` attachment, since pi has no `--`
  separator and argv has an OS size limit.

### Why not the alternatives

`pandysp/pi-mcp-server` depends on `@mariozechner/pi-coding-agent@^0.52.9` — the old fork under
pi's previous package name — so it runs a bundled copy of a much older agent instead of your CLI,
and knows only a fixed provider list. Everything else in the ecosystem (`pi-mcp-adapter`,
`pi-mcp-extension` and forks) runs the opposite direction: MCP servers *into* pi. `pi` itself has
no native `mcp-server` subcommand.

## Development

TypeScript, mirroring pi's own toolchain — one version newer where there is a newer one.

| | pi 0.84 | here |
|---|---|---|
| compiler | `tsgo` dev-preview + typescript 5.9 | **typescript 7** (`tsc`, the native compiler, stable) |
| lint / format | Biome 2.3.5, `recommended: true` | **Biome 2.5.9**, `preset` (the field that replaced it) |
| tests | Vitest 4.1.9 | **Vitest 4.1.11**, with `--typecheck` on |
| module | `Node16` | **`nodenext`** |
| strictness | `strict`, `erasableSyntaxOnly` | plus `verbatimModuleSyntax`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, `noImplicitOverride`, `noFallthroughCasesInSwitch`, `noUnusedLocals/Parameters` |

```bash
npm run hooks          # once per clone: git hooks from .githooks/
npm run build          # tsc -> dist/
npm test               # unit + type tests, no API access, no tokens
npm run check          # format + types + tests
npm run fix            # biome --write
PI_CLI_MCP_LIVE=1 npm test   # also exercise the real pi binary
```

What changed between versions is in [CHANGELOG.md](CHANGELOG.md). House rules are in
[AGENTS.md](AGENTS.md); procedures — releasing, validation, pi's contract, testing — in
[`.agents/skills/`](.agents/skills/).

## License

MIT
