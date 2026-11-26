# Changelog

What changed in behaviour, in the order it shipped. Dates are publish dates from the registry.

Nothing below 0.4.0 was ever published; the `0.3.0` that lingered in `package-lock.json` was a
stale lock, not a release.

## Unreleased

Built for clients that cannot hold a call open or watch a stream — the ChatGPT and Claude mobile
apps, where the request can close before a long Pi turn ends — so you can start work from a phone,
walk away, and be told when it is done without polling. All of it is opt-in: nothing changes for an
operator who sets none of the variables below.

- **Opt-in durable async executions.** Behind `PI_MCP_ASYNC_STATE_DIR`, the server gains `pi_start`,
  `pi_status`, `pi_wait`, `pi_result`, and `pi_cancel`; unset, it exposes the original six tools and
  behaves identically. A started execution is durably recorded before a model is reached,
  persists state, bounded progress projections, and the settled final in a private store, and
  always runs the `rpc` transport. `pi_wait` returns the `pi_status` payload, blocking up to
  `timeout_ms` (0–20000, default 5000) for terminal state; cancelling the wait does not cancel the
  execution. On restart, orphans are reconciled as `interrupted` with their partials intact and
  nothing is ever auto-replayed. Cleanup is verified before a session lane frees: a turn is over
  only once its whole process group is confirmed empty, and an unconfirmable group fails closed,
  marks the execution `failed`, and quarantines the session until the group is gone. One
  supervisor owns a state directory via an exclusive lock; a first store failure fails the whole
  async subsystem closed (`store_unavailable`) while the synchronous tools keep working.
- **Lifecycle and startup diagnostics.** A run that dies before settlement is reported with a typed
  classification — spawn error, abnormal exit, clean exit without `agent_settled` — and a bounded
  `process_diagnostics` block (spawn error code, exit code or signal, `rpc_prompt_acknowledged`);
  never raw stderr or RPC payloads. Shutdown holds the async store lock until parked writes are done
  and every pi process group is confirmed empty, and a mid-run teardown with closed stdio releases
  the lock without manual removal.
- **Opt-in completion hook.** `PI_MCP_COMPLETION_HOOK` names an absolute path to an operator-owned
  executable invoked at most once, best-effort, after a durably committed nonterminal → terminal
  transition, with exactly two arguments (execution id, terminal state), no shell, an empty
  environment, and a bounded whole-group reap on timeout. No delivery guarantee, no retry, no replay;
  the server logs only a bounded one-line status and never reads the wrapper's output.
- **Fail-closed hardening across the async path.** A nonblank malformed line on pi's protocol stdout
  fails the run — sync and async alike — even when the surviving events settle the turn; the
  diagnostic reports the count, never the line. A malformed EOF tail with no trailing newline
  fails it too, as does a line dropped for exceeding the capture limit — content the reader never
  saw is a contract violation, not a success. Reload validates a record's final against the exact
  shape the writer emits and refuses the record otherwise. Retention mutations (event expiry,
  tombstone commit, delete) propagate their failures: the store goes unhealthy, the triggering
  request reports `store_unavailable`, live jobs are cancelled and nothing further dispatches —
  never a stale read or an expiry result that was never committed. `pi_start` resolves `cwd` on an
  idempotency-key miss only, so an identical retry still finds its execution after the directory
  disappears.

## 0.7.1 — 2026-09-08

- Every tool declares MCP `annotations` — `readOnlyHint`, `destructiveHint`, `idempotentHint`,
  `openWorldHint` as explicit booleans matching what the handler actually does. Hosts use them to
  warn before invoking, and directories (OpenAI's among them) reject a tool that leaves any of the
  four out.

## 0.7.0 — 2026-09-02

- **No default run deadline.** The server-wide wall clock (`PI_MCP_TIMEOUT_MS`) defaulted to 30
  minutes, killing long work at a point unrelated to the task. It now defaults to unset: pi runs
  until it finishes unless the caller sets `timeout_ms` for that leg or an operator installs a
  server-wide default. A changed default is a behaviour change, so this is a minor rather than a
  patch.

## 0.6.0 — 2026-09-02

- **Higher defaults for the two limits that gate how much work one server can hold.** Concurrent pi processes (`PI_MCP_MAX_CONCURRENT`) default to 100 instead of 4, and remembered sessions (`PI_MCP_MAX_SESSIONS`) to 1000 instead of 200. Changed defaults are behaviour changes, so this is a minor rather than a patch.
- An empty completion is attributed to the model instead of the protocol. A turn can settle with
  `stopReason: "stop"` and an entirely empty content array — a well-formed stream where the model
  said nothing. The response used to claim the event stream did not match the expected contract,
  sending the reader after a protocol bug that was not there.

## 0.5.1 — 2026-08-22

- A failed turn reports pi's own reason. pi's error-termination contract puts it in `errorMessage`
  on the failing message; that field was never read, so a session that could not be read after the
  disk filled, or a `400 invalid params` from the provider, both arrived as "no usable answer text".
- README stopped restating what `AGENTS.md` and `.agents/skills/` own.

## 0.5.0 — 2026-08-21

- **`rpc` is the default transport.** It is a superset of `print`: same event stream, same answer,
  plus a running turn stays reachable and an interrupted one is closed in-protocol. `print` remains
  available per call and through `PI_MCP_TRANSPORT`.
- Tool descriptions rewritten around the choices a model gets wrong: new task versus continue versus
  reach-a-running-turn, which lister covers which state, that the prompt is read by an agent which
  cannot see the conversation, and that pi edits files and runs shell commands unless restricted.
- Nothing pi left detached outlives its request. A process group is not empty just because pi
  exited, so the group is now swept on a clean finish too, not only after a kill.
- The handshake version is read from the manifest. It had been hard-coded and was a release behind.

## 0.4.0 — 2026-08-21

- **A killed run is resumable instead of lost.** Timeout, cancellation and non-zero exit return the
  session id, the last thing pi said, what it did, and the `pi_reply` call to continue with. The
  session is recorded before the run rather than after it succeeds.
- **`timeout_ms` per call.** A single global deadline ended long work at a point unrelated to the
  task.
- **A running turn can be sent a message.** `pi_send` passes pi's own `steer` / `follow_up` /
  `abort` into a live rpc turn; `pi_running` lists what is reachable. Nothing is ever sent on the
  server's initiative.
- An interrupted rpc turn is closed with pi's `abort` before any signal, because pi skips its stdout
  flush on `SIGTERM` and signalling first can cost the tail of the stream.
- stderr lines that parse as protocol events are tallied by type instead of pasted in with their
  payloads and prompts.
- Rewritten in TypeScript, with pi's wire types borrowed rather than re-described, and type tests
  that fail when pi's own types move. Runtime dependencies: none.
