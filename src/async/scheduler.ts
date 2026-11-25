// The async supervisor: one serialized writer per execution, a bounded queue in
// front of the same session lanes and process slots the synchronous tools use,
// and the recovery rules that keep a lost supervisor from lying about work.
//
// Invariants this module owns (see Plan.md):
//   - nothing is dispatched before its acceptance record (and idempotency key,
//     if any) is durable;
//   - `running` is committed before spawn, and a cancellation that wins the
//     pre-spawn window commits `cancelled` with zero launches;
//   - the first serialized terminal decision wins: completion is immutable, a
//     committed `cancelling` beats a late completion;
//   - the first store failure fails the whole subsystem closed — no admissions,
//     no dispatch, live async children reaped, queued plans undispatched;
//   - prompts live only in bounded memory; the store never sees them.
//
// Every state decision and its commit happen in one synchronous section: the
// JS event loop cannot interleave two synchronous blocks, which is what makes
// the writer "one serialized writer" without an extra lock.

import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { type Accumulator, answerProblem, newAccumulator, type SelectedAnswer, selectAnswer } from "../answer.ts";
import {
	ASYNC_FLUSH_INTERVAL_MS,
	ASYNC_MAX_EVENTS,
	ASYNC_MAX_EXEC_EVENT_BYTES,
	ASYNC_MAX_QUEUED,
	ASYNC_MAX_QUEUED_BYTES,
	ASYNC_MAX_QUEUED_PER_SESSION,
	ASYNC_MAX_RECORDS,
	ASYNC_MAX_STORE_BYTES,
	ASYNC_PENDING_EVENTS,
	ASYNC_STATE_DIR,
	ASYNC_TEST_ACCEPT_GATE,
	ASYNC_TEST_SPAWN_GATE,
	asyncEventTextLimit,
	asyncFinalCap,
	DEFAULT_MODEL,
	DEFAULT_THINKING,
} from "../config.ts";
import { asPiEvent, eventAs, readAssistantMessage } from "../parse.ts";
import { makeCancelToken, type RunResult, withSlot } from "../pi-process.ts";
import { getSession, rememberSession, withSessionLock } from "../sessions.ts";
import { invokePi, resolveCwd } from "../tools.ts";
import { rpcTransport } from "../transport/rpc.ts";
import type { RunPlan } from "../transport/types.ts";
import type { CallContext, CancelToken, RunOverrides, ThinkingLevel } from "../types.ts";
import { isStopOk, isThinkingLevel } from "../types.ts";
import { notifyCompletion } from "./hook.ts";
import { quarantineSession, releaseQuarantine, sessionQuarantine } from "./quarantine.ts";
import { ExecutionStore, StoreUnavailableError } from "./store.ts";
import {
	AsyncToolError,
	CancelledBeforeAccept,
	type ExecutionRecord,
	type ExecutionState,
	type FinalOutcome,
	isTerminal,
	type ProcessDiagnostics,
	type ProjectionEvent,
	type StartIntent,
} from "./types.ts";

const SLEEP_MS = 20;

interface PendingEvent {
	type: ProjectionEvent["type"];
	text: string;
	truncated?: boolean;
	/** Set only on a gap marker: how many events the memory bound dropped. */
	dropped?: number;
	at: number;
}

interface Job {
	id: string;
	sessionId: string;
	cwd: string;
	prompt: string;
	overrides: RunOverrides;
	timeoutMs: number | undefined;
	/** Job-owned: no MCP request or progress subscription is attached to it. */
	token: CancelToken;
	/** Unflushed projection events, bounded in memory. Sequences are assigned
	 * at commit time, so the memory bound can never open an interior gap. */
	pending: PendingEvent[];
	/** Events dropped from `pending` by the memory bound since the last commit. */
	dropped: number;
	sawSettled: boolean;
	startedAt: number | null;
	flushTimer: NodeJS.Timeout | undefined;
	finished: Promise<void>;
	finish: () => void;
}

const jobs = new Map<string, Job>();
let queuedBytes = 0;
let store: ExecutionStore | null = null;
let configured = false;
let shuttingDown = false;
let failedClosed = false;

// --- lifecycle --------------------------------------------------------------

function warn(message: string): void {
	process.stderr.write(`pi-mcp: ${message}\n`);
}

/** True when the operator asked for async tools (env var set, any value). */
export function asyncConfigured(): boolean {
	return ASYNC_STATE_DIR !== null;
}

/** Open the store, reconcile recovered records, repair routing metadata. */
export function asyncInit(): void {
	if (!asyncConfigured()) return;
	configured = true;
	if (!ASYNC_STATE_DIR?.startsWith("/")) {
		warn(`PI_MCP_ASYNC_STATE_DIR must be an absolute path; async tools will report store_unavailable`);
		return;
	}
	store = ExecutionStore.open(ASYNC_STATE_DIR, notifyCompletion);
	if (store === null || !store.healthy) {
		// An unopened or unhealthy store is the same state for every caller:
		// the async tools report store_unavailable and the legacy tools are
		// untouched. Keeping a dead instance around would leak partial reads.
		store = null;
		warn("async execution store is unavailable; pi_start/pi_status/pi_result/pi_cancel will fail closed");
		return;
	}

	// Reconcile every persisted nonterminal record before serving requests: a
	// record without a terminal commit means its supervisor is gone, whatever
	// pi might have done in the meantime. No recovered prompt is ever replayed.
	for (const record of store.allRecords()) {
		if (isTerminal(record.state)) continue;
		try {
			store.commit({ ...record, state: "interrupted", finished_at: Date.now() });
		} catch {
			// The store failed closed; every call will now report it.
			break;
		}
	}

	// Routing repair: a session this adapter no longer remembers may still have
	// history on disk if an execution reached `running`. Only fill gaps — never
	// overwrite newer entries — and never claim history: pi_reply still warns
	// when pi itself has no such session.
	for (const record of store.allRecords()) {
		if (record.started_at === null) continue;
		if (getSession(record.session_id) !== undefined) continue;
		rememberSession(record.session_id, record.cwd, {
			model: record.run_options.model,
			thinking: record.run_options.thinking as ThinkingLevel | undefined,
		});
	}

	store.sweep();
}

/**
 * Shutdown, phase 1: stop admissions and dispatch, durably park live work.
 * The ownership lock is deliberately NOT released here — it is held until the
 * old pi process groups are reaped, so a supervisor starting in the cleanup
 * window cannot acquire the store while the dying one's children may still be
 * running. `asyncRelease` is phase 2, called by the server once the trees are
 * gone (or immediately if there were none).
 */
export function asyncPark(): void {
	if (!configured || store === null) return;
	shuttingDown = true;
	for (const job of jobs.values()) stopFlushTimer(job);
	for (const record of store.allRecords()) {
		if (isTerminal(record.state)) continue;
		try {
			// Park it with whatever partials were already committed. A
			// completion that lands during the kill grace sees a terminal
			// record and stays silent: the first serialized decision wins.
			store.commit({ ...record, state: "interrupted", finished_at: Date.now() });
		} catch {
			break; // unhealthy store: startup reconciliation owns it now
		}
	}
	for (const job of jobs.values()) job.token.cancel();
}

/**
 * Shutdown, phase 2: the last write has been parked and the pi process groups
 * are gone, so the next supervisor may acquire the store. A process that dies
 * before this point leaves the lock behind on purpose: startup reports it and
 * the operator clears it by hand after confirming the owner is gone.
 */
export function asyncRelease(): void {
	if (!configured || store === null) return;
	store.releaseLock();
}

// --- idempotency -------------------------------------------------------------

/**
 * Versioned fingerprint of the canonical request intent: fixed field order,
 * explicitly supplied values only (never generated ids, current time, or
 * environment defaults), and no mutable session-map resolution — a continuation
 * retry must resolve by key even after its session-map entry was pruned.
 */
function computeFingerprint(intent: StartIntent): string {
	const canonical: Record<string, unknown> = {
		v: 1,
		session_id: intent.sessionId,
		prompt: intent.prompt,
		// A continuation's cwd is the session's own directory, not part of the
		// caller's intent; only an explicitly supplied one is canonical.
		cwd: intent.sessionId === null ? intent.cwd : null,
		model: intent.model ?? null,
		thinking: intent.thinking ?? null,
		timeout_ms: intent.timeoutMs ?? null,
		tools: intent.tools ?? null,
		no_tools: intent.noTools === true ? true : null,
		system_prompt_append: intent.systemPromptAppend ?? null,
	};
	return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

/** UTF-8 bytes of everything a queued plan keeps in memory: the prompt, the
 * overrides snapshot, and fixed scaffolding headroom for the record itself. */
function planBytes(prompt: string, overrides: RunOverrides): number {
	return Buffer.byteLength(prompt, "utf8") + Buffer.byteLength(JSON.stringify(overrides), "utf8") + 256;
}

function countPending(): number {
	return jobs.size;
}

function countPendingForSession(sessionId: string): number {
	let count = 0;
	for (const job of jobs.values()) if (job.sessionId === sessionId) count += 1;
	return count;
}

// --- acceptance --------------------------------------------------------------

export interface AcceptedExecution {
	executionId: string;
	sessionId: string;
	state: ExecutionState;
}

/**
 * Validate-free acceptance: the caller has already run the strict shape and
 * size checks. This performs, in order: idempotency dedup (before anything
 * mutable or any capacity), mutable session/cwd checks on a key miss, capacity
 * checks, then the durable acceptance commit and dispatch.
 */
export async function accept(intent: StartIntent, requestToken: CancelToken): Promise<AcceptedExecution> {
	if (store === null) {
		throw new AsyncToolError("store_unavailable", "the async execution store is not configured");
	}
	if (!store.healthy || failedClosed) {
		throw new AsyncToolError("store_unavailable", "the async execution store is unavailable");
	}
	if (shuttingDown) {
		throw new AsyncToolError("store_unavailable", "the server is shutting down; no new executions are accepted");
	}

	const fingerprint = computeFingerprint(intent);

	// Deduplication first: a retry needs no new capacity, no new session, no
	// cwd existence check. A tombstone reserves the key through its window.
	if (intent.idempotencyKey !== null) {
		let hit: ReturnType<ExecutionStore["lookupKey"]>;
		try {
			hit = store.lookupKey(intent.idempotencyKey);
		} catch (err) {
			// A lookup can mutate: retention expiry commits. That mutation
			// failing is a store failure like any other.
			failClosed("a retention commit during key lookup failed");
			throw unavailable(err);
		}
		if (hit.kind === "tombstone") {
			throw new AsyncToolError(
				"execution_expired",
				"this idempotency key's result has expired; retry with a fresh key to create new work",
			);
		}
		if (hit.kind === "record") {
			if (hit.record.fingerprint !== fingerprint) {
				throw new AsyncToolError(
					"idempotency_conflict",
					"this idempotency key was already used for a different request",
				);
			}
			return {
				executionId: hit.record.execution_id,
				sessionId: hit.record.session_id,
				state: hit.record.state,
			};
		}
	}

	// Key miss: mutable checks. A continuation must name a session this adapter
	// knows, and inherits its directory and remembered options like pi_reply.
	let sessionId: string;
	let cwd: string;
	// The effective run options, resolved after the mutable checks: explicit
	// caller values, else the session's remembered settings on a continuation,
	// else the operator's env defaults. The fingerprint above never sees these.
	let model = intent.model;
	let thinking = intent.thinking;
	if (intent.sessionId !== null) {
		const known = getSession(intent.sessionId);
		if (known === undefined) {
			throw new AsyncToolError(
				"unknown_session",
				`session ${intent.sessionId} is not known to this adapter; start a new execution without session_id`,
			);
		}
		const blocked = sessionQuarantine(intent.sessionId);
		if (blocked !== undefined) {
			// The session's previous turn ended with a process group that could
			// not be confirmed empty. A new turn now could run beside whatever
			// is left in that group; refuse until the sweep confirms it is gone.
			throw new AsyncToolError(
				"session_quarantined",
				`session ${intent.sessionId} is blocked: the previous turn's pi process group could not be confirmed empty; retry once the leftover processes are confirmed gone`,
			);
		}
		if (intent.explicitCwd && intent.cwd !== known.cwd) {
			throw new AsyncToolError(
				"invalid_input",
				`cwd ${intent.cwd} does not match session ${intent.sessionId}, which lives in ${known.cwd}`,
			);
		}
		if (intent.noTools === true || intent.tools !== undefined || intent.systemPromptAppend !== undefined) {
			throw new AsyncToolError(
				"invalid_input",
				"tools, no_tools and system_prompt_append are only accepted when starting a new session",
			);
		}
		sessionId = intent.sessionId;
		cwd = known.cwd;
		model = intent.model ?? known.model;
		thinking = intent.thinking ?? known.thinking;
	} else {
		sessionId = randomUUID();
		cwd = resolveCwd(intent.explicitCwd ? intent.cwd : undefined);
		model = intent.model ?? DEFAULT_MODEL ?? undefined;
		thinking =
			intent.thinking ??
			(DEFAULT_THINKING !== null && isThinkingLevel(DEFAULT_THINKING) ? DEFAULT_THINKING : undefined);
	}

	// Capacity: the bounded queue counts everything nonterminal this process
	// owns, waiting on either its session lane or a process slot.
	if (countPending() >= ASYNC_MAX_QUEUED) {
		throw new AsyncToolError("queue_full", `the async queue is full (${ASYNC_MAX_QUEUED} pending executions)`);
	}
	if (countPendingForSession(sessionId) >= ASYNC_MAX_QUEUED_PER_SESSION) {
		throw new AsyncToolError(
			"queue_full",
			`session ${sessionId} already has ${ASYNC_MAX_QUEUED_PER_SESSION} pending async executions`,
		);
	}
	const bytes = planBytes(intent.prompt, buildOverrides(model, thinking, intent));
	if (queuedBytes + bytes > ASYNC_MAX_QUEUED_BYTES) {
		throw new AsyncToolError("queue_full", "the queued-plan byte budget is exhausted");
	}
	// Retention pressure: prune oldest terminal records before deciding whether
	// there is headroom; never evict active records to accept a job.
	store.sweep();
	// Reserve, up front, the disk the accepted work can still grow into. Each
	// live execution can add a full serialized event ring plus a final (both
	// clipped to those exact byte caps), and the terminal commit writes the
	// grown record twice for its atomic rename — tmp file plus the final name —
	// so the transient copy is budgeted too. Counting only what is committed
	// now would accept work whose growth cannot fit the cap.
	const growthBytes = ASYNC_MAX_EXEC_EVENT_BYTES + asyncFinalCap() + 4096;
	const reserved = 2 * growthBytes * (jobs.size + 1);
	if (store.recordCount >= ASYNC_MAX_RECORDS || store.totalBytes + reserved > ASYNC_MAX_STORE_BYTES) {
		throw new AsyncToolError("queue_full", "the execution store is at its record/byte bound");
	}

	// Test seam: hold the acceptance just before its commit so an MCP request
	// cancellation can still land in the pre-durability window.
	if (ASYNC_TEST_ACCEPT_GATE !== null) {
		while (!existsSync(ASYNC_TEST_ACCEPT_GATE)) {
			await new Promise<void>((resolve) => setTimeout(resolve, SLEEP_MS));
		}
	}
	if (requestToken.cancelled) throw new CancelledBeforeAccept();

	const executionId = randomUUID();
	const record: ExecutionRecord = {
		version: 1,
		execution_id: executionId,
		session_id: sessionId,
		state: "queued",
		idempotency_key: intent.idempotencyKey,
		fingerprint,
		cwd,
		run_options: {
			...(model !== undefined ? { model } : {}),
			...(thinking !== undefined ? { thinking } : {}),
		},
		accepted_at: Date.now(),
		started_at: null,
		finished_at: null,
		events: [],
		oldest_retained_seq: 1,
		latest_seq: 0,
		final: null,
	};

	// The durable acceptance. A failure here is the store failing closed —
	// nothing was dispatched, and no session was allocated anywhere but here.
	commitOrClosed(record, "acceptance");

	const job = createJob(record, intent.prompt, buildOverrides(model, thinking, intent));
	job.timeoutMs = intent.timeoutMs;
	jobs.set(executionId, job);
	queuedBytes += bytes;
	void runJob(job);
	return { executionId, sessionId, state: "queued" };
}

/** Overrides with no undefined-valued keys (exactOptionalPropertyTypes). */
function buildOverrides(
	model: string | undefined,
	thinking: ThinkingLevel | undefined,
	intent: StartIntent,
): RunOverrides {
	const overrides: RunOverrides = {};
	if (model !== undefined) overrides.model = model;
	if (thinking !== undefined) overrides.thinking = thinking;
	if (intent.noTools === true) overrides.no_tools = true;
	else if (intent.tools !== undefined) overrides.tools = intent.tools;
	if (intent.systemPromptAppend !== undefined) overrides.system_prompt_append = intent.systemPromptAppend;
	return overrides;
}

function createJob(record: ExecutionRecord, prompt: string, overrides: RunOverrides): Job {
	let finish!: () => void;
	const finished = new Promise<void>((resolve) => {
		finish = resolve;
	});
	return {
		id: record.execution_id,
		sessionId: record.session_id,
		cwd: record.cwd,
		prompt,
		overrides,
		timeoutMs: undefined,
		token: makeCancelToken(),
		pending: [],
		dropped: 0,
		sawSettled: false,
		startedAt: null,
		flushTimer: undefined,
		finished,
		finish,
	};
}

// --- dispatch ---------------------------------------------------------------

/**
 * One async turn: wait for the session lane (the same lane pi_reply uses) and
 * then a process slot (the same slot pool every run shares), commit `running`,
 * and drive the existing rpc runner. A job whose record is no longer `queued`
 * when it finally gets in (cancelled while waiting) launches nothing.
 */
async function runJob(job: Job): Promise<void> {
	try {
		await withSessionLock(job.sessionId, async () => {
			await withSlot(async () => {
				const record = store?.getRaw(job.id);
				if (store === null || !store.healthy || record === undefined || record.state !== "queued") return;
				// Admission checked the quarantine when the job was accepted; a
				// turn that ended unverified since then blocks the session. The
				// check is repeated here, under the lane and the slot this job
				// will spawn through — a job that queued before the block landed
				// must fail closed, not spawn beside the leftover the sweep has
				// not confirmed gone. (No new quarantine can appear while this
				// job holds the lane: only a turn on this very session sets one,
				// and this job is the turn.)
				if (sessionQuarantine(job.sessionId) !== undefined) {
					commitTerminal(
						job,
						"failed",
						"session_quarantined: the previous turn's pi process group could not be confirmed empty; retry once the leftover processes are confirmed gone",
					);
					return;
				}

				const startedAt = Date.now();
				// `running` before spawn — this state includes a not-yet-spawned
				// window, and that is exactly what the gates below close.
				try {
					store.commit({ ...record, state: "running", started_at: startedAt });
				} catch (err) {
					failClosed("the running commit failed");
					throw err;
				}
				job.startedAt = startedAt;
				// Routing metadata resolves before spawn, separately from any
				// advisory remembering of the queued job: a session that never
				// spawned is never listed as pi history.
				rememberSession(job.sessionId, job.cwd, {
					model: job.overrides.model,
					thinking: job.overrides.thinking,
				});
				if (job.token.cancelled) {
					// Cancellation won the pre-spawn window: zero launches.
					commitTerminal(job, "cancelled", "cancelled before the turn started");
					return;
				}

				// Test seam: park between the running commit and the spawn so the
				// pre-spawn cancellation window can be exercised deterministically.
				if (ASYNC_TEST_SPAWN_GATE !== null) {
					while (!existsSync(ASYNC_TEST_SPAWN_GATE)) {
						await new Promise<void>((resolve) => setTimeout(resolve, SLEEP_MS));
					}
					// The window closed: re-decide from the durable record, not from
					// stale locals. A cancel that won the window (or a store that
					// failed meanwhile) must launch nothing.
					const current = store?.getRaw(job.id);
					if (store === null || !store.healthy || current === undefined || current.state !== "running") {
						if (current !== undefined && current.state === "cancelling") {
							commitTerminal(job, "cancelled", "cancelled before the turn started");
						}
						return;
					}
				}

				startFlushTimer(job);
				try {
					const plan: RunPlan = {
						cwd: job.cwd,
						sessionId: job.sessionId,
						prompt: job.prompt,
						overrides: job.overrides,
						timeoutMs: job.timeoutMs,
					};
					const ctx: CallContext = {
						token: job.token,
						progress: (note) => pushProgress(job, note),
						// Fires from the tree sweep once the group is confirmed empty — on a
						// clean run it lands before the resolve and is a no-op; after an
						// unverified cleanup it is the only thing that lifts the quarantine.
						onTreeGone: () => releaseQuarantine(job.sessionId, job.id),
					};
					const outcome = await invokePi(rpcTransport, plan, ctx, (raw) => observeRaw(job, raw));
					if (outcome.result.cleanupUnverified === true) {
						// The group could not be confirmed empty: block the session until the
						// sweep proves it is. Returning from the run would otherwise free the
						// session lane while a leftover process may still be in it.
						quarantineSession(job.sessionId, job.id);
					}
					finalize(job, outcome.acc, outcome.result, outcome.elapsedMs);
				} finally {
					stopFlushTimer(job);
				}
			});
		});
	} catch (err) {
		// The runner never rejects by contract; a rejection here is a bug in
		// this module. Park the job rather than leave a phantom `running`.
		warn(`async run failed unexpectedly: ${(err as Error).message}`);
		const record = store?.getRaw(job.id);
		if (store?.healthy && record !== undefined && !isTerminal(record.state)) {
			try {
				store.commit({ ...record, state: "interrupted", finished_at: Date.now() });
			} catch {
				/* startup reconciliation owns it */
			}
		}
	} finally {
		queuedBytes -= planBytes(job.prompt, job.overrides);
		jobs.delete(job.id);
		job.finish();
	}
}

// --- projections --------------------------------------------------------------

function observeRaw(job: Job, raw: unknown): void {
	const event = asPiEvent(raw);
	if (event === null) return;
	if (event.type === "agent_settled") {
		job.sawSettled = true;
		return;
	}
	if (event.type === "message_end") {
		const message = readAssistantMessage(eventAs(event, "message_end").message);
		// Provisional answer snapshot: bounded text from a validated assistant
		// message_end that settled. The terminal result supersedes it; live
		// token deltas, narration, and tool payloads never appear.
		if (message !== null && isStopOk(message.stopReason) && message.text) {
			pushEvent(job, "snapshot", message.text);
		}
	}
}

function pushProgress(job: Job, note: string): void {
	pushEvent(job, "progress", note);
}

/**
 * Clip to a UTF-8 byte budget without splitting a surrogate pair. Code-unit
 * clipping would let multibyte text spend up to four bytes per counted
 * "char" and blow past the page-budget share the clip exists to protect.
 */
function clipToBytes(text: string, limitBytes: number): { text: string; truncated: boolean } {
	if (Buffer.byteLength(text, "utf8") <= limitBytes) return { text, truncated: false };
	// Characters first (each is at most 4 bytes, so this never undershoots by
	// more than three), then back off to the exact byte boundary.
	let clipped = text.slice(0, limitBytes);
	while (Buffer.byteLength(clipped, "utf8") > limitBytes) clipped = clipped.slice(0, -1);
	if (clipped.length > 0) {
		const last = clipped.charCodeAt(clipped.length - 1);
		if (last >= 0xd800 && last <= 0xdbff) clipped = clipped.slice(0, -1);
	}
	return { text: clipped, truncated: true };
}

function pushEvent(job: Job, type: ProjectionEvent["type"], text: string): void {
	// Clip now, in bytes; the clip is marked. The sequence is assigned at commit time.
	const { text: clipped, truncated } = clipToBytes(text, asyncEventTextLimit());
	job.pending.push({
		type,
		text: clipped,
		...(truncated ? { truncated: true } : {}),
		at: Date.now(),
	});
	// Bound the unflushed buffer. Dropping is never silent: a count is kept and
	// the next commit opens with a gap marker carrying it, so the committed
	// sequence stays contiguous and loss stays visible.
	while (job.pending.length > ASYNC_PENDING_EVENTS) {
		job.pending.shift();
		job.dropped += 1;
	}
}

/**
 * Everything unflushed, in arrival order, with committed sequences assigned
 * contiguously from `latestSeq`. A drop that occurred since the last commit
 * becomes a leading gap marker: no interior sequence hole can ever be
 * committed, and loss is marked rather than silent.
 */
function collectBatch(job: Job, latestSeq: number): ProjectionEvent[] {
	const raw: PendingEvent[] = [];
	if (job.dropped > 0) {
		raw.push({
			type: "gap",
			text: `${job.dropped} events dropped before commit (memory bound)`,
			dropped: job.dropped,
			at: Date.now(),
		});
		job.dropped = 0;
	}
	raw.push(...job.pending);
	job.pending = [];
	return raw.map((event, index) => ({
		seq: latestSeq + 1 + index,
		type: event.type,
		text: event.text,
		...(event.truncated === true ? { truncated: true } : {}),
		...(event.dropped !== undefined ? { dropped: event.dropped } : {}),
		at: event.at,
	}));
}

function startFlushTimer(job: Job): void {
	job.flushTimer = setInterval(() => {
		try {
			flushJob(job);
		} catch {
			failClosed("a progress commit failed");
		}
	}, ASYNC_FLUSH_INTERVAL_MS);
	job.flushTimer.unref();
}

function stopFlushTimer(job: Job): void {
	if (job.flushTimer !== undefined) {
		clearInterval(job.flushTimer);
		job.flushTimer = undefined;
	}
}

/** Merge retained events under the per-execution bounds; drop from the front.
 * The bound is serialized UTF-8 bytes — what the record actually spends on
 * disk — so multibyte and escape-heavy events cost what they really cost. */
function retainEvents(existing: ProjectionEvent[], incoming: ProjectionEvent[]): ProjectionEvent[] {
	const merged = [...existing, ...incoming];
	let bytes = 0;
	const sizes = merged.map((event) => Buffer.byteLength(JSON.stringify(event), "utf8"));
	for (const size of sizes) bytes += size;
	let start = 0;
	while (start < merged.length && (merged.length - start > ASYNC_MAX_EVENTS || bytes > ASYNC_MAX_EXEC_EVENT_BYTES)) {
		bytes -= sizes[start] ?? 0;
		start += 1;
	}
	return start === 0 ? merged : merged.slice(start);
}

function flushJob(job: Job): void {
	if (job.pending.length === 0 && job.dropped === 0) return;
	const record = store?.getRaw(job.id);
	if (store === null || !store.healthy || record === undefined || isTerminal(record.state)) return;
	const batch = collectBatch(job, record.latest_seq);
	const events = retainEvents(record.events, batch);
	try {
		store.commit({
			...record,
			events,
			oldest_retained_seq: events.length > 0 ? (events[0]?.seq ?? latestOf(record, batch)) : latestOf(record, batch),
			latest_seq: latestOf(record, batch),
		});
	} catch (err) {
		failClosed("a progress commit failed");
		throw unavailable(err);
	}
}

function latestOf(record: ExecutionRecord, batch: ProjectionEvent[]): number {
	return record.latest_seq + batch.length;
}

// --- completion ---------------------------------------------------------------

type RunEnd = Omit<RunResult, "stdout" | "stderr" | "endedBy">;

function finalize(job: Job, acc: Accumulator, end: RunEnd, elapsedMs: number): void {
	const record = store?.getRaw(job.id);
	if (store === null || !store.healthy || record === undefined) return;
	if (isTerminal(record.state)) return; // interrupted at shutdown, or already final

	if (record.state === "cancelling") {
		// First serialized decision wins: a committed cancelling beats a late
		// completion, and the partials already committed are kept — unless the
		// group could not be verified empty. Then the cancel did not cleanly
		// happen: fail closed instead of reporting cancelled while a
		// descendant may still be running.
		const answer = selectAnswer(acc);
		if (end.cleanupUnverified === true) {
			commitTerminal(job, "failed", UNVERIFIED_CLEANUP, answer, acc, elapsedMs);
			return;
		}
		commitTerminal(job, "cancelled", "cancelled by request", answer, acc, elapsedMs);
		return;
	}

	const answer = selectAnswer(acc);
	const problem = answerProblem(answer);
	const failed = end.code !== 0 || end.timedOut === true || end.cancelled === true || end.cleanupUnverified === true;
	if (!failed && job.sawSettled && problem === null && answer.text !== null && answer.source === "settled") {
		commitTerminal(job, "succeeded", null, answer, acc, elapsedMs);
		return;
	}
	commitTerminal(
		job,
		"failed",
		failureReason(job, end, problem, job.sawSettled, answer),
		answer,
		acc,
		elapsedMs,
		!job.sawSettled || end.code !== 0 ? processDiagnostics(end) : undefined,
	);
}

/** What an unverifiable process group means to the caller. */
const UNVERIFIED_CLEANUP =
	"the pi process group could not be confirmed empty after SIGKILL; a leftover process may still be running — check before reusing the session";

function processDiagnostics(end: RunEnd): ProcessDiagnostics {
	return {
		...(end.spawnErrorCode === undefined ? {} : { spawn_error_code: end.spawnErrorCode }),
		...(end.exitCode === undefined ? {} : { exit_code: end.exitCode }),
		...(end.signal === undefined ? {} : { signal: end.signal }),
		rpc_prompt_acknowledged: end.rpcPromptAcknowledged === true,
	};
}

function failureReason(
	job: Job,
	end: RunEnd,
	problem: string | null,
	sawSettled: boolean,
	answer: SelectedAnswer,
): string {
	if (end.cleanupUnverified === true) return UNVERIFIED_CLEANUP;
	if (end.timedOut === true) return `timed out after ${job.timeoutMs ?? "the configured"} ms`;
	if (end.spawnErrorCode !== undefined) return `could not start pi (spawn error ${end.spawnErrorCode})`;
	if (!sawSettled && end.rpcPromptAcknowledged !== true && end.code !== 0) {
		const status = end.signal ?? (end.code >= 0 ? `exit code ${end.code}` : "unknown exit status");
		return `pi terminated before acknowledging the RPC prompt (${status})`;
	}
	if (answer.errorMessage !== undefined && answer.errorMessage !== null) {
		return `pi reported: ${clipReason(answer.errorMessage)}`;
	}
	if (end.signal !== undefined) return `pi terminated by ${end.signal}`;
	if (end.code !== 0) return end.code >= 0 ? `pi exited with code ${end.code}` : "pi exited with unknown status";
	if (!sawSettled) {
		return end.rpcPromptAcknowledged === true
			? "pi exited cleanly without settling the turn (no agent_settled observed)"
			: "pi exited cleanly without acknowledging the RPC prompt or settling the turn (no agent_settled observed)";
	}
	if (problem !== null) return clipReason(problem);
	return "no settled answer text";
}

function clipReason(text: string): string {
	return text.length > 300 ? `${text.slice(0, 300)}…` : text;
}

function buildFinal(
	outcome: FinalOutcome["outcome"],
	failureReasonText: string | null,
	answer: SelectedAnswer | null,
	acc: Accumulator,
	elapsedMs: number,
	processInfo?: ProcessDiagnostics,
): FinalOutcome {
	const counts = new Map<string, number>();
	for (const name of acc.toolCalls) counts.set(name, (counts.get(name) ?? 0) + 1);
	const toolCalls = [...counts.entries()]
		.sort((a, b) => b[1] - a[1])
		.slice(0, 32)
		.map(([name, count]) => ({ name, count }));

	const meta = {
		outcome,
		failure_reason: failureReasonText,
		...(processInfo === undefined ? {} : { process_diagnostics: processInfo }),
		provider: acc.provider,
		model: acc.model,
		usage: { ...acc.usage },
		tool_calls: toolCalls,
		turns: acc.turns,
		retries: acc.retries,
		elapsed_ms: elapsedMs,
	};
	let finalAnswer: FinalOutcome["answer"] = null;
	if (answer?.text) {
		// The cap is on the final as it is persisted: the serialized UTF-8
		// bytes of the whole outcome — JSON escaping included — not the raw
		// character count. Multibyte and escape-heavy answers cost what they
		// actually cost on disk, and the reservation that admitted the job
		// (asyncFinalCap per execution) stays honest.
		const mkAnswer = (text: string, truncated: boolean): FinalOutcome["answer"] => ({
			text,
			...(answer.stopReason !== undefined ? { stop_reason: answer.stopReason } : {}),
			truncated,
			complete: outcome === "succeeded",
		});
		const fits = (candidate: FinalOutcome): boolean =>
			Buffer.byteLength(JSON.stringify(candidate), "utf8") <= asyncFinalCap();
		const full = { ...meta, answer: mkAnswer(answer.text, false) };
		if (fits(full)) finalAnswer = full.answer;
		else {
			// Binary-search the largest text prefix whose serialized final
			// fits. lo always fits (an empty-text final is envelope-sized),
			// hi never does.
			const text = answer.text;
			let lo = 0;
			let hi = text.length;
			while (lo + 1 < hi) {
				const mid = (lo + hi) >> 1;
				const candidate = {
					...full,
					answer: mkAnswer(text.slice(0, mid), true),
				};
				if (fits(candidate)) lo = mid;
				else hi = mid;
			}
			// Never persist half of a surrogate pair.
			let end = lo;
			if (end > 0 && (text.charCodeAt(end - 1) & 0xfc00) === 0xd800 && (text.charCodeAt(end) & 0xfc00) === 0xdc00) {
				end -= 1;
			}
			finalAnswer = mkAnswer(text.slice(0, end), true);
		}
	}

	return { ...meta, answer: finalAnswer };
}

function commitTerminal(
	job: Job,
	state: Extract<ExecutionState, "succeeded" | "failed" | "cancelled" | "interrupted">,
	failureReasonText: string | null,
	answer?: SelectedAnswer,
	acc?: Accumulator,
	elapsedMs?: number,
	processInfo?: ProcessDiagnostics,
): void {
	const record = store?.getRaw(job.id);
	if (store === null || !store.healthy || record === undefined) return;
	const final = buildFinal(
		state,
		failureReasonText,
		answer ?? null,
		acc ?? newAccumulator(),
		elapsedMs ?? 0,
		processInfo,
	);
	// One commit: the last flushed events, the terminal state, and the final
	// answer all become durable together. A failure here is routed through
	// failClosed — the state is never reported as settled when it was not
	// committed, and the children are reaped rather than left running.
	const batch = collectBatch(job, record.latest_seq);
	const events = retainEvents(record.events, batch);
	const latestSeq = latestOf(record, batch);
	try {
		store.commit({
			...record,
			events,
			oldest_retained_seq: events.length > 0 ? (events[0]?.seq ?? latestSeq + 1) : latestSeq + 1,
			latest_seq: latestSeq,
			state,
			finished_at: Date.now(),
			final,
		});
	} catch (err) {
		failClosed(`the terminal commit failed: ${(err as Error).message}`);
	}
}

// --- cancellation --------------------------------------------------------------

export interface ExecutionView {
	executionId: string;
	sessionId: string;
	state: ExecutionState;
	finishedAt: number | null;
}

function view(record: ExecutionRecord): ExecutionView {
	return {
		executionId: record.execution_id,
		sessionId: record.session_id,
		state: record.state,
		finishedAt: record.finished_at,
	};
}

function lookupForMutation(executionId: string): ExecutionRecord {
	if (store === null) {
		throw new AsyncToolError("store_unavailable", "the async execution store is not configured");
	}
	if (!store.healthy) {
		throw new AsyncToolError("store_unavailable", "the async execution store is unavailable");
	}
	let hit: ReturnType<ExecutionStore["lookup"]>;
	try {
		// A lookup can mutate: retention expiry commits here too.
		hit = store.lookup(executionId);
	} catch (err) {
		failClosed("a retention commit during lookup failed");
		throw unavailable(err);
	}
	if (hit.kind === "none") {
		throw new AsyncToolError("unknown_execution", `no execution ${executionId}`);
	}
	if (hit.kind === "tombstone") {
		throw new AsyncToolError("execution_expired", `execution ${executionId} has expired`);
	}
	return hit.record;
}

export function statusOf(executionId: string): { record: ExecutionRecord } {
	const record = lookupForMutation(executionId);
	return { record };
}

/**
 * Idempotent cancellation. A terminal execution is a no-op. A queued one
 * commits `cancelled` without spawning. A running one commits `cancelling`,
 * cancels the job token once, and waits for child close and group cleanup
 * before answering.
 */
export async function cancelExecution(executionId: string): Promise<ExecutionView> {
	if (store === null) throw new AsyncToolError("store_unavailable", "the async execution store is not configured");
	if (!store.healthy) throw new AsyncToolError("store_unavailable", "the async execution store is unavailable");

	const record = lookupForMutation(executionId);
	if (isTerminal(record.state)) return view(record);

	const job = jobs.get(executionId);
	if (job === undefined) {
		// A nonterminal record with no live job is an orphan in this process
		// (it can only be recovery residue): park it, do not guess.
		const next = { ...record, state: "interrupted" as const, finished_at: Date.now() };
		commitOrClosed(next, "orphan cancel");
		return view(next);
	}

	if (record.state === "queued") {
		const next = { ...record, state: "cancelled" as const, finished_at: Date.now() };
		commitOrClosed(next, "queued cancel");
		job.token.cancel();
		return view(next);
	}

	if (record.state === "cancelling") {
		await job.finished; // repeated cancel: the first one already owns it
		return view(store.getRaw(executionId) ?? record);
	}

	// running: commit cancelling, cancel the token once, wait for cleanup.
	const next = { ...record, state: "cancelling" as const };
	commitOrClosed(next, "cancel");
	job.token.cancel();
	await job.finished;
	const finalRecord = store.getRaw(executionId);
	return view(finalRecord ?? next);
}

/**
 * Every store mutation from the cancellation and acceptance paths goes through
 * here: any failure reaps the live async children, stops admissions and
 * dispatch, and reports store_unavailable. No path may swallow a write
 * failure and answer as if the state it promised had been committed.
 */
function commitOrClosed(record: ExecutionRecord, context: string): void {
	if (store === null) return;
	try {
		store.commit(record);
	} catch (err) {
		failClosed(`the ${context} commit failed`);
		throw unavailable(err);
	}
}

function unavailable(err: unknown): AsyncToolError {
	if (err instanceof StoreUnavailableError) {
		return new AsyncToolError("store_unavailable", err.message);
	}
	return new AsyncToolError("store_unavailable", `the execution store failed: ${(err as Error).message}`);
}

/**
 * Route a legacy `pi_send abort` aimed at a running async turn through the same
 * cancellation transition. Without this the in-protocol abort would end the
 * child cleanly and the job would be misreported as a success.
 */
export function notifyAsyncAbort(sessionId: string): void {
	if (store === null || !store.healthy) return;
	for (const job of jobs.values()) {
		if (job.sessionId !== sessionId) continue;
		const record = store.getRaw(job.id);
		if (record === undefined || record.state !== "running") continue;
		try {
			store.commit({ ...record, state: "cancelling" });
			job.token.cancel();
		} catch {
			failClosed("a cancel commit failed");
		}
		return;
	}
}

// --- fail closed ----------------------------------------------------------------

/**
 * The first store failure ends the async subsystem: no admissions, no
 * mutations, no dispatch. Every live async child is reaped via its job token
 * (abort, then TERM, then KILL — the existing escalation), and every queued
 * plan stays undispatched in memory.
 */
function failClosed(reason: string): void {
	if (failedClosed) return;
	failedClosed = true;
	warn(`async subsystem failing closed: ${reason}`);
	for (const job of jobs.values()) {
		stopFlushTimer(job);
		job.token.cancel();
	}
}
