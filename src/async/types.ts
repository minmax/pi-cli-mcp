// Shared shapes of the durable async subsystem. Everything here is either the
// persisted projection of an execution or the tool-level contract around it.
//
// Nothing in this module carries prompts, credentials, or raw transcript data:
// the store holds bounded progress notes, provisional answer snapshots, and
// the settled final — never the queued plan (that lives in bounded memory only)
// and never the raw event stream.

import type { ThinkingLevel } from "../types.ts";

// --- state machine ---------------------------------------------------------

export type ExecutionState = "queued" | "running" | "cancelling" | "succeeded" | "failed" | "cancelled" | "interrupted";

export const TERMINAL_STATES: readonly ExecutionState[] = ["succeeded", "failed", "cancelled", "interrupted"];

export function isTerminal(state: ExecutionState): boolean {
	return (TERMINAL_STATES as readonly string[]).includes(state);
}

function isExecutionState(value: unknown): value is ExecutionState {
	return (
		typeof value === "string" &&
		(value === "queued" ||
			value === "running" ||
			value === "cancelling" ||
			value === "succeeded" ||
			value === "failed" ||
			value === "cancelled" ||
			value === "interrupted")
	);
}

/**
 * The legal transitions. `interrupted` is written only by recovery: at startup
 * reconciliation, or at clean shutdown when the supervisor is going away and the
 * run cannot be finished. `cancelling` may land on `failed` for exactly one
 * reason: the pi process group could not be confirmed empty, so the cancel
 * did not cleanly happen and pretending it did would be a lie. A terminal state
 * never transitions again; its payload may still expire under retention.
 */
const TRANSITIONS: Record<ExecutionState, readonly ExecutionState[]> = {
	// A queued job can also fail without ever running: dispatch refuses it
	// when the session is quarantined behind an unverified process group, so
	// nothing spawns beside the leftover. The failure is committed, not
	// parked — the caller retries once the sweep confirms the group empty.
	queued: ["running", "failed", "cancelled", "interrupted"],
	running: ["succeeded", "failed", "cancelling", "interrupted"],
	cancelling: ["cancelled", "failed", "interrupted"],
	succeeded: [],
	failed: [],
	cancelled: [],
	interrupted: [],
};

export function canTransition(from: ExecutionState, to: ExecutionState): boolean {
	return (TRANSITIONS[from] as readonly string[]).includes(to);
}

// --- projection events -----------------------------------------------------

/**
 * One committed projection event. `progress` is a short safe note (turn, tool
 * label, retry, compaction); `snapshot` is bounded text from a validated
 * assistant message_end with a settled stop reason — a provisional answer the
 * terminal result supersedes; `gap` marks events dropped by the in-memory
 * bound before they could be committed, carrying the count in `dropped`, so
 * the committed sequence stays contiguous and loss stays visible. Oversized
 * projections are clipped before their sequence is assigned and carry
 * `truncated`.
 */
export interface ProjectionEvent {
	seq: number;
	type: "progress" | "snapshot" | "gap";
	text: string;
	truncated?: boolean;
	/** Present only on a `gap` event: how many events were dropped. */
	dropped?: number;
	at: number;
}

// --- final -----------------------------------------------------------------

export interface FinalAnswer {
	text: string;
	stop_reason?: string;
	/** True when the stored text was clipped to the final-answer cap. */
	truncated: boolean;
	/** False when this is the last text of an unfinished run, not a settled answer. */
	complete: boolean;
}

/**
 * Normalized terminal outcome. Failure reasons are short and safe; the legacy
 * formatted error/diagnostic string is deliberately not persisted.
 */
export interface FinalOutcome {
	outcome: "succeeded" | "failed" | "cancelled" | "interrupted";
	answer: FinalAnswer | null;
	failure_reason: string | null;
	/** Optional for older records and failures unrelated to process exit. */
	process_diagnostics?: ProcessDiagnostics;
	provider: string | null;
	model: string | null;
	usage: { input: number; output: number; reasoning: number; cost: number };
	tool_calls: { name: string; count: number }[];
	turns: number;
	retries: number;
	elapsed_ms: number;
}

/** Only bounded process metadata; never stderr, argv, or RPC payloads. */
export interface ProcessDiagnostics {
	spawn_error_code?: string;
	exit_code?: number;
	signal?: string;
	rpc_prompt_acknowledged: boolean;
}

function isProcessDiagnostics(value: unknown): value is ProcessDiagnostics {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
	const {
		spawn_error_code: spawnError,
		exit_code: exitCode,
		signal,
		rpc_prompt_acknowledged: ack,
	} = value as Record<string, unknown>;
	const known = ["spawn_error_code", "exit_code", "signal", "rpc_prompt_acknowledged"];
	if (Object.keys(value).some((key) => !known.includes(key))) return false;
	return (
		typeof ack === "boolean" &&
		(spawnError === undefined || (typeof spawnError === "string" && /^[A-Z][A-Z0-9_]{0,31}$/.test(spawnError))) &&
		(exitCode === undefined || (typeof exitCode === "number" && Number.isSafeInteger(exitCode) && exitCode >= 0)) &&
		(signal === undefined || (typeof signal === "string" && /^SIG[A-Z0-9]{1,16}$/.test(signal)))
	);
}

// --- persisted record ------------------------------------------------------

export interface ExecutionRecord {
	version: 1;
	execution_id: string;
	session_id: string;
	state: ExecutionState;
	/** Client idempotency key, or null. Reserved through retention + tombstone. */
	idempotency_key: string | null;
	/** Versioned fingerprint of the canonical request intent. */
	fingerprint: string;
	cwd: string;
	/** Explicit model/thinking only — kept for routing repair, not replay. */
	run_options: { model?: string; thinking?: ThinkingLevel };
	accepted_at: number;
	started_at: number | null;
	finished_at: number | null;
	events: ProjectionEvent[];
	/** First retained sequence; events below it are detectably lost. */
	oldest_retained_seq: number;
	latest_seq: number;
	final: FinalOutcome | null;
	/** True once finals/keys retention elapsed: the record is a tombstone. */
	finals_expired?: boolean;
}

export function readExecutionRecord(value: unknown): ExecutionRecord | null {
	const entry =
		typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
	if (entry === null) return null;
	if (entry.version !== 1) return null;
	if (typeof entry.execution_id !== "string" || typeof entry.session_id !== "string") return null;
	if (!isExecutionState(entry.state)) return null;
	if (typeof entry.fingerprint !== "string" || typeof entry.cwd !== "string") return null;
	if (typeof entry.accepted_at !== "number" || !Number.isFinite(entry.accepted_at)) return null;
	const startedAt = entry.started_at;
	if (startedAt !== null && typeof startedAt !== "number") return null;
	const finishedAt = entry.finished_at;
	if (finishedAt !== null && typeof finishedAt !== "number") return null;
	const key = entry.idempotency_key;
	if (key !== null && typeof key !== "string") return null;
	const runOptions =
		typeof entry.run_options === "object" && entry.run_options !== null && !Array.isArray(entry.run_options)
			? (entry.run_options as Record<string, unknown>)
			: {};
	if (runOptions.model !== undefined && typeof runOptions.model !== "string") return null;
	if (runOptions.thinking !== undefined && typeof runOptions.thinking !== "string") return null;

	const events: ProjectionEvent[] = [];
	if (Array.isArray(entry.events)) {
		for (const raw of entry.events) {
			const event =
				typeof raw === "object" && raw !== null && !Array.isArray(raw) ? (raw as Record<string, unknown>) : null;
			if (event === null) return null;
			if (typeof event.seq !== "number" || !Number.isFinite(event.seq)) return null;
			if (event.type !== "progress" && event.type !== "snapshot" && event.type !== "gap") return null;
			if (typeof event.text !== "string") return null;
			if (event.truncated !== undefined && typeof event.truncated !== "boolean") return null;
			if (event.dropped !== undefined && typeof event.dropped !== "number") return null;
			if (typeof event.at !== "number" || !Number.isFinite(event.at)) return null;
			events.push({
				seq: event.seq,
				type: event.type,
				text: event.text,
				...(event.truncated === true ? { truncated: true } : {}),
				...(event.dropped !== undefined ? { dropped: event.dropped } : {}),
				at: event.at,
			});
		}
	} else {
		return null;
	}
	if (typeof entry.oldest_retained_seq !== "number" || !Number.isFinite(entry.oldest_retained_seq)) return null;
	if (typeof entry.latest_seq !== "number" || !Number.isFinite(entry.latest_seq)) return null;
	if (entry.final !== null && entry.final !== undefined) {
		const final = entry.final as Record<string, unknown>;
		if (typeof final !== "object" || final === null) return null;
		if (
			final.outcome !== "succeeded" &&
			final.outcome !== "failed" &&
			final.outcome !== "cancelled" &&
			final.outcome !== "interrupted"
		)
			return null;
		if (typeof final.failure_reason !== "string" && final.failure_reason !== null) return null;
		if (typeof final.turns !== "number" || typeof final.retries !== "number") return null;
		if (final.process_diagnostics !== undefined && !isProcessDiagnostics(final.process_diagnostics)) return null;
	} else if (entry.final === undefined) {
		return null;
	}
	if (entry.finals_expired !== undefined && typeof entry.finals_expired !== "boolean") return null;

	return {
		version: 1,
		execution_id: entry.execution_id,
		session_id: entry.session_id,
		state: entry.state,
		idempotency_key: key,
		fingerprint: entry.fingerprint,
		cwd: entry.cwd,
		run_options: {
			...(runOptions.model !== undefined ? { model: runOptions.model } : {}),
			...(runOptions.thinking !== undefined ? { thinking: runOptions.thinking as ThinkingLevel } : {}),
		},
		accepted_at: entry.accepted_at,
		started_at: startedAt ?? null,
		finished_at: finishedAt ?? null,
		events,
		oldest_retained_seq: entry.oldest_retained_seq,
		latest_seq: entry.latest_seq,
		final: (entry.final as FinalOutcome | null) ?? null,
		...(entry.finals_expired === true ? { finals_expired: true } : {}),
	};
}

// --- tool errors ------------------------------------------------------------

export type AsyncErrorCode =
	| "invalid_input"
	| "unknown_session"
	| "unknown_execution"
	| "execution_expired"
	| "invalid_cursor"
	| "idempotency_conflict"
	| "queue_full"
	| "store_unavailable"
	| "session_quarantined"
	| "page_too_small";

export class AsyncToolError extends Error {
	readonly code: AsyncErrorCode;

	constructor(code: AsyncErrorCode, message: string) {
		super(message);
		this.code = code;
	}
}

/** An MCP request cancelled before its acceptance became durable. */
export class CancelledBeforeAccept extends Error {
	constructor() {
		super("cancelled before acceptance");
	}
}

// --- idempotent start intent ------------------------------------------------

/** What pi_start validated, before any mutable check or capacity decision. */
export interface StartIntent {
	prompt: string;
	/** null means "allocate a new pi session UUID". */
	sessionId: string | null;
	idempotencyKey: string | null;
	/** Canonical cwd: explicitly supplied, else the server's cwd. */
	cwd: string;
	explicitCwd: boolean;
	model?: string | undefined;
	thinking?: ThinkingLevel | undefined;
	timeoutMs?: number | undefined;
	tools?: string | undefined;
	noTools?: boolean;
	systemPromptAppend?: string | undefined;
}
