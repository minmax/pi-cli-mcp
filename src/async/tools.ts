// pi_start / pi_status / pi_wait / pi_result / pi_cancel: the opt-in async contract.
// Appended after the six legacy tools only when PI_MCP_ASYNC_STATE_DIR is set;
// with it unset, the adapter exposes exactly the original six.
//
// Machine-readable payloads go out as structuredContent where supported, with
// an equivalent bounded JSON text copy for clients and proxies that drop the
// structured one. Errors carry stable codes; a known nonterminal execution is
// pending, not an error.

import { ASYNC_PAGE_EVENTS, asyncPageBytes } from "../config.ts";
import { readOverrides, readPrompt, readTimeout, resolveCwd, toolResult } from "../tools.ts";
import type { CancelToken, RunOverrides, ThinkingLevel, ToolDefinition, ToolResult } from "../types.ts";
import { isThinkingLevel, THINKING_LEVELS } from "../types.ts";
import { accept, cancelExecution, type ExecutionView, statusOf } from "./scheduler.ts";
import {
	AsyncToolError,
	CancelledBeforeAccept,
	type ExecutionRecord,
	type FinalOutcome,
	isTerminal,
	type ProjectionEvent,
} from "./types.ts";

// --- schemas ----------------------------------------------------------------

const MUTATING = {
	readOnlyHint: false,
	destructiveHint: true,
	idempotentHint: false,
	openWorldHint: true,
} as const;
/** pi_cancel removes queued work and ends running work, but repeating it is a no-op. */
const IDEMPOTENT_MUTATING = {
	readOnlyHint: false,
	destructiveHint: true,
	idempotentHint: true,
	openWorldHint: true,
} as const;
/** pi_status, pi_wait and pi_result read only the local execution store. */
const LOCAL_READING = {
	readOnlyHint: true,
	destructiveHint: false,
	idempotentHint: true,
	openWorldHint: false,
} as const;

const RUN_PROPS = {
	cwd: {
		type: "string",
		description:
			"Usually omit to use this server's cwd. If set, must be an absolute path (relative is rejected). " +
			"pi works and edits here, and reads AGENTS.md / CLAUDE.md from here.",
	},
	model: {
		type: "string",
		description:
			"Model pattern or id, e.g. 'sonnet', 'bifrost/minimax/MiniMax-M3', 'provider/id:thinking'. " +
			"Defaults to pi's own settings; pi_models lists valid values.",
	},
	thinking: {
		type: "string",
		enum: THINKING_LEVELS,
		description:
			"Thinking level. No-op on models without thinking support (check pi_models). Defaults to pi's " +
			"own settings.",
	},
	timeout_ms: {
		type: "integer",
		minimum: 1000,
		description:
			"Usually omit — there is no default deadline for an async run. A run killed at the deadline " +
			"ends as a failed execution and its pi session stays resumable with pi_reply.",
	},
} as const;

/** pi_wait bounds: the default blocks briefly, the ceiling keeps a call well inside client timeouts. */
const WAIT_DEFAULT_MS = 5_000;
const WAIT_MAX_MS = 20_000;
// Nonterminal lookups are in memory. A job's finished promise cannot be
// unsubscribed and may resolve after a queued cancellation commits terminal state.
// Poll the status rather than retaining a callback on a job past the request.
const WAIT_POLL_MS = 50;

export const ASYNC_TOOLS: ToolDefinition[] = [
	{
		name: "pi_start",
		description:
			"Start a NEW asynchronous pi execution and return immediately — before a model is reached, a " +
			"slot is taken, or pi has spawned. The execution belongs to the server, not to this call: it " +
			"keeps running even if this conversation moves on. Poll pi_status for progress, pi_result for " +
			"ordered partial events and the final answer, pi_cancel to stop it.\n" +
			"Optional `idempotency_key` makes retries safe: the same key with the same request returns the " +
			"original execution; the same key with a different request is rejected. Omit `session_id` to " +
			"start a new pi session; supply it to add a turn to a session this adapter knows.\n" +
			"Same reach as the `pi` tool: with default tools, pi edits files and runs shell commands as " +
			"your user inside `cwd`. Async runs always use the rpc transport.",
		inputSchema: {
			type: "object",
			properties: {
				prompt: {
					type: "string",
					description:
						"The complete task. pi cannot see this conversation, so include everything it needs: " +
						"file paths, goal, constraints, expected output format.",
				},
				session_id: {
					type: "string",
					description:
						"Omit to allocate a new pi session UUID. Supply a session id from pi_start/pi_sessions " +
						"to continue that conversation; it must be known to this adapter, and tools/" +
						"no_tools/system_prompt_append are not accepted on a continuation.",
				},
				idempotency_key: {
					type: "string",
					maxLength: 256,
					description:
						"Client-generated key for safe retries (random, e.g. a UUID). Same key + same request " +
						"returns the original execution; same key + different request is a conflict. The key is " +
						"reserved while the result is retained, and briefly beyond.",
				},
				...RUN_PROPS,
				tools: {
					type: "string",
					description:
						"New sessions only. Usually omit to keep pi's default set (includes bash/edit/write). " +
						"Set a comma-separated allowlist of pi tool names only to restrict, e.g. 'read,grep,ls'.",
				},
				no_tools: {
					type: "boolean",
					description: "New sessions only. Disable all pi tools: pure reasoning, no file or shell access.",
				},
				system_prompt_append: {
					type: "string",
					description: "New sessions only. Extra text appended to pi's system prompt.",
				},
			},
			required: ["prompt"],
			additionalProperties: false,
		},
		annotations: MUTATING,
	},
	{
		name: "pi_status",
		description:
			"Check an async execution: state (queued, running, cancelling, succeeded, failed, cancelled, " +
			"interrupted), timestamps, a short safe progress summary, and the latest committed event " +
			"sequence. No answer text and no prompt — use pi_result for events and the final.",
		inputSchema: {
			type: "object",
			properties: {
				execution_id: { type: "string", description: "Execution id from pi_start." },
			},
			required: ["execution_id"],
			additionalProperties: false,
		},
		annotations: LOCAL_READING,
	},
	{
		name: "pi_wait",
		description:
			"Bounded wait on an async execution. Returns exactly what pi_status returns: immediately if the " +
			"execution is already terminal, otherwise after it turns terminal or `timeout_ms` elapses, " +
			"whichever comes first — then the latest status, which may still be nonterminal. A timeout is " +
			"not an error and does not affect the execution; call again, or use pi_result for the final.",
		inputSchema: {
			type: "object",
			properties: {
				execution_id: { type: "string", description: "Execution id from pi_start." },
				timeout_ms: {
					type: "integer",
					minimum: 0,
					maximum: WAIT_MAX_MS,
					description: `Longest this call blocks, in milliseconds. Default ${WAIT_DEFAULT_MS}, maximum ${WAIT_MAX_MS}.`,
				},
			},
			required: ["execution_id"],
			additionalProperties: false,
		},
		annotations: LOCAL_READING,
	},
	{
		name: "pi_result",
		description:
			"Read an async execution: ordered committed events since `after_cursor` plus the final result " +
			"once terminal (never requires replaying the whole sequence). Events are a safe projection — " +
			"short progress notes and provisional answer snapshots — not the transcript. Deduplicate on " +
			"(execution_id, seq), advance your cursor only after processing, drain has_more before " +
			"stopping. An expired cursor is reported in the response, never hidden by an error. Poll " +
			"every 1-2 s at first, backing off to 5-10 s on empty pages. A final marked `page_clipped` " +
			"was cut to fit the page byte cap; its `recovery_hint` says how to get the whole thing — " +
			"`final_only=true` pages just the retained final by bytes, no events. The byte cap covers the " +
			"exact frame as written — envelope, both payload representations, the trailing newline. One " +
			"exemption, explicit: the JSON-RPC request id, echoed as the protocol requires; a request id " +
			"too large to fit beside a page gets the same bounded `page_too_small` refusal, and the only " +
			"bytes it carries over the cap are the id itself.",
		inputSchema: {
			type: "object",
			properties: {
				execution_id: { type: "string", description: "Execution id from pi_start." },
				after_cursor: {
					type: "string",
					description:
						"Exclusive cursor from a previous page's next_cursor. Omit to read from the " +
						"first retained event; the final is always included when retained.",
				},
				limit: {
					type: "integer",
					minimum: 1,
					maximum: 1000,
					description: "Maximum events per page. The server also bounds the page by bytes.",
				},
				final_only: {
					type: "boolean",
					description:
						"Skip the events: return only the retained final, paged by bytes when it " +
						"exceeds the page byte cap (advance with the returned next_cursor until " +
						"has_more is false). The recovery path for a page_clipped final.",
				},
			},
			required: ["execution_id"],
			additionalProperties: false,
		},
		annotations: LOCAL_READING,
	},
	{
		name: "pi_cancel",
		description:
			"Cancel an async execution. Idempotent: cancelling a terminal execution is a no-op. A queued " +
			"execution is cancelled without ever spawning pi. A running one is asked to stop (abort, then " +
			"signals) and this call waits until the pi process and its group are reaped.",
		inputSchema: {
			type: "object",
			properties: {
				execution_id: { type: "string", description: "Execution id from pi_start." },
			},
			required: ["execution_id"],
			additionalProperties: false,
		},
		annotations: IDEMPOTENT_MUTATING,
	},
];

// --- shared helpers -----------------------------------------------------------

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function invalid(message: string): AsyncToolError {
	return new AsyncToolError("invalid_input", message);
}

/** Structured payload plus its bounded JSON text copy. */
function payloadResult(payload: Record<string, unknown>, isError = false): ToolResult {
	return {
		content: [{ type: "text", text: JSON.stringify(payload) }],
		structuredContent: payload,
		...(isError ? { isError: true } : {}),
	};
}

function errorResult(tool: string, err: unknown): ToolResult {
	if (err instanceof AsyncToolError) {
		const payload = { error: { code: err.code, message: err.message } };
		return {
			content: [{ type: "text", text: `${tool}: ${err.code}: ${err.message}` }],
			structuredContent: payload,
			isError: true,
		};
	}
	if (err instanceof CancelledBeforeAccept) {
		return toolResult(
			`${tool}: the request was cancelled before the execution became durable; nothing was started. ` +
				"Retry with the same idempotency_key if a new attempt is wanted.",
			true,
		);
	}
	// Anything else is a bug in the adapter; index.ts renders internal errors.
	throw err instanceof Error ? err : new Error(String(err));
}

// --- cursor codec ---------------------------------------------------------------

function encodeCursor(executionId: string, seq: number): string {
	return Buffer.from(`v1:${executionId}:${seq}`, "utf8").toString("base64url");
}

function decodeCursor(executionId: string, cursor: string): number | null {
	const raw = Buffer.from(cursor, "base64url").toString("utf8");
	const match = raw.match(/^v1:([^:]+):(\d+)$/);
	if (match === null) return null;
	if (match[1] !== executionId) return null;
	const seq = Number(match[2]);
	// Round-trip check: base64url decoding is lenient, the encoding is not.
	if (encodeCursor(executionId, seq) !== cursor) return null;
	if (!Number.isSafeInteger(seq) || seq < 0) return null;
	return seq;
}

/** Byte-offset cursor for final_only pages — a different namespace from event
 * cursors, so one can never be mistaken for the other. */
function encodeFinalCursor(executionId: string, offset: number): string {
	return Buffer.from(`v1:${executionId}:f:${offset}`, "utf8").toString("base64url");
}

function decodeFinalCursor(executionId: string, cursor: string): number | null {
	const raw = Buffer.from(cursor, "base64url").toString("utf8");
	const match = raw.match(/^v1:([^:]+):f:(\d+)$/);
	if (match === null) return null;
	if (match[1] !== executionId) return null;
	const offset = Number(match[2]);
	if (encodeFinalCursor(executionId, offset) !== cursor) return null;
	if (!Number.isSafeInteger(offset) || offset < 0) return null;
	return offset;
}

// --- pi_start -------------------------------------------------------------------

interface ParsedStart {
	prompt: string;
	sessionId: string | null;
	idempotencyKey: string | null;
	explicitCwd: boolean;
	cwd: string;
	overrides: RunOverrides;
	/** Values the caller actually supplied, before environment defaults fold in. */
	explicitModel: string | undefined;
	explicitThinking: ThinkingLevel | undefined;
	timeoutMs: number | undefined;
	hasSessionProps: boolean;
}

function parseStart(input: Record<string, unknown>): ParsedStart {
	const allowed = new Set([
		"prompt",
		"session_id",
		"idempotency_key",
		"cwd",
		"model",
		"thinking",
		"timeout_ms",
		"tools",
		"no_tools",
		"system_prompt_append",
	]);
	for (const key of Object.keys(input)) {
		if (allowed.has(key)) continue;
		if (key === "transport") {
			throw invalid("async runs always use the rpc transport; do not pass transport");
		}
		throw invalid(`unknown field "${key}"`);
	}

	const prompt = readPrompt(input.prompt, "pi_start");

	const sessionIdRaw = input.session_id;
	let sessionId: string | null = null;
	if (sessionIdRaw !== undefined && sessionIdRaw !== null) {
		if (typeof sessionIdRaw !== "string" || !UUID_RE.test(sessionIdRaw)) {
			throw invalid("session_id must be a pi session UUID (or omitted to start a new session)");
		}
		sessionId = sessionIdRaw;
	}

	const keyRaw = input.idempotency_key;
	let idempotencyKey: string | null = null;
	if (keyRaw !== undefined && keyRaw !== null && keyRaw !== "") {
		if (typeof keyRaw !== "string" || keyRaw.length > 256) {
			throw invalid("idempotency_key must be a string of at most 256 characters");
		}
		idempotencyKey = keyRaw;
	}

	const cwdRaw = input.cwd;
	let explicitCwd = false;
	let cwd: string;
	if (cwdRaw !== undefined && cwdRaw !== null && cwdRaw !== "") {
		if (typeof cwdRaw !== "string") throw invalid("cwd must be a string");
		if (!cwdRaw.startsWith("/")) throw invalid(`cwd must be an absolute path: ${cwdRaw}`);
		explicitCwd = true;
		cwd = cwdRaw;
	} else {
		cwd = process.cwd();
	}

	const hasSessionProps =
		input.tools !== undefined || input.no_tools !== undefined || input.system_prompt_append !== undefined;

	return {
		prompt,
		sessionId,
		idempotencyKey,
		explicitCwd,
		cwd,
		overrides: readOverrides(input),
		explicitModel: input.model === undefined ? undefined : String(input.model),
		explicitThinking:
			input.thinking === undefined ? undefined : isThinkingLevel(input.thinking) ? input.thinking : undefined,
		timeoutMs: readTimeout(input.timeout_ms),
		hasSessionProps,
	};
}

async function startExecution(input: Record<string, unknown>, token: CancelToken): Promise<ToolResult> {
	let parsed: ParsedStart;
	try {
		parsed = parseStart(input);
		// The same pi-value validation the synchronous tools run; for a
		// continuation these fields are rejected outright in the scheduler.
		if (parsed.sessionId !== null && parsed.hasSessionProps) {
			throw invalid("tools, no_tools and system_prompt_append are only accepted on a new session");
		}
		if (parsed.sessionId === null) resolveCwd(parsed.explicitCwd ? parsed.cwd : undefined);
	} catch (err) {
		return errorResult("pi_start", err instanceof AsyncToolError ? err : invalid((err as Error).message));
	}

	try {
		const accepted = await accept(
			{
				prompt: parsed.prompt,
				sessionId: parsed.sessionId,
				idempotencyKey: parsed.idempotencyKey,
				cwd: parsed.cwd,
				explicitCwd: parsed.explicitCwd,
				model: parsed.explicitModel,
				thinking: parsed.explicitThinking,
				timeoutMs: parsed.timeoutMs,
				tools: parsed.overrides.tools,
				noTools: parsed.overrides.no_tools === true,
				systemPromptAppend: parsed.overrides.system_prompt_append,
			},
			token,
		);
		return payloadResult({
			execution_id: accepted.executionId,
			session_id: accepted.sessionId,
			state: accepted.state,
		});
	} catch (err) {
		return errorResult("pi_start", err);
	}
}

// --- pi_status -------------------------------------------------------------------

function statusPayload(record: ExecutionRecord): Record<string, unknown> {
	// A bounded, safe summary: the last few committed progress notes only.
	const notes: string[] = [];
	for (const event of [...record.events].reverse()) {
		if (event.type !== "progress") continue;
		notes.unshift(event.text);
		if (notes.length >= 3) break;
	}
	return {
		execution_id: record.execution_id,
		session_id: record.session_id,
		state: record.state,
		accepted_at: record.accepted_at,
		started_at: record.started_at,
		finished_at: record.finished_at,
		progress_summary: { notes, retained_events: record.events.length },
		latest_seq: record.latest_seq,
		oldest_retained_seq: record.oldest_retained_seq,
	};
}

function readExecutionId(input: Record<string, unknown>): string {
	const id = input.execution_id;
	if (typeof id !== "string" || !UUID_RE.test(id)) {
		throw invalid("execution_id must be the UUID returned by pi_start");
	}
	return id;
}

async function statusExecution(input: Record<string, unknown>): Promise<ToolResult> {
	try {
		const record = statusOf(readExecutionId(input)).record;
		return payloadResult(statusPayload(record));
	} catch (err) {
		return errorResult("pi_status", err);
	}
}

// --- pi_wait ---------------------------------------------------------------------

function readWaitTimeout(value: unknown): number {
	if (value === undefined) return WAIT_DEFAULT_MS;
	if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
		throw invalid("timeout_ms must be a non-negative integer number of milliseconds");
	}
	if (value > WAIT_MAX_MS) {
		throw invalid(`timeout_ms ${value} exceeds the pi_wait ceiling of ${WAIT_MAX_MS} ms`);
	}
	return value;
}

/** Resolves after `ms`, or as soon as the request is cancelled. Leaves no timer or subscription behind. */
function pause(ms: number, token: CancelToken): Promise<void> {
	return new Promise((resolve) => {
		if (token.cancelled) return resolve();
		let unsubscribe = (): void => {};
		const timer = setTimeout(() => {
			unsubscribe();
			resolve();
		}, ms);
		unsubscribe = token.subscribe(() => {
			clearTimeout(timer);
			unsubscribe();
			resolve();
		});
	});
}

async function waitExecution(input: Record<string, unknown>, token: CancelToken): Promise<ToolResult> {
	try {
		const allowed = new Set(["execution_id", "timeout_ms"]);
		for (const key of Object.keys(input)) {
			if (!allowed.has(key)) throw invalid(`unknown field "${key}"`);
		}
		const id = readExecutionId(input);
		const timeoutMs = readWaitTimeout(input.timeout_ms);
		const deadline = performance.now() + timeoutMs;
		for (;;) {
			// The same read pi_status does, every round: an expiry or store failure surfaces as its typed error.
			const record = statusOf(id).record;
			const remaining = deadline - performance.now();
			if (isTerminal(record.state) || remaining <= 0 || token.cancelled) {
				return payloadResult(statusPayload(record));
			}
			await pause(Math.min(WAIT_POLL_MS, remaining), token);
		}
	} catch (err) {
		return errorResult("pi_wait", err);
	}
}

// --- pi_result --------------------------------------------------------------------

/**
 * The page budget is spent on bytes actually written: the whole JSON-RPC
 * frame — envelope, the payload carried twice (structuredContent, and its JSON
 * text copy with the added escaping of being embedded as a string), and the
 * newline that ends the frame on the wire. Measured, never estimated — UTF-8
 * bytes, real escaping, and the exact id the client will see echoed back,
 * which may be a long string.
 */
function resultFrameBytes(payload: Record<string, unknown>, replyId: unknown): number {
	const frame = { jsonrpc: "2.0", id: replyId, result: payloadResult(payload) };
	return Buffer.byteLength(JSON.stringify(frame), "utf8") + 1;
}

/** In-band, on every page-clipped final: the bytes are recoverable — this is
 * how. `page_clipped` alone would only say that bytes are missing. */
const FINAL_RECOVERY = "the full retained final is paged by pi_result with final_only=true";

/** The bounded refusal for an impossible envelope: no oversized success, a
 * short fixed message, and the cap that caused it. The refusal frame is
 * measured like any other: everything the server writes stays under the
 * cap, with one explicit exemption — the echoed request id, which JSON-RPC
 * requires and the caller chose. */
function pageTooSmall(detail: string): AsyncToolError {
	return new AsyncToolError("page_too_small", `PI_MCP_ASYNC_PAGE_BYTES=${asyncPageBytes()} cannot carry ${detail}`);
}

function resultPayload(
	record: ExecutionRecord,
	afterSeq: number | null,
	limit: number,
	replyId: unknown,
): Record<string, unknown> {
	const floor = record.oldest_retained_seq;
	const latest = record.latest_seq;

	let startSeq: number;
	let expired = false;
	if (afterSeq === null) {
		startSeq = floor;
	} else {
		if (afterSeq > latest) throw new AsyncToolError("invalid_cursor", "cursor is ahead of the committed sequence");
		if (afterSeq + 1 < floor) {
			expired = true;
			startSeq = floor;
		} else {
			startSeq = afterSeq + 1;
		}
	}

	const project = (event: ProjectionEvent): Record<string, unknown> => ({
		seq: event.seq,
		type: event.type,
		text: event.text,
		...(event.truncated === true ? { truncated: true } : {}),
		...(event.dropped !== undefined ? { dropped: event.dropped } : {}),
	});

	const base = {
		execution_id: record.execution_id,
		session_id: record.session_id,
		state: record.state,
	};

	// The final answer shares the frame with the events. It rides the terminal
	// page — the one whose cursor has nothing after it — and any earlier page it
	// still fits on; a big answer never starves the event stream it pages with.
	// A null before terminal is one key, attached everywhere like any field.
	const frameWith = (selected: Record<string, unknown>[], final: unknown): number =>
		resultFrameBytes(
			{
				...base,
				events: selected,
				// The cursor/has_more fields depend on what is selected; charge the
				// caller the worst case so the shipped frame can only be smaller.
				next_cursor: encodeCursor(record.execution_id, Number.MAX_SAFE_INTEGER),
				has_more: true,
				oldest_retained_seq: Number.MAX_SAFE_INTEGER,
				latest_seq: Number.MAX_SAFE_INTEGER,
				...(expired ? { cursor_expired: true } : {}),
				final,
			},
			replyId,
		);

	/** The final this page ships: whole where it fits, clipped (and marked) on
	 * the first cursorless page and the terminal one, absent in between. */
	const pageFinal = (
		projected: Record<string, unknown>[],
		final: FinalOutcome | null,
		fresh: boolean,
		terminal: boolean,
	): FinalOutcome | (FinalOutcome & { page_clipped: true; recovery_hint: string }) | null | undefined => {
		// A terminal record without an answer reports final: null on every
		// page — one key, present like any field, not silently missing.
		if (final === null) return null;
		if (frameWith(projected, final) <= asyncPageBytes()) return final;
		if (!fresh && !terminal) return undefined;
		// The marked ship: the stored final plus the delivery marks. Everything
		// below measures exactly this object — a candidate that fits is the
		// object that ships, byte for byte, hint included.
		if (final.answer === null) {
			// Nothing to shrink: an oversized frame with no answer text is the
			// envelope itself, and clipping cannot help it. Ship it marked.
			return { ...final, page_clipped: true, recovery_hint: FINAL_RECOVERY };
		}
		const marked = (text: string): FinalOutcome & { page_clipped: true; recovery_hint: string } => ({
			...final,
			answer: { ...final.answer, text } as FinalOutcome["answer"],
			page_clipped: true,
			recovery_hint: FINAL_RECOVERY,
		});
		// Binary-search the largest prefix of the answer text whose frame fits.
		// lo always fits (an empty-text final is envelope-sized), hi never does.
		const text = final.answer.text;
		let lo = 0;
		let hi = text.length;
		while (lo + 1 < hi) {
			const mid = (lo + hi) >> 1;
			if (frameWith(projected, marked(text.slice(0, mid))) <= asyncPageBytes()) lo = mid;
			else hi = mid;
		}
		// Never ship half of a surrogate pair.
		let end = lo;
		if (end > 0 && (text.charCodeAt(end - 1) & 0xfc00) === 0xd800 && (text.charCodeAt(end) & 0xfc00) === 0xdc00) {
			end -= 1;
		}
		return marked(text.slice(0, end));
	};

	// The cap is enforced on the exact frame this call will write, newline
	// included. If even an empty page — no events, the final shrunk to no text
	// — cannot fit it, no honest success exists: refuse with a bounded typed
	// error instead of shipping an oversized frame and pretending the caller
	// got a page. One exemption is explicit: the echoed request id is part of
	// the frame, is required by JSON-RPC, and is the caller's choice, so it is
	// not counted against the cap — a caller echoing a huge string id can
	// make every page impossible, and the refusal then says which side is
	// over: the id, not the record.
	// The largest final any page might ship: the stored final, answer whole,
	// plus the delivery marks. The event loop charges this — the biggest
	// thing the frame could be asked to carry — while pageFinal below measures
	// the exact object it returns.
	const markedFull =
		record.final === null ? null : { ...record.final, page_clipped: true, recovery_hint: FINAL_RECOVERY };
	const emptyFinal =
		record.final === null
			? null
			: record.final.answer === null
				? { ...record.final, page_clipped: true, recovery_hint: FINAL_RECOVERY }
				: {
						...record.final,
						answer: { ...record.final.answer, text: "" },
						page_clipped: true,
						recovery_hint: FINAL_RECOVERY,
					};
	if (frameWith([], emptyFinal) > asyncPageBytes()) {
		// Which side is over? Everything the server writes, with the id's bytes
		// set aside, still inside the cap means the caller's id is what pushed
		// the frame out — the refusal names the exemption instead of blaming
		// the record.
		const idBytes = replyId === undefined ? 0 : Buffer.byteLength(JSON.stringify(replyId), "utf8");
		if (frameWith([], emptyFinal) - idBytes <= asyncPageBytes()) {
			throw pageTooSmall(
				"even an empty page beside the echoed request id — the id is echoed as the protocol requires and is the only cap-exempt part of the frame; this refusal is all the server adds",
			);
		}
		throw pageTooSmall("even an empty page for this execution; raise the cap to read it");
	}

	// Select committed events in order, under both the count and byte budgets,
	// with the final charged up front: it ships on the terminal page and on any
	// earlier page it still fits on, so its bytes belong to the budget whenever
	// it will ride the frame. The first event must fit the frame — whole when
	// it can, its text clipped (and marked) when the page is too small for it
	// — so a page is never emitted oversized and the cursor always advances.
	// If not even an empty-text event fits, the events cannot be paged under
	// this cap at all: the typed refusal is the honest answer, and the final
	// stays reachable through final_only.
	const cap = asyncPageBytes();

	// What the final contributes before any event is placed: its empty-text
	// floor on every page. A cursorless page and the terminal page always
	// carry it — clipped to fit — and a cursor page carries it whole when it
	// fits, so every page is budgeted for the final it might have to ship.
	// Charging less (nothing on mid pages) would size the events without the
	// final and break the terminal page's clip invariant; charging the whole
	// final up front would refuse pages the cap can honestly carry.

	/** Clip an event's text at page level: the largest prefix whose frame, with
	 * the final charged as this page will ship it, fits the cap. Null when not
	 * even an empty-text event fits — the page budget cannot carry an event at
	 * all. The marker is `page_clipped`, the same delivery-level word the final
	 * uses: the stored event is whole; only this page's copy is short. */
	const clipEventToFrame = (
		event: ProjectionEvent,
		final: FinalOutcome | null,
		budget: number,
	): Record<string, unknown> | null => {
		const text = event.text ?? "";
		if (frameWith([{ ...project(event), text: "", page_clipped: true }], final) > budget) return null;
		let lo = 0;
		let hi = text.length;
		while (lo + 1 < hi) {
			const mid = (lo + hi) >> 1;
			const candidate = { ...project(event), text: text.slice(0, mid), page_clipped: true };
			if (frameWith([candidate], final) <= budget) lo = mid;
			else hi = mid;
		}
		let end = lo;
		if (end > 0 && (text.charCodeAt(end - 1) & 0xfc00) === 0xd800 && (text.charCodeAt(end) & 0xfc00) === 0xdc00) {
			end -= 1;
		}
		return { ...project(event), text: text.slice(0, end), page_clipped: true };
	};

	const selected: Record<string, unknown>[] = [];
	let lastSeq: number | undefined;
	for (const event of record.events) {
		if (event.seq < startSeq) continue;
		if (selected.length >= limit) break;
		if (selected.length === 0) {
			const whole = project(event);
			if (frameWith([whole], emptyFinal) <= cap) {
				selected.push(whole);
				lastSeq = event.seq;
				continue;
			}
			const clipped = clipEventToFrame(event, emptyFinal, cap);
			if (clipped === null) {
				throw pageTooSmall(
					"not even one event fits; raise the cap to page the events, or read the final with final_only",
				);
			}
			selected.push(clipped);
			lastSeq = event.seq;
			break;
		}
		if (frameWith([...selected, project(event)], markedFull) > cap) break;
		selected.push(project(event));
		lastSeq = event.seq;
	}

	const hasMore =
		lastSeq === undefined
			? record.events.some((event) => event.seq >= startSeq)
			: record.events.some((event) => event.seq > lastSeq);

	// The final rides any page it fits on whole. It also rides the first page
	// a caller sees without a cursor — the settled answer is recoverable in one
	// call, without replaying the event sequence — and the terminal page, even
	// when it must be clipped to fit: never silently exempt from the cap. A page
	// clip is delivery-level only — the stored final stays whole — and is marked
	// `page_clipped` with a `recovery_hint` pointing at final_only, so the
	// caller knows both that bytes are missing and how to get them, and that
	// it was the page, not pi, that clipped (`answer.truncated` says pi did).
	const finalOut = pageFinal(selected, record.final, afterSeq === null, !hasMore);

	let nextCursor: string;
	if (lastSeq !== undefined) nextCursor = encodeCursor(record.execution_id, lastSeq);
	else if (afterSeq !== null && !expired) nextCursor = encodeCursor(record.execution_id, afterSeq);
	else nextCursor = encodeCursor(record.execution_id, latest);

	return {
		...base,
		events: selected,
		next_cursor: nextCursor,
		has_more: hasMore,
		oldest_retained_seq: floor,
		latest_seq: latest,
		...(expired ? { cursor_expired: true } : {}),
		...(finalOut !== undefined ? { final: finalOut } : {}),
	};
}

/**
 * The final alone, paged by UTF-8 bytes: the documented recovery path for a
 * `page_clipped` final — no events, no progress replay, no cap raise needed.
 * The whole final metadata rides every chunk; only the answer text is paged,
 * each chunk measured into the same exact-frame budget, and the chunks
 * concatenate to the full retained answer text.
 */
function finalPayload(record: ExecutionRecord, offset: number, replyId: unknown): Record<string, unknown> {
	const base = { execution_id: record.execution_id, session_id: record.session_id, state: record.state };
	const cap = asyncPageBytes();
	const final = record.final;
	if (final === null) return { ...base, final: null, has_more: false, next_cursor: null };
	if (final.answer === null) {
		// No text to page: the whole final in one frame, or the honest refusal.
		const payload = { ...base, final, final_offset: 0, final_bytes: 0, has_more: false, next_cursor: null };
		if (resultFrameBytes(payload, replyId) > cap) {
			throw pageTooSmall("the final of this execution; raise the cap to read it");
		}
		return payload;
	}

	const text = final.answer.text;
	const totalBytes = Buffer.byteLength(text, "utf8");
	// Server-issued offsets always land on a code-unit boundary; a defensive
	// clamp keeps a hand-made cursor honest instead of erroring mid-answer.
	const from = Math.min(offset, totalBytes);

	// Byte offset -> code-unit index: the largest prefix whose UTF-8 byte length
	// is <= the offset. On a boundary this is exact.
	let lo = 0;
	let hi = text.length;
	while (lo < hi) {
		const mid = (lo + hi + 1) >> 1;
		if (Buffer.byteLength(text.slice(0, mid), "utf8") <= from) lo = mid;
		else hi = mid - 1;
	}
	const start = lo;

	const chunkAt = (end: number): Record<string, unknown> => ({
		...base,
		final: { ...final, answer: { ...final.answer, text: text.slice(start, end) } },
		final_offset: from,
		final_bytes: totalBytes,
		// Worst-case cursor: the shipped next_cursor can only be shorter.
		has_more: true,
		next_cursor: encodeFinalCursor(record.execution_id, totalBytes),
	});

	// Largest chunk end whose exact frame fits. lo always fits (the empty chunk
	// is the minimal frame, already above the cap's floor for this shape);
	// hi never does — or it is the end of the text.
	lo = start;
	hi = text.length;
	const fits = (end: number): boolean => resultFrameBytes(chunkAt(end), replyId) <= cap;
	if (!fits(start)) throw pageTooSmall("even an empty final_only page for this execution");
	while (lo < hi) {
		const mid = (lo + hi + 1) >> 1;
		if (fits(mid)) lo = mid;
		else hi = mid - 1;
	}
	let end = lo;
	if (
		end < text.length &&
		(text.charCodeAt(end - 1) & 0xfc00) === 0xd800 &&
		(text.charCodeAt(end) & 0xfc00) === 0xdc00
	) {
		end -= 1; // never split a surrogate pair
	}
	if (end === start && start < text.length) {
		throw pageTooSmall("even one character of this final's answer; raise the cap to read it");
	}
	const more = end < text.length;
	return {
		...base,
		final: { ...final, answer: { ...final.answer, text: text.slice(start, end) } },
		final_offset: from,
		final_bytes: totalBytes,
		has_more: more,
		next_cursor: more ? encodeFinalCursor(record.execution_id, Buffer.byteLength(text.slice(0, end), "utf8")) : null,
	};
}

async function resultExecution(input: Record<string, unknown>, replyId: unknown): Promise<ToolResult> {
	try {
		const id = readExecutionId(input);
		const limitRaw = input.limit;
		let limit = ASYNC_PAGE_EVENTS;
		if (limitRaw !== undefined && limitRaw !== null) {
			if (typeof limitRaw !== "number" || !Number.isInteger(limitRaw) || limitRaw < 1 || limitRaw > 1000) {
				throw invalid("limit must be an integer between 1 and 1000");
			}
			limit = limitRaw;
		}
		const cursorRaw = input.after_cursor;
		const finalOnlyRaw = input.final_only;
		if (finalOnlyRaw !== undefined && finalOnlyRaw !== null && typeof finalOnlyRaw !== "boolean") {
			throw invalid("final_only must be a boolean");
		}
		if (finalOnlyRaw === true) {
			let offset = 0;
			if (cursorRaw !== undefined && cursorRaw !== null) {
				if (typeof cursorRaw !== "string") throw invalid("after_cursor must be a string");
				const decoded = decodeFinalCursor(id, cursorRaw);
				if (decoded === null) {
					throw new AsyncToolError(
						"invalid_cursor",
						"after_cursor is malformed, from another execution, or an event cursor; final_only pages advance with the cursors they return",
					);
				}
				offset = decoded;
			}
			return payloadResult(finalPayload(statusOf(id).record, offset, replyId));
		}
		let afterSeq: number | null = null;
		if (cursorRaw !== undefined && cursorRaw !== null) {
			if (typeof cursorRaw !== "string") throw invalid("after_cursor must be a string");
			afterSeq = decodeCursor(id, cursorRaw);
			if (afterSeq === null) {
				throw new AsyncToolError(
					"invalid_cursor",
					"after_cursor is malformed, from another execution, or ahead of the committed sequence",
				);
			}
		}
		const record = statusOf(id).record;
		return payloadResult(resultPayload(record, afterSeq, limit, replyId));
	} catch (err) {
		return errorResult("pi_result", err);
	}
}

// --- pi_cancel -----------------------------------------------------------------------

async function cancelExecutionTool(input: Record<string, unknown>): Promise<ToolResult> {
	try {
		const view: ExecutionView = await cancelExecution(readExecutionId(input));
		return payloadResult({
			execution_id: view.executionId,
			session_id: view.sessionId,
			state: view.state,
			finished_at: view.finishedAt,
		});
	} catch (err) {
		return errorResult("pi_cancel", err);
	}
}

// --- dispatch -------------------------------------------------------------------------

export async function callAsyncTool(
	name: unknown,
	input: Record<string, unknown>,
	token: CancelToken,
	replyId: unknown,
): Promise<ToolResult | null> {
	if (name === "pi_start") return startExecution(input, token);
	if (name === "pi_status") return statusExecution(input);
	if (name === "pi_wait") return waitExecution(input, token);
	if (name === "pi_result") return resultExecution(input, replyId);
	if (name === "pi_cancel") return cancelExecutionTool(input);
	return null;
}
