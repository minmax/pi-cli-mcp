import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

function numEnv(name: string, fallback: number, min: number): number {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return fallback;
	const value = Number(raw);
	if (!Number.isFinite(value) || value < min) {
		process.stderr.write(`pi-mcp: ignoring invalid ${name}=${raw}, using ${fallback}\n`);
		return fallback;
	}
	return value;
}

/** Like numEnv, but an absent variable means "no limit" rather than a number. */
function numEnvOrUndefined(name: string, min: number): number | undefined {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return undefined;
	const value = Number(raw);
	if (!Number.isFinite(value) || value < min) {
		process.stderr.write(`pi-mcp: ignoring invalid ${name}=${raw}, using no limit\n`);
		return undefined;
	}
	return value;
}

export const PI_BIN = process.env.PI_MCP_BIN ?? "pi";

/**
 * Optional command prefix, e.g. PI_MCP_WRAP="sandbox-exec -f /path/profile.sb".
 * Lets you jail pi without this server knowing anything about the sandbox.
 */
export const PI_WRAP = (process.env.PI_MCP_WRAP ?? "").trim();

/**
 * Server-wide wall clock for one run. Off by default: only the caller knows how
 * long its own task may take, so the right deadline is a property of the task,
 * not of the server — a task killed at an arbitrary global deadline loses its
 * result even though the work was done. Set PI_MCP_TIMEOUT_MS to install a
 * default; a caller can still raise or lower it per call with `timeout_ms`.
 */
export const TIMEOUT_MS = numEnvOrUndefined("PI_MCP_TIMEOUT_MS", 1_000);
/** Hard ceiling on what a per-call `timeout_ms` may ask for. 24h by default. */
export const MAX_TIMEOUT_MS = numEnv("PI_MCP_MAX_TIMEOUT_MS", 86_400_000, 1_000);
export const KILL_GRACE_MS = numEnv("PI_MCP_KILL_GRACE_MS", 5_000, 0);
/** How long shutdown waits, after SIGKILL, to confirm each pi process group
 * is actually empty before handing over the async store lock. */
export const TREE_GONE_MS = numEnv("PI_MCP_TREE_GONE_MS", 5_000, 0);

/**
 * How long a transport that can end a turn in-protocol gets to do so before
 * signals are sent. Only the rpc transport can: it sends pi's own `abort`.
 * Counted on top of the run's deadline, so a timeout can overrun by this much.
 */
export const ABORT_GRACE_MS = numEnv("PI_MCP_ABORT_GRACE_MS", 5_000, 0);

/**
 * The answer is what the caller asked for, so it is NOT truncated by default: if
 * pi produced it, it goes to the caller in full. Set PI_MCP_MAX_OUTPUT to opt
 * into a cap. MAX_CAPTURE below is the only backstop, and it bounds the read
 * buffer so a runaway stream cannot exhaust memory — a process guard, not an
 * editorial limit on the answer.
 */
export const MAX_OUTPUT = process.env.PI_MCP_MAX_OUTPUT
	? numEnv("PI_MCP_MAX_OUTPUT", Number.POSITIVE_INFINITY, 1_000)
	: Number.POSITIVE_INFINITY;

/** stderr is diagnostics, not the deliverable: keep only a small tail. */
export const STDERR_LIMIT = numEnv("PI_MCP_STDERR_LIMIT", 1_500, 200);

/**
 * Forward stderr verbatim, including lines that parse as protocol events.
 * The strict reading of the protocol is that events belong on stdout, so such a
 * line is a channel violation; dropping it is a guard on top, and this switch
 * turns the guard off.
 */
export const KEEP_STDERR_EVENTS = process.env.PI_MCP_STDERR_KEEP_EVENTS === "1";

export const MAX_CAPTURE = numEnv("PI_MCP_MAX_CAPTURE", 16_000_000, 100_000);

/**
 * Bounds on a single unterminated line: one from pi's event stream, one from the
 * client's JSON-RPC stream. Without them a stream that never sends a newline
 * grows until the process dies, taking every pending response with it.
 */
export const MAX_LINE = numEnv("PI_MCP_MAX_LINE", 8_000_000, 100_000);
export const MAX_FRAME = numEnv("PI_MCP_MAX_FRAME", 8_000_000, 100_000);

export const MAX_SESSIONS = numEnv("PI_MCP_MAX_SESSIONS", 1_000, 1);
export const MAX_CONCURRENT = numEnv("PI_MCP_MAX_CONCURRENT", 100, 1);

export const STATE_FILE = process.env.PI_MCP_STATE ?? join(homedir(), ".local", "state", "pi-mcp", "sessions.json");

export const DEFAULT_MODEL = process.env.PI_MCP_MODEL ?? null;
export const DEFAULT_THINKING = process.env.PI_MCP_THINKING ?? null;

/**
 * Operator-owned argv tokens, prepended to every pi spawn — the place for a
 * flag no tool input reaches (e.g. one an operator needs to widen a permission
 * extension's rpc behaviour). Comma- or space-separated, so a token may not
 * itself contain either. Tokens are passed verbatim: one that is not a flag,
 * or repeats a flag the server sets (e.g. --mode), is parsed by pi its own
 * way — and pi's error text for a bad token reaches the caller, fail closed.
 * Off unless set; callers cannot set it.
 */
export const EXTRA_ARGS: readonly string[] = (process.env.PI_MCP_EXTRA_ARGS ?? "")
	.split(/[\s,]+/)
	.filter((token) => token !== "");

/**
 * Which way to drive pi when a call does not say.
 *
 * `rpc` by default: it is a superset of what `print` can do. Same event stream,
 * same answer, plus a running turn stays reachable — and an interrupted one is
 * ended with pi's own `abort`, which keeps the tail of the stream that a signal
 * would cost. `print` (`pi -p --mode json`, one process per turn) remains for
 * anything that prefers a process that cannot be talked to.
 */
export const DEFAULT_TRANSPORT = process.env.PI_MCP_TRANSPORT === "print" ? "print" : "rpc";

/** argv has an OS size limit; longer prompts go through a temp file instead. */
export const MAX_PROMPT = 2_000_000;
export const ARGV_PROMPT_LIMIT = 100_000;

/**
 * Reported in the MCP handshake. Read from the manifest rather than repeated
 * here: a second copy is a second thing to forget on release, and it lands in
 * the one place a client actually looks. The path resolves the same from `src/`
 * and from the built `dist/`, both being one level below the package root.
 */
function readVersion(): string {
	try {
		const manifest = readFileSync(new URL("../package.json", import.meta.url), "utf8");
		const version = (JSON.parse(manifest) as { version?: unknown }).version;
		if (typeof version === "string") return version;
	} catch {
		// Unreadable manifest is not worth failing a handshake over.
	}
	return "0.0.0";
}

export const SERVER_INFO = { name: "pi", version: readVersion() } as const;
export const PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"] as const;
export const FALLBACK_PROTOCOL = "2025-06-18";

// --- opt-in async executions (Phase 1) -------------------------------------

/**
 * Absolute, private, dedicated directory for the async execution store. When
 * unset, the adapter exposes exactly the original six tools and runs nothing in
 * the background. When set, pi_start/pi_status/pi_result/pi_cancel are appended
 * after them. Anything other than an absolute path disables the subsystem with
 * a warning instead of guessing what the operator meant.
 */
export const ASYNC_STATE_DIR = process.env.PI_MCP_ASYNC_STATE_DIR ?? null;

/** Total nonterminal async executions (queued + running + cancelling). */
export const ASYNC_MAX_QUEUED = numEnv("PI_MCP_ASYNC_MAX_QUEUED", 32, 0);
/** Same, per pi session: one turn per session, plus a bounded waiting room. */
export const ASYNC_MAX_QUEUED_PER_SESSION = numEnv("PI_MCP_ASYNC_MAX_QUEUED_PER_SESSION", 8, 0);
/** Aggregate bytes of prompts held in memory for queued plans. */
export const ASYNC_MAX_QUEUED_BYTES = numEnv("PI_MCP_ASYNC_MAX_QUEUED_BYTES", 16_777_216, 1);

/** Retained projection events per execution. */
export const ASYNC_MAX_EVENTS = numEnv("PI_MCP_ASYNC_MAX_EVENTS", 256, 1);
/**
 * Unflushed-event memory bound per execution — independent of the retained
 * event bound: overflow drops from the front and is replaced by a `gap`
 * marker at the next commit, so the committed sequence stays contiguous.
 */
export const ASYNC_PENDING_EVENTS = numEnv("PI_MCP_ASYNC_PENDING_EVENTS", 512, 1);
/** Retained projection bytes per execution (the event ring, not the final). */
export const ASYNC_MAX_EXEC_EVENT_BYTES = numEnv("PI_MCP_ASYNC_MAX_EXEC_EVENT_BYTES", 262_144, 1_024);
/** Default events per pi_result page. */
export const ASYNC_PAGE_EVENTS = numEnv("PI_MCP_ASYNC_PAGE_EVENTS", 50, 1);
/** Byte cap on the serialized pi_result frame: UTF-8 bytes, both payload
 * representations, envelope and attached final included. */
export const ASYNC_PAGE_BYTES = numEnv("PI_MCP_ASYNC_PAGE_BYTES", 131_072, 1_024);
/** Clip for a single event's text. Kept below an eighth of the page budget: a
 * page carries each event twice (structuredContent + JSON text copy), so this
 * guarantees a page with unread retained events always returns at least one. */
export const ASYNC_EVENT_TEXT_LIMIT = numEnv("PI_MCP_ASYNC_EVENT_TEXT_LIMIT", 32_768, 256);
/** Default cap for the settled answer stored in a final (explicit truncation). */
export const ASYNC_MAX_FINAL_BYTES = numEnv("PI_MCP_ASYNC_MAX_FINAL_BYTES", 1_048_576, 1_024);

/** How often unflushed projection events are committed while a run is live. */
export const ASYNC_FLUSH_INTERVAL_MS = numEnv("PI_MCP_ASYNC_FLUSH_MS", 2_000, 100);

/** Partial history (projection events) expires this long after finish. */
export const ASYNC_PARTIAL_RETENTION_MS = numEnv("PI_MCP_ASYNC_PARTIAL_RETENTION_MS", 86_400_000, 1);
/** Finals and idempotency keys expire this long after finish. */
export const ASYNC_FINAL_RETENTION_MS = numEnv("PI_MCP_ASYNC_FINAL_RETENTION_MS", 604_800_000, 1);
/** Tombstones (expired keys) linger this long beyond final retention. */
export const ASYNC_TOMBSTONE_RETENTION_MS = numEnv("PI_MCP_ASYNC_TOMBSTONE_RETENTION_MS", 604_800_000, 1);

// --- opt-in completion hook -----------------------------------------------

/**
 * Operator-owned executable invoked once at the boundary where an async
 * execution's nonterminal → terminal transition has become durable. The
 * adapter adds no notifications of its own: unset, nothing is ever invoked;
 * set, the server spawns the path directly (no shell) with exactly two
 * arguments, the execution id and the terminal state. Anything other than an
 * absolute path disables the hook with a warning instead of guessing what the
 * operator meant — same rule as PI_MCP_ASYNC_STATE_DIR.
 */
export const COMPLETION_HOOK = (() => {
	const raw = (process.env.PI_MCP_COMPLETION_HOOK ?? "").trim();
	if (raw === "") return null;
	if (!isAbsolute(raw)) {
		process.stderr.write(
			"pi-mcp: PI_MCP_COMPLETION_HOOK must be an absolute path; the completion hook is disabled\n",
		);
		return null;
	}
	return raw;
})();

/** How long one hook invocation gets before its whole process group is
 * terminated (SIGTERM, then SIGKILL after a short grace — descendants
 * included, so an Apprise-style delivery child cannot linger). Capped: a
 * Node timer scheduled past 2^31-1 ms fires after ~1 ms instead, so a huge
 * configured value would turn "never time out" into "time out immediately".
 * The cap is the honest bound, and it is documented in the README. */
const COMPLETION_HOOK_TIMEOUT_CAP_MS = 600_000;
export const COMPLETION_HOOK_TIMEOUT_MS = (() => {
	const value = numEnv("PI_MCP_COMPLETION_HOOK_TIMEOUT_MS", 10_000, 100);
	if (value <= COMPLETION_HOOK_TIMEOUT_CAP_MS) return value;
	process.stderr.write(
		`pi-mcp: PI_MCP_COMPLETION_HOOK_TIMEOUT_MS is capped at ${COMPLETION_HOOK_TIMEOUT_CAP_MS} (larger values would overflow Node's timers)\n`,
	);
	return COMPLETION_HOOK_TIMEOUT_CAP_MS;
})();

/** Global store bounds; pressure prunes oldest terminal records first. */
export const ASYNC_MAX_RECORDS = numEnv("PI_MCP_ASYNC_MAX_RECORDS", 1_024, 10);
export const ASYNC_MAX_STORE_BYTES = numEnv("PI_MCP_ASYNC_MAX_STORE_BYTES", 67_108_864, 1_024);

/**
 * Effective per-event clip, in UTF-8 bytes. A page carries each event twice —
 * once inside structuredContent and once inside the JSON text copy — so the
 * clip must stay below an eighth of the page budget to guarantee a page with
 * unread retained events always returns at least one.
 */
export function asyncEventTextLimit(): number {
	return Math.min(ASYNC_EVENT_TEXT_LIMIT, Math.floor(asyncPageBytes() / 8));
}

/** Effective page budget: never let a configured page outrun the frame limit. */
export function asyncPageBytes(): number {
	return Math.min(ASYNC_PAGE_BYTES, Math.floor(MAX_FRAME / 8));
}

/**
 * Effective final-answer cap. Explicit truncation is reported; stricter
 * configured caps (PI_MCP_MAX_OUTPUT, frame headroom) are honored too.
 */
export function asyncFinalCap(): number {
	const byFrame = Number.isFinite(MAX_FRAME) ? Math.floor(MAX_FRAME / 4) : Number.POSITIVE_INFINITY;
	return Math.min(ASYNC_MAX_FINAL_BYTES, MAX_OUTPUT, byFrame);
}

// --- test seams (never set in production) ----------------------------------

/**
 * When set, the first store commit that would produce this state fails at
 * PI_MCP_ASYNC_TEST_FAIL_POINT ("before_rename" or "after_rename"). Used by the
 * suite to prove the fail-closed path; the store goes unhealthy on the first
 * hit and no further commits are attempted.
 */
export const ASYNC_TEST_FAIL_STATE = process.env.PI_MCP_ASYNC_TEST_FAIL_STATE ?? null;
export const ASYNC_TEST_FAIL_POINT =
	process.env.PI_MCP_ASYNC_TEST_FAIL_POINT === "after_rename" ? "after_rename" : "before_rename";

/**
 * Test seam: PI_MCP_ASYNC_TEST_FAIL_RETENTION=1 fails the next retention
 * mutation (event expiry, tombstone commit, or tombstone delete) exactly like
 * a real write failure — the store goes unhealthy and the error propagates
 * through lookup, key lookup, and the admission sweep.
 */
export const ASYNC_TEST_FAIL_RETENTION = process.env.PI_MCP_ASYNC_TEST_FAIL_RETENTION === "1";

/**
 * When set, pi_start blocks right before the acceptance commit until this path
 * exists. Opens the window in which an MCP request cancellation can still land
 * before the execution becomes durable.
 */
export const ASYNC_TEST_ACCEPT_GATE = process.env.PI_MCP_ASYNC_TEST_ACCEPT_GATE ?? null;

/**
 * When set, a dispatched job parks between its `running` commit and the spawn
 * until this path exists. Opens the exact pre-spawn cancellation window: a
 * cancel landing there must commit `cancelled` with zero launches.
 */
export const ASYNC_TEST_SPAWN_GATE = process.env.PI_MCP_ASYNC_TEST_SPAWN_GATE ?? null;
