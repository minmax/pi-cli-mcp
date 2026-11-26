// The opt-in completion hook (PI_MCP_COMPLETION_HOOK): one best-effort
// invocation of an operator-owned executable at the exact boundary where an
// async execution's nonterminal → terminal transition has become durable.
// The adapter itself still sends nothing — where and how to deliver (Apprise
// or otherwise) is entirely the wrapper's business.
//
// Contract, kept deliberately narrow:
//   - the executable is spawned directly (no shell) with exactly two
//     arguments, the execution id and the terminal state;
//   - it runs detached in its own process group with an empty environment:
//     nothing inherited from this server or its gateway reaches it, so every
//     secret and path it needs lives inside the wrapper itself;
//   - its output is never read and never logged — the server logs only a
//     bounded one-line status (start failure, nonzero exit, timeout) per
//     invocation, never wrapper output, target configuration, or arguments;
//   - it is fire-and-forget: no commit waits on it, no tool result is delayed
//     by it, and its failure can never mutate a record or fail a commit;
//   - the whole group is bounded: a timeout escalates SIGTERM → SIGKILL, and
//     a run is only forgotten once probing its group is ESRCH — every member
//     gone. A SIGKILL that cannot be confirmed empty inside the bounded
//     window is reported, never assumed.
//
// Delivery is best-effort, never durable: at most one attempt per durably
// committed qualifying transition, never retried and never replayed. A crash
// between the durable commit and the invocation loses the notification — the
// next startup sees an already-terminal record and stays silent by design.
// Killing this server outright promises nothing about in-flight hooks: only
// a clean shutdown reaps them.

import { type ChildProcess, spawn } from "node:child_process";
import { COMPLETION_HOOK, COMPLETION_HOOK_TIMEOUT_MS } from "../config.ts";
import type { ExecutionState } from "./types.ts";

/** Concurrent invocations before further qualifying transitions are skipped
 * with a logged note. Bounded with deferred cleanup in mind: a finished run
 * stays tracked until its group is confirmed empty. */
const MAX_IN_FLIGHT = 8;
/** SIGTERM → SIGKILL grace inside the timeout escalation, and the deferred
 * group sweep after any exit: a wrapper may background its delivery and exit
 * 0, and the sweep is what stops that descendant from outliving this server. */
const KILL_GRACE_MS = 500;
/** How long the sweep keeps probing for group-empty confirmation after its
 * SIGKILL before saying so and leaving the run tracked. Sending the signal is
 * not proof of exit; ESRCH on the group probe is. */
const CONFIRM_MS = 5_000;
const CONFIRM_POLL_MS = 50;

interface HookRun {
	child: ChildProcess;
	executionId: string;
	state: ExecutionState;
	/** The spawn itself failed (ENOENT, EACCES): nothing is running, and the
	 * close event that follows must not log a second status line. */
	failed: boolean;
	timedOut: boolean;
	/** The pending sweep or confirmation timer; every removal path cancels it,
	 * so no stale timer can outlive its run (and signal a recycled pgid). */
	sweep?: NodeJS.Timeout | undefined;
}

const runs = new Set<HookRun>();

function warn(message: string): void {
	process.stderr.write(`pi-mcp: ${message}\n`);
}

/** Signal the whole detached group. ESRCH means nothing is left to signal;
 * any other error is ignored here and settles at the confirmation probe. */
function signalGroup(run: HookRun, signal: NodeJS.Signals): void {
	if (run.child.pid === undefined) return;
	try {
		process.kill(-run.child.pid, signal);
	} catch {
		/* ESRCH: nothing left to signal */
	}
}

/**
 * True only when probing the group is ESRCH — every member gone. A spawn that
 * never produced a pid is gone by definition. Any other probe error (EPERM on
 * a foreign group, a recycled pgid) is NOT proof and keeps the run tracked:
 * forgetting it on a guess is how orphans and stale timers are made.
 */
function groupGone(run: HookRun): boolean {
	if (run.child.pid === undefined) return true;
	try {
		process.kill(-run.child.pid, 0);
		return false;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === "ESRCH";
	}
}

/** Forget a run: cancel its pending timer first, so no sweep or confirmation
 * can fire after removal. The only removal path — ESRCH is the only "gone". */
function forget(run: HookRun): void {
	if (run.sweep !== undefined) {
		clearTimeout(run.sweep);
		run.sweep = undefined;
	}
	runs.delete(run);
}

/**
 * Arm the deferred group sweep: SIGKILL the whole group after the grace
 * (idempotent, ESRCH-safe), then probe until the group is confirmed empty
 * within the bounded window. Only the confirmation forgets the run; an
 * unconfirmable group is reported once and stays tracked — the shutdown reap
 * then sees it and says so again instead of assuming the signal worked.
 */
function armSweep(run: HookRun): void {
	// A forgotten run must never be re-armed: a timer referencing it could
	// fire after its tracking ended, signaling a group it no longer owns.
	if (!runs.has(run)) return;
	if (run.sweep !== undefined) return;
	// Probe before arming: a normal exit with no lingering delivery descendant
	// leaves the group already empty, and arming a delayed SIGKILL for it
	// would only re-probe a dead group (and keep a needless timer ref'd).
	if (groupGone(run)) {
		forget(run);
		return;
	}
	run.sweep = setTimeout(() => {
		run.sweep = undefined;
		signalGroup(run, "SIGKILL");
		confirmGone(run, Date.now() + CONFIRM_MS);
	}, KILL_GRACE_MS);
}

function confirmGone(run: HookRun, deadline: number): void {
	if (groupGone(run)) {
		forget(run);
		return;
	}
	if (Date.now() >= deadline) {
		warn(
			`completion hook group could not be confirmed empty (${run.executionId} ${run.state}); ` +
				"check for leftover processes before trusting the delivery",
		);
		return;
	}
	run.sweep = setTimeout(() => confirmGone(run, deadline), CONFIRM_POLL_MS);
}

/**
 * The store's completed-commit boundary calls this after the terminal record
 * is durable (fsync, rename, directory fsync all done) and visible in memory.
 * Synchronous work here is one spawn, nothing more; a throw is impossible by
 * construction, and the store guards against it anyway.
 */
export function notifyCompletion(executionId: string, state: ExecutionState): void {
	if (COMPLETION_HOOK === null) return;
	if (runs.size >= MAX_IN_FLIGHT) {
		warn(`completion hook is busy (${runs.size} in flight); skipping notification for ${executionId} (${state})`);
		return;
	}

	let child: ChildProcess;
	try {
		child = spawn(COMPLETION_HOOK, [executionId, state], {
			shell: false,
			detached: true,
			stdio: "ignore",
			// Empty by design: no gateway secret, no server setting, nothing
			// inherited reaches the wrapper. It owns everything it needs.
			env: {},
		});
	} catch (err) {
		// spawn() throwing synchronously is our own bug; a bad path or a bad
		// permission arrives asynchronously through the "error" event below.
		// The error's own text is not logged: it can carry the configured path.
		warn(
			`completion hook could not be spawned (${(err as NodeJS.ErrnoException).code ?? "error"}); no notification for ${executionId} (${state})`,
		);
		return;
	}

	const run: HookRun = { child, executionId, state, failed: false, timedOut: false };
	runs.add(run);
	const timeoutTimer = setTimeout(() => {
		run.timedOut = true;
		warn(`completion hook timed out after ${COMPLETION_HOOK_TIMEOUT_MS}ms; terminating (${executionId} ${state})`);
		signalGroup(run, "SIGTERM");
		armSweep(run);
	}, COMPLETION_HOOK_TIMEOUT_MS);

	child.on("error", (err: NodeJS.ErrnoException) => {
		clearTimeout(timeoutTimer);
		run.failed = true;
		forget(run);
		warn(`completion hook could not start (${err.code ?? "error"}); no notification for ${executionId} (${state})`);
	});
	child.on("close", (code, signal) => {
		clearTimeout(timeoutTimer);
		// A failed spawn was already forgotten by its error handler; the close
		// event that follows must not re-track it, re-arm a sweep, or log a
		// second status line.
		if (!runs.has(run)) return;
		armSweep(run);
		if (run.failed || run.timedOut) return; // already logged, one line per event
		if (code !== 0) {
			warn(
				`completion hook exited with ${signal ?? `code ${code}`} (${executionId} ${state}); delivery is not confirmed`,
			);
		}
	});
}

/** How many hook groups may still be alive. Prunes runs whose group is
 * confirmed empty (probe ESRCH), so a finished notification never delays
 * shutdown by more than its own bounded lifetime. Anything unconfirmable
 * stays counted — it is exactly what the shutdown reap must still see. */
export function completionHookInFlight(): number {
	for (const run of [...runs]) {
		if (groupGone(run)) forget(run);
	}
	return runs.size;
}

/** Poll until every hook group is confirmed gone, or until `deadlineMs`
 * passes. Resolves false when a group could not be confirmed empty — the
 * caller must say so rather than claim the reap. The poll timers stay ref'd:
 * the only caller is process shutdown, where nothing else may hold the event
 * loop, and an unref'd poll would let node exit before the confirmation (or
 * the honest report that it failed) ever runs.
 */
export function completionHookGone(deadlineMs: number): Promise<boolean> {
	const started = Date.now();
	return new Promise<boolean>((resolve) => {
		const poll = (): void => {
			if (completionHookInFlight() === 0) {
				resolve(true);
				return;
			}
			if (Date.now() - started >= deadlineMs) {
				resolve(false);
				return;
			}
			setTimeout(poll, CONFIRM_POLL_MS);
		};
		poll();
	});
}

/** Shutdown's forced reap: SIGKILL every group still alive, descendants
 * included. The runs stay tracked on purpose — only the ESRCH probe that
 * follows (completionHookGone) may forget them. Sending the signal is not
 * proof of exit; claiming it would be the one lie this module must never
 * tell. There is no phase-1 SIGTERM on purpose: hooks keep the shutdown
 * grace window to finish delivering, and only what is still running at the
 * end of it is reaped. */
export function completionHookKill(): void {
	for (const run of [...runs]) {
		if (run.sweep !== undefined) {
			clearTimeout(run.sweep);
			run.sweep = undefined;
		}
		signalGroup(run, "SIGKILL");
	}
}
