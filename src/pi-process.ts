// Spawning pi, and the lifecycle guarantees around it: timeouts, cancellation,
// and never leaving a process behind.

import { spawn } from "node:child_process";
import {
	ABORT_GRACE_MS,
	KILL_GRACE_MS,
	MAX_CAPTURE,
	MAX_CONCURRENT,
	MAX_LINE,
	PI_BIN,
	PI_WRAP,
	TIMEOUT_MS,
	TREE_GONE_MS,
} from "./config.ts";
import type { CancelToken } from "./types.ts";

export interface RunResult {
	code: number;
	stdout: string;
	stderr: string;
	spawnErrorCode?: string;
	/** Native exit code, not the synthetic failure sentinel in code. */
	exitCode?: number;
	signal?: NodeJS.Signals;
	rpcPromptAcknowledged?: boolean;
	timedOut?: boolean;
	cancelled?: boolean;
	/**
	 * How many nonblank lines on pi's protocol stdout failed to parse as
	 * JSON. Any value above zero is a contract violation — the run must not be
	 * reported as a success even if the surviving events settle the turn.
	 * A count, not the text: the line itself is never echoed.
	 */
	corruptLines?: number;
	/** Lines dropped for exceeding MAX_LINE before they could be read. */
	droppedLines?: number;
	/**
	 * How an interrupted run actually ended: `abort` means the transport closed
	 * the turn in-protocol and pi exited on its own, `signal` means it had to be
	 * killed. Only set when the run was interrupted.
	 */
	endedBy?: "abort" | "signal";
	/**
	 * The leader exited but its process group could not be confirmed empty
	 * after SIGKILL within the verification window — a descendant may still
	 * be alive. Fail closed on this: it is not a clean end, whatever the
	 * leader's own exit code says.
	 */
	cleanupUnverified?: true;
}

/** Writing side of a running pi process, for transports that talk back. */
export interface PiHandle {
	/** Send one JSONL command on stdin. No-op once the process is gone. */
	send(command: unknown): void;
	/** Close stdin, which is how pi's rpc mode is asked to shut down. */
	endInput(): void;
}

export interface RunOptions {
	/** Called for each parsed JSON line of pi's stdout stream. */
	onEvent?: (event: unknown) => void;
	token?: CancelToken | undefined;
	/** Overrides the default wall clock for this run. */
	timeoutMs?: number | undefined;
	/**
	 * Open stdin as a pipe and hand the caller a writer. Print mode leaves stdin
	 * closed; rpc mode needs it to send commands.
	 */
	onStart?: (handle: PiHandle) => void;
	stdin?: "ignore" | "pipe";
	/**
	 * Called once the run's process group is confirmed empty — including
	 * after a fail-closed resolve that already returned with
	 * `cleanupUnverified`. The interval sweep that tracks the group fires it
	 * the moment ESRCH proves every member gone, so a caller that blocked the
	 * session on an unverified cleanup can release the block.
	 */
	onTreeGone?: (() => void) | undefined;
	/**
	 * Asked first when a run has to end early, before any signal is sent.
	 *
	 * It exists because pi skips `flushRawStdout()` on SIGTERM (see its
	 * rpc-mode.ts), so signalling can cost the tail of the event stream — possibly
	 * the answer pi was in the middle of writing. A transport that can end the
	 * turn in-protocol gets ABORT_GRACE_MS to do so; signals follow either way.
	 */
	gracefulStop?: (reason: "timeout" | "cancelled") => void;
}

/**
 * Live pi process groups, so shutdown can take their whole trees with it.
 * Detached children do not die with this process on their own. A tree stays
 * tracked until its process group is *confirmed empty* — the leader closing
 * proves nothing, since a descendant that ignores SIGTERM would then never
 * receive the shutdown SIGKILL.
 */
export interface TreeHandle {
	/** Signal the whole process group; fall back to the single child. */
	signal(sig: NodeJS.Signals): void;
	/** True only once probing the process group is ESRCH — every member gone. */
	gone(): boolean;
}

const liveTrees = new Set<TreeHandle>();

export function treeCount(): number {
	return liveTrees.size;
}

export function killAllTrees(signal: NodeJS.Signals): void {
	for (const tree of [...liveTrees]) tree.signal(signal);
}

/** Drop trees whose groups are confirmed empty; true when none are left. */
export function treesGone(): boolean {
	for (const tree of [...liveTrees]) {
		if (tree.gone()) liveTrees.delete(tree);
	}
	return liveTrees.size === 0;
}

/**
 * Poll until every tracked group is confirmed gone, or until `deadlineMs`
 * passes. Resolves false when groups could not be confirmed empty — the caller
 * must then not hand over anything those groups could still be writing to.
 *
 * The poll timers are ref'd: the only caller is process shutdown, where nothing
 * else may hold the event loop (the client's pipes are often already closed by
 * then). An unref'd poll would let node exit before this resolves, cutting off
 * the group-empty confirmation and whatever the caller does with it.
 */
export function waitForTreesGone(deadlineMs: number): Promise<boolean> {
	const started = Date.now();
	return new Promise<boolean>((resolve) => {
		const poll = (): void => {
			if (treesGone()) {
				resolve(true);
				return;
			}
			if (Date.now() - started >= deadlineMs) {
				resolve(false);
				return;
			}
			setTimeout(poll, 50);
		};
		poll();
	});
}

export function makeCancelToken(): CancelToken {
	const listeners = new Set<() => void>();
	let cancelled = false;
	return {
		get cancelled() {
			return cancelled;
		},
		subscribe(fn: () => void) {
			if (cancelled) {
				fn();
				return () => {};
			}
			listeners.add(fn);
			return () => {
				listeners.delete(fn);
			};
		},
		cancel() {
			if (cancelled) return;
			cancelled = true;
			for (const fn of listeners) {
				try {
					fn();
				} catch (err) {
					process.stderr.write(`pi-mcp: cancel listener failed: ${(err as Error).message}\n`);
				}
			}
			listeners.clear();
		},
	};
}

// --- concurrency ----------------------------------------------------------

let running = 0;
const waiting: (() => void)[] = [];

/** pi runs are heavy and the client may fan out, so cap how many run at once. */
export async function withSlot<T>(fn: () => Promise<T>): Promise<T> {
	if (running >= MAX_CONCURRENT) {
		await new Promise<void>((resolve) => {
			waiting.push(resolve);
		});
	}
	running += 1;
	try {
		return await fn();
	} finally {
		running -= 1;
		waiting.shift()?.();
	}
}

// --- running pi -----------------------------------------------------------

function buildCommand(args: string[]): { command: string; argv: string[] } {
	if (!PI_WRAP) return { command: PI_BIN, argv: args };
	const parts = PI_WRAP.split(/\s+/);
	const [command, ...prefix] = parts;
	return { command: command ?? PI_BIN, argv: [...prefix, PI_BIN, ...args] };
}

function appendCapped(current: string, chunk: string): string {
	if (current.length >= MAX_CAPTURE) return current;
	return current + chunk.slice(0, MAX_CAPTURE - current.length);
}

function spawnErrorCode(error: unknown): string {
	const code = (error as NodeJS.ErrnoException | null)?.code;
	return typeof code === "string" && /^[A-Z][A-Z0-9_]{0,31}$/.test(code) ? code : "UNKNOWN";
}

/**
 * Runs pi, streaming `--mode json` events to onEvent. Resolves with the raw
 * capture plus exit status; never rejects.
 */
export function runPi(args: string[], cwd: string, options: RunOptions = {}): Promise<RunResult> {
	const { onEvent, token } = options;
	const timeoutMs = options.timeoutMs ?? TIMEOUT_MS;

	return new Promise<RunResult>((resolve) => {
		// Cancelled while queued for a slot or a session lock: never start pi.
		if (token?.cancelled) {
			resolve({ code: -1, stdout: "", stderr: "", cancelled: true });
			return;
		}

		const { command, argv } = buildCommand(args);
		let child: ReturnType<typeof spawn>;
		try {
			// `detached` puts pi in its own process group so a timeout or cancel can
			// kill the whole tree. Without it, anything pi spawned (a `sleep`, a test
			// runner, a dev server) survives as an orphan when pi does not forward
			// the signal itself.
			child = spawn(command, argv, {
				cwd,
				stdio: [options.stdin ?? "ignore", "pipe", "pipe"],
				env: process.env,
				detached: true,
			});
		} catch (err) {
			resolve({
				code: -1,
				stdout: "",
				stderr: `failed to spawn ${command}: ${(err as Error).message}`,
				spawnErrorCode: spawnErrorCode(err),
			});
			return;
		}

		let stdout = "";
		let stderr = "";
		let pending = "";
		let timedOut = false;
		let cancelled = false;
		let corruptLines = 0;
		let droppedLines = 0;
		let killTimer: NodeJS.Timeout | undefined;
		let gracefulTimer: NodeJS.Timeout | undefined;
		let endedBy: "abort" | "signal" | undefined;

		// Signal the whole process group; fall back to the single child if the group
		// is already gone (ESRCH) or the platform refuses the negative pid.
		const signalTree = (signal: NodeJS.Signals): void => {
			try {
				if (child.pid !== undefined) process.kill(-child.pid, signal);
			} catch {
				try {
					child.kill(signal);
				} catch {
					// Already dead — nothing to do.
				}
			}
		};

		// ESRCH means no member of the group exists — not even a zombie. EPERM or
		// anything else means someone is still there; that is not "gone".
		const groupGone = (): boolean => {
			if (child.pid === undefined) return true;
			try {
				process.kill(-child.pid, 0);
				return false;
			} catch (err) {
				return (err as NodeJS.ErrnoException).code === "ESRCH";
			}
		};

		const tree: TreeHandle = { signal: signalTree, gone: groupGone };

		const hardStop = (): void => {
			endedBy = "signal";
			signalTree("SIGTERM");
			killTimer ??= setTimeout(() => signalTree("SIGKILL"), KILL_GRACE_MS);
		};

		let stopping = false;
		const stop = (reason: "timeout" | "cancelled"): void => {
			if (stopping) return;
			stopping = true;
			if (reason === "timeout") timedOut = true;
			if (reason === "cancelled") cancelled = true;

			// Ask in-protocol first when the transport can: pi then closes the turn
			// through its normal path and the events — including whatever it was
			// writing — actually arrive. Signals are the fallback, not the opener.
			if (options.gracefulStop) {
				endedBy = "abort";
				options.gracefulStop(reason);
				gracefulTimer = setTimeout(hardStop, ABORT_GRACE_MS);
				return;
			}
			hardStop();
		};

		liveTrees.add(tree);
		// No default deadline: the run lasts until pi finishes unless a caller
		// bound one with timeout_ms or the server was configured with one.
		const timer = timeoutMs === undefined ? undefined : setTimeout(() => stop("timeout"), timeoutMs);
		const unsubscribe = token?.subscribe(() => stop("cancelled")) ?? ((): void => {});

		child.stdin?.on("error", (err: NodeJS.ErrnoException) => {
			// A subprocess can exit between accepting a pipe write and reading it.
			// Its close status, not a broken pipe, is the authoritative outcome.
			if (err.code !== "EPIPE" && err.code !== "ERR_STREAM_DESTROYED") {
				process.stderr.write("pi-mcp: pi stdin stream failed\n");
			}
		});
		child.once("spawn", () => {
			options.onStart?.({
				send: (payload: unknown) => {
					// A closed or dead stdin is not an error here: the run may have
					// settled or been killed between deciding to write and writing.
					if (child.stdin === null || child.stdin.destroyed || child.stdin.writableEnded) return;
					child.stdin.write(`${JSON.stringify(payload)}\n`);
				},
				endInput: () => {
					if (child.stdin === null || child.stdin.writableEnded) return;
					child.stdin.end();
				},
			});
		});

		const consumeLine = (line: string): void => {
			const trimmed = line.trim();
			if (!trimmed || !onEvent) return;
			let event: unknown;
			try {
				event = JSON.parse(trimmed);
			} catch {
				// A nonblank line that is not JSON is a protocol violation, not noise.
				// Count it and fail closed at settle — never normalize the
				// unrecognized into success. The raw capture keeps the line for
				// diagnostics, but its text is never reported to a caller.
				corruptLines += 1;
				return;
			}
			try {
				onEvent(event);
			} catch {
				// An event-handler error is ours, not pi's: it does not say anything
				// about the stream contract, so it must not mark the run corrupt.
			}
		};

		child.stdout?.setEncoding("utf8");
		child.stderr?.setEncoding("utf8");

		child.stdout?.on("data", (chunk: string) => {
			stdout = appendCapped(stdout, chunk);
			if (!onEvent) return;
			pending += chunk;
			let newline = pending.indexOf("\n");
			while (newline !== -1) {
				consumeLine(pending.slice(0, newline));
				pending = pending.slice(newline + 1);
				newline = pending.indexOf("\n");
			}
			// A single unterminated line must not grow without bound; MAX_CAPTURE
			// guards the transcript copy, not the line being assembled here. The
			// drop is recorded, never silent: a line the reader did not consume is
			// a contract violation whether or not it was parseable.
			if (pending.length > MAX_LINE) {
				process.stderr.write(`pi-mcp: dropping an over-long event line (${pending.length} > ${MAX_LINE} chars)\n`);
				droppedLines += 1;
				pending = "";
			}
		});

		child.stderr?.on("data", (chunk: string) => {
			stderr = appendCapped(stderr, chunk);
		});

		const finish = (result: RunResult): void => {
			clearTimeout(timer);
			if (killTimer) clearTimeout(killTimer);
			if (gracefulTimer) clearTimeout(gracefulTimer);
			unsubscribe();
			// pi exiting does not mean its group is empty: anything it left detached
			// keeps running, and nothing else will reap it. A clean exit gets the same
			// sweep as a killed one — on an already-empty group this is a no-op.
			signalTree("SIGTERM");
			// The tree stays tracked until the group is confirmed empty, so a
			// descendant that ignores SIGTERM still receives a later SIGKILL. On a
			// normal exit the first probe confirms it and the tree is dropped at
			// once; a survivor keeps it (and shutdown's sweep) on the books.
			let sweep: NodeJS.Timeout | undefined;
			const sweepOnce = (): void => {
				if (!tree.gone()) return;
				if (sweep) clearInterval(sweep);
				liveTrees.delete(tree);
				options.onTreeGone?.();
			};
			sweepOnce();
			if (liveTrees.has(tree)) {
				sweep = setInterval(sweepOnce, 50);
				sweep.unref();
			}
			// pi may end its last event at EOF instead of a newline; without this the
			// final answer would be missed and the run would look contract-broken.
			// The tail is parsed before the corruption marker is read back: a
			// malformed unterminated line is still part of the stream, and a result
			// snapshotted before this parse would report the run as clean.
			if (pending) {
				consumeLine(pending);
				pending = "";
			}
			const finalResult: RunResult = {
				...result,
				...(corruptLines > 0 ? { corruptLines } : {}),
				...(droppedLines > 0 ? { droppedLines } : {}),
			};
			if (!liveTrees.has(tree)) {
				resolve(finalResult);
				return;
			}
			// The leader is gone but its group is not: a descendant is still
			// alive (a TERM-immune one, by construction — the SIGTERM above is
			// the opener). The run is not over. Publishing the result now would
			// settle the turn and free the session lane while a leftover still
			// runs. Escalate exactly like the abort path — SIGTERM, then
			// SIGKILL after the grace — and resolve only once the group is
			// confirmed empty. If it cannot be verified in time, say so: the
			// result is delivered marked cleanupUnverified and the caller
			// reports an unclean end instead of pretending the run settled.
			let sent = false;
			let deadline = Number.POSITIVE_INFINITY;
			const killedAt = Date.now() + KILL_GRACE_MS;
			const step = (): void => {
				if (tree.gone()) {
					resolve(finalResult);
					return;
				}
				const now = Date.now();
				if (!sent && now >= killedAt) {
					signalTree("SIGKILL");
					sent = true;
					deadline = now + TREE_GONE_MS;
				}
				if (sent && now >= deadline) {
					resolve({ ...finalResult, cleanupUnverified: true });
					return;
				}
				killTimer = setTimeout(step, 50);
				killTimer.unref();
			};
			step();
		};

		let childError: Error | undefined;
		child.on("error", (err: Error) => {
			childError = err;
		});
		// Node emits close after exit or a spawn error. Finalize once, after the
		// output pipes drain, and retain the OS status separately from stderr.
		child.once("close", (code: number | null, signal: NodeJS.Signals | null) => {
			finish({
				code: childError === undefined ? (code ?? -1) : -1,
				stdout,
				stderr: childError === undefined ? stderr : `${stderr}\n${command}: ${childError.message}`.trim(),
				timedOut,
				cancelled,
				...(child.pid === undefined && childError !== undefined
					? { spawnErrorCode: spawnErrorCode(childError) }
					: {}),
				...(child.pid !== undefined && code !== null && code >= 0 ? { exitCode: code } : {}),
				...(signal === null ? {} : { signal }),
				...(endedBy ? { endedBy } : {}),
			});
		});
	});
}
