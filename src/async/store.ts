// A small versioned filesystem store for async executions: one bounded atomic
// snapshot per execution, an ownership lock directory, and retention that prunes
// oldest terminal records under pressure. Local filesystem only, sync FS, no
// runtime dependencies.
//
// Durability protocol per commit: write a private temp file in the same
// directory, fsync it, rename over the target, then fsync the directory before
// the change is visible in memory. A failure at any step leaves the store
// unhealthy and the whole async subsystem fails closed (see scheduler).
//
// Known limit, documented rather than hidden: on a platform whose filesystem
// refuses fsync on a directory fd, the store refuses to open rather than
// pretend renames are durable. (macOS APFS and Linux both support it.) The
// rename itself is atomic either way; what a refused fsync would cost is the
// visibility of the rename across a crash, and that is not ours to wave away.

import {
	chmodSync,
	closeSync,
	existsSync,
	fsyncSync,
	mkdirSync,
	openSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	unlinkSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { isAbsolute, join } from "node:path";
import {
	ASYNC_FINAL_RETENTION_MS,
	ASYNC_MAX_RECORDS,
	ASYNC_MAX_STORE_BYTES,
	ASYNC_PARTIAL_RETENTION_MS,
	ASYNC_TEST_FAIL_POINT,
	ASYNC_TEST_FAIL_STATE,
	ASYNC_TOMBSTONE_RETENTION_MS,
} from "../config.ts";
import { canTransition, type ExecutionRecord, type ExecutionState, isTerminal, readExecutionRecord } from "./types.ts";

export type LookupResult = { kind: "none" } | { kind: "tombstone" } | { kind: "record"; record: ExecutionRecord };

/**
 * Optional observer of durably completed terminal transitions: a nonterminal
 * record that became terminal, with the commit already fsynced, renamed and
 * directory-fsynced, and the memory updated. Called synchronously from
 * commit(); an implementation must not throw, block, or touch store state.
 * Same-state commits (retention expiry on a terminal record) and failed
 * commits never reach it — neither is a qualifying transition.
 */
export type TerminalTransitionObserver = (executionId: string, state: ExecutionState) => void;

export class StoreUnavailableError extends Error {}

function warn(message: string): void {
	process.stderr.write(`pi-mcp: ${message}\n`);
}

/** Record size as persisted, matching what is on disk: UTF-8 bytes, not
 * code units — multibyte content costs what it actually costs. */
function recordBytes(record: ExecutionRecord): number {
	return Buffer.byteLength(JSON.stringify(record), "utf8");
}

export class ExecutionStore {
	private readonly dir: string;
	private readonly execDir: string;
	private readonly lockDir: string;
	private readonly onTerminal: TerminalTransitionObserver | undefined;
	private readonly records = new Map<string, ExecutionRecord>();
	private readonly keyIndex = new Map<string, string>();
	private bytes = 0;
	private healthy_ = false;
	private lockOwned = false;

	get healthy(): boolean {
		return this.healthy_;
	}

	private constructor(dir: string, onTerminal: TerminalTransitionObserver | undefined) {
		this.dir = dir;
		this.execDir = join(dir, "executions");
		this.lockDir = join(dir, "lock");
		this.onTerminal = onTerminal;
	}

	get recordCount(): number {
		return this.records.size;
	}

	get totalBytes(): number {
		return this.bytes;
	}

	get ownsLock(): boolean {
		return this.lockOwned;
	}

	// --- open ---------------------------------------------------------------

	static open(dir: string, onTerminal?: TerminalTransitionObserver): ExecutionStore {
		const store = new ExecutionStore(dir, onTerminal);
		store.open();
		return store;
	}

	private open(): void {
		try {
			if (!isAbsolute(this.dir)) {
				warn(`async state directory must be an absolute path: ${this.dir}`);
				return;
			}
			mkdirSync(this.dir, { recursive: true, mode: 0o700 });
			const stat = statSync(this.dir);
			// Fail closed on unsafe permissions: the store carries answers and
			// routing metadata, and a world-readable directory is not what the
			// operator asked for. Attempt one repair, then refuse.
			if ((stat.mode & 0o777) !== 0o700) {
				try {
					chmodSync(this.dir, 0o700);
				} catch {
					/* fall through to the check below */
				}
				if ((statSync(this.dir).mode & 0o777) !== 0o700) {
					warn(`async state directory has unsafe permissions; refusing: ${this.dir}`);
					return;
				}
			}
			if (typeof process.getuid === "function" && stat.uid !== process.getuid()) {
				warn(`async state directory is owned by uid ${stat.uid}, not this process; refusing: ${this.dir}`);
				return;
			}

			// Exclusive ownership: an atomic lock directory. No automatic
			// PID-based stealing — a stale lock requires the documented operator
			// check that the previous owner and its pi children are gone.
			try {
				mkdirSync(this.lockDir);
			} catch (err) {
				const code = (err as NodeJS.ErrnoException).code;
				if (code === "EEXIST") {
					warn(
						`async state directory is locked by another owner: ${this.lockDir}\n` +
							"  If you are sure that owner and its pi children are gone, remove the lock\n" +
							"  directory by hand and restart. The store will not be opened twice.",
					);
					return;
				}
				throw err;
			}
			this.lockOwned = true;
			writeFileSync(join(this.lockDir, "owner"), `pid ${process.pid} started ${new Date().toISOString()}\n`, {
				mode: 0o600,
			});

			mkdirSync(this.execDir, { recursive: true, mode: 0o700 });
			// Probe the durability protocol up front: a filesystem that refuses
			// directory fsync would fail the first commit anyway, and refusing to
			// open says that at startup instead of mid-flight.
			this.fsyncDir(this.execDir);

			// Load every record; reconstruct the key index from the records
			// themselves instead of a separately committed index. Malformed
			// records or an unknown version fail the store closed: guessing at a
			// half-written history would be worse than refusing.
			for (const name of readdirSync(this.execDir)) {
				if (!name.endsWith(".json")) continue;
				const file = join(this.execDir, name);
				const fstat = statSync(file);
				if ((fstat.mode & 0o777) !== 0o600) {
					warn(`execution record has unsafe permissions; refusing to load: ${file}`);
					this.releaseLock();
					return;
				}
				let parsed: unknown;
				try {
					parsed = JSON.parse(readFileSync(file, "utf8"));
				} catch {
					warn(`execution record is not valid JSON; refusing to load: ${file}`);
					this.releaseLock();
					return;
				}
				const record = readExecutionRecord(parsed);
				if (record === null) {
					warn(`execution record is malformed or has an unknown version; refusing to load: ${file}`);
					this.releaseLock();
					return;
				}
				this.records.set(record.execution_id, record);
				this.bytes += recordBytes(record);
				if (record.idempotency_key !== null) this.keyIndex.set(record.idempotency_key, record.execution_id);
			}
			this.healthy_ = true;
		} catch (err) {
			warn(`async store could not be opened: ${(err as Error).message}`);
			this.releaseLock();
		}
	}

	/** Drop the ownership lock. Only the process that created it may do this. */
	releaseLock(): void {
		if (!this.lockOwned) return;
		this.lockOwned = false;
		try {
			rmSync(this.lockDir, { recursive: true, force: true });
		} catch {
			// Leaving a stale lock behind is the failure the next startup
			// reports; do not pretend it was released.
		}
	}

	// --- commits --------------------------------------------------------------

	/**
	 * Persist one record atomically. Same-state commits are allowed (payload
	 * changes: progress flushes, retention expiry); a state change must follow
	 * the legal transition table. Throws (and marks the store unhealthy) on any
	 * write/sync failure — callers fail closed.
	 */
	commit(next: ExecutionRecord): void {
		if (!this.healthy_) {
			throw new StoreUnavailableError("the execution store is unavailable");
		}
		const prev = this.records.get(next.execution_id);
		if (prev !== undefined) {
			if (prev.state !== next.state && !canTransition(prev.state, next.state)) {
				throw new Error(`illegal transition ${prev.state} -> ${next.state} for ${next.execution_id}`);
			}
		} else if (next.state !== "queued") {
			throw new Error(`first commit for ${next.execution_id} must be queued, got ${next.state}`);
		}

		const file = join(this.execDir, `${next.execution_id}.json`);
		const tmp = `${file}.${process.pid}.${Math.floor(Math.random() * 1e9)}.tmp`;
		const payload = JSON.stringify(next);

		// Test seam (see config): inject one commit failure at the chosen point.
		// Gating on the point keeps both reachable: "before_rename" fails with
		// nothing durable, "after_rename" fails with the record already renamed —
		// the ambiguous-acceptance case recovery must survive.
		const inject = ASYNC_TEST_FAIL_STATE === next.state;
		try {
			const fd = openSync(tmp, "w", 0o600);
			try {
				writeSync(fd, payload);
				fsyncSync(fd);
			} finally {
				closeSync(fd);
			}
			if (inject && ASYNC_TEST_FAIL_POINT === "before_rename") {
				unlinkSync(tmp);
				throw new Error(`injected failure (before rename) committing ${next.state}`);
			}
			renameSync(tmp, file);
			if (inject && ASYNC_TEST_FAIL_POINT === "after_rename") {
				throw new Error(`injected failure (after rename) committing ${next.state}`);
			}
			this.fsyncDir(this.execDir);
		} catch (err) {
			this.healthy_ = false;
			if (existsSync(tmp)) {
				try {
					unlinkSync(tmp);
				} catch {
					/* the failure below is the real one */
				}
			}
			throw new StoreUnavailableError(`execution store commit failed: ${(err as Error).message}`);
		}

		if (prev !== undefined) this.bytes -= recordBytes(prev);
		this.bytes += Buffer.byteLength(payload, "utf8");
		this.records.set(next.execution_id, next);
		if (next.idempotency_key !== null) this.keyIndex.set(next.idempotency_key, next.execution_id);

		// The completed-commit boundary: the transition above is durable and
		// visible, everything after this point is bookkeeping. The observer is
		// fire-and-forget by contract; guarding here anyway keeps an observer
		// bug from turning a durable commit into a reported failure.
		if (this.onTerminal !== undefined && prev !== undefined && !isTerminal(prev.state) && isTerminal(next.state)) {
			try {
				this.onTerminal(next.execution_id, next.state);
			} catch (err) {
				warn(`terminal-transition observer threw (ignored): ${(err as Error).message}`);
			}
		}
	}

	private fsyncDir(dir: string): void {
		let fd: number | undefined;
		try {
			fd = openSync(dir, "r");
			fsyncSync(fd);
		} catch (err) {
			// A failed directory fsync is a durability failure like any other:
			// the rename above may not be visible after a crash. Reporting success
			// here would be the one lie this store must never tell, so it fails
			// closed through the same path as every other write failure.
			throw new StoreUnavailableError(`directory fsync failed: ${(err as Error).message}`);
		} finally {
			if (fd !== undefined) {
				try {
					closeSync(fd);
				} catch {
					/* nothing more to do */
				}
			}
		}
	}

	private deleteRecord(id: string): void {
		const prev = this.records.get(id);
		if (prev === undefined) return;
		try {
			unlinkSync(join(this.execDir, `${id}.json`));
		} catch (err) {
			this.healthy_ = false;
			throw new StoreUnavailableError(`execution store delete failed: ${(err as Error).message}`);
		}
		this.bytes -= recordBytes(prev);
		this.records.delete(id);
		if (prev.idempotency_key !== null && this.keyIndex.get(prev.idempotency_key) === id) {
			this.keyIndex.delete(prev.idempotency_key);
		}
	}

	// --- reads ------------------------------------------------------------------

	/** The stored record with no expiry applied; for recovery paths only. */
	getRaw(id: string): ExecutionRecord | undefined {
		return this.records.get(id);
	}

	allRecords(): ExecutionRecord[] {
		return [...this.records.values()];
	}

	/**
	 * Look up an execution with retention applied lazily: events drop after
	 * partial retention, finals/keys expire into a tombstone, tombstones are
	 * deleted after their window. Nonterminal records are never expired.
	 */
	lookup(id: string, now = Date.now()): LookupResult {
		const record = this.records.get(id);
		if (record === undefined) return { kind: "none" };
		const result = this.applyExpiry(record, now);
		if (result === "deleted") return { kind: "none" };
		if (result === "tombstone") return { kind: "tombstone" };
		return { kind: "record", record: result };
	}

	/** Same lookup, keyed by idempotency key. */
	lookupKey(key: string, now = Date.now()): LookupResult {
		const id = this.keyIndex.get(key);
		if (id === undefined) return { kind: "none" };
		return this.lookup(id, now);
	}

	private applyExpiry(record: ExecutionRecord, now: number): ExecutionRecord | "tombstone" | "deleted" {
		if (!isTerminal(record.state)) return record;
		const finished = record.finished_at ?? record.accepted_at;

		// Partial history expires first; detectable via the retained floor.
		if (record.events.length > 0 && now > finished + ASYNC_PARTIAL_RETENTION_MS) {
			const next: ExecutionRecord = {
				...record,
				events: [],
				oldest_retained_seq: record.latest_seq + 1,
			};
			try {
				this.commit(next);
				record = next;
			} catch {
				// Unhealthy store: serve the record as-is rather than guess.
			}
		}

		if (record.finals_expired === true) {
			if (now > finished + ASYNC_FINAL_RETENTION_MS + ASYNC_TOMBSTONE_RETENTION_MS) {
				try {
					this.deleteRecord(record.execution_id);
					return "deleted";
				} catch {
					return "tombstone";
				}
			}
			return "tombstone";
		}

		if (now > finished + ASYNC_FINAL_RETENTION_MS) {
			const tomb: ExecutionRecord = {
				...record,
				events: [],
				oldest_retained_seq: record.latest_seq + 1,
				final: null,
				finals_expired: true,
			};
			try {
				this.commit(tomb);
			} catch {
				// Serve the un-expired record; expiry retries on the next lookup.
			}
			return "tombstone";
		}

		return record;
	}

	/**
	 * Retention sweep plus global bounds. Deterministic: oldest terminal
	 * records (by finished_at) are pruned first; nonterminal records are never
	 * evicted. A store over the bounds with only active records is the
	 * admission path's problem, not a reason to drop live work.
	 */
	sweep(now = Date.now()): void {
		for (const record of this.allRecords()) {
			this.applyExpiry(record, now);
			if (!this.healthy_) return;
		}

		const terminal = this.allRecords()
			.filter((record) => isTerminal(record.state))
			.sort((a, b) => (a.finished_at ?? a.accepted_at) - (b.finished_at ?? b.accepted_at));
		let count = this.recordCount;
		let bytes = this.bytes;
		for (const record of terminal) {
			if (count <= ASYNC_MAX_RECORDS && bytes <= ASYNC_MAX_STORE_BYTES) break;
			try {
				this.deleteRecord(record.execution_id);
			} catch {
				return;
			}
			count -= 1;
			bytes -= recordBytes(record);
		}
	}
}
