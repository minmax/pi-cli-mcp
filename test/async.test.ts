// Phase 1 async contract tests. Everything here drives the real stdio server
// against the fake pi binary — no in-process harness, no fakes of our own code.
//
// The flows that matter most get their own tests: cancellation before spawn,
// store failure, idempotent retries before the session is known to the adapter,
// cursor expiry, oversized projections, and recovery of a lost supervisor.

import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, test } from "vitest";
import { Client, makeFakeBin, makeWorkspace, sleep, type Workspace, waitFor } from "./helpers/client.ts";
import { Gateway } from "./helpers/gateway.ts";

const LEGACY_TOOLS = ["pi", "pi_reply", "pi_models", "pi_send", "pi_running", "pi_sessions"];
const ASYNC_TOOL_NAMES = ["pi_start", "pi_status", "pi_wait", "pi_result", "pi_cancel"];

interface AsyncWorkspace extends Workspace {
	stateDir: string;
	asyncEnv: Record<string, string>;
}

/** A workspace with the async opt-in pointing at a fresh absolute state dir. */
function makeAsyncWorkspace(): AsyncWorkspace {
	const ws = makeWorkspace();
	const stateDir = join(ws.dir, "async-state");
	return { ...ws, stateDir, asyncEnv: { ...ws.env, PI_MCP_ASYNC_STATE_DIR: stateDir } };
}

/** The file the fake appends one line to per pi invocation. */
/** Sum of every file's bytes under a directory — what the store actually
 * spends on disk, transient tmp writes included while they live. */
function storeBytesOnDisk(dir: string): number {
	let total = 0;
	for (const entry of readdirSync(dir)) {
		const full = join(dir, entry);
		const st = statSync(full);
		total += st.isDirectory() ? storeBytesOnDisk(full) : st.size;
	}
	return total;
}

function startedFile(ws: Workspace): string {
	return join(ws.dir, "started.log");
}

function startedCount(ws: Workspace): number {
	if (!existsSync(startedFile(ws))) return 0;
	return readFileSync(startedFile(ws), "utf8").trim().split("\n").filter(Boolean).length;
}

/** A per-execution exclusive cursor, built exactly as the server builds it. */
function cursor(executionId: string, seq: number): string {
	return Buffer.from(`v1:${executionId}:${seq}`, "utf8").toString("base64url");
}

function expectErrorCode(res: { text: string; isError: boolean; structured: any }, code: string): void {
	expect(res.isError).toBe(true);
	expect(res.structured?.error?.code).toBe(code);
}

/** Tagged grandchild detector, so reaping can be asserted per test file. */
const CHILD_TAG = "pi-cli-mcp-async-child";

function survivors(): string {
	return spawnSync("pgrep", ["-f", CHILD_TAG], { encoding: "utf8" }).stdout.trim();
}

/** A pid probe the way the server does it: ESRCH is the only "gone". */
function pidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code !== "ESRCH";
	}
}

// --- completion hook helpers ---------------------------------------------

/** Tag for a hook-owned descendant, so hook reaping is asserted per test file
 * without seeing other files' children (the fake-pi tag is a different one). */
const HOOK_TAG = "pi-cli-mcp-async-hook-child";

function hookSurvivors(): string {
	return spawnSync("pgrep", ["-f", HOOK_TAG], { encoding: "utf8" }).stdout.trim();
}

/** A disposable operator-owned hook executable, the way a real one looks:
 * spawned directly, absolute path, its own PATH because its environment is
 * empty. */
function makeHook(dir: string, body: string): string {
	const hook = join(dir, "notify-hook.sh");
	writeFileSync(hook, `#!/bin/sh\nexport PATH="/usr/bin:/bin:/usr/local/bin"\n${body}\n`, { mode: 0o755 });
	chmodSync(hook, 0o755);
	return hook;
}

function hookLines(file: string): string[] {
	if (!existsSync(file)) return [];
	return readFileSync(file, "utf8").trim().split("\n").filter(Boolean);
}

/** The hanging hook logs `<pid> <exec> <state>` on entry; this is its pid. */
function hookPidOf(spawnLog: string): number {
	const first = hookLines(spawnLog).at(0);
	expect(first).toBeDefined();
	const pid = Number(first?.split(" ")[0]);
	expect(pid).toBeGreaterThan(0);
	return pid;
}

/** A hook body that logs exactly when the terminal record is already durable
 * on disk — the contract the server must hold at invocation time: the commit
 * (fsync, rename, directory fsync) is finished before the hook is spawned. */
function durableRecordHook(stateDir: string, log: string): string {
	return `
rec="${stateDir}/executions/$1.json"
if [ -f "$rec" ] && grep -q "\\"state\\":\\"$2\\"" "$rec"; then
	echo "$1 $2" >> "${log}"
fi
`;
}

/** A hook body that hangs past any test window, logging its leader pid, and
 * backgrounding a TERM-immune tagged descendant — the Apprise-shaped case the
 * timeout escalation and the shutdown reap must take out with the group. */
function hangingHook(log: string): string {
	return `
echo "$$ $1 $2" >> "${log}"
( trap '' TERM; exec sh -c "sleep 600; true # ${HOOK_TAG}" ) &
wait
`;
}

describe("async tools", () => {
	let cleanup: (() => void)[] = [];

	beforeEach(() => {
		cleanup = [];
	});
	afterEach(() => {
		for (const fn of cleanup) fn();
	});
	afterAll(() => {
		for (const fn of cleanup) fn();
	});

	function track<T extends { cleanup: () => void }>(ws: T): T {
		cleanup.push(ws.cleanup);
		return ws;
	}

	test("tools/list is exactly the six legacy tools when async is not opted in", async () => {
		const ws = track(makeWorkspace());
		const client = new Client(ws.env);
		cleanup.push(() => client.close());
		const res = await client.call("tools/list");
		expect(res.result.tools.map((t: { name: string }) => t.name)).toEqual(LEGACY_TOOLS);
	});

	test("tools/list appends the five async tools in order when opted in", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client(ws.asyncEnv);
		cleanup.push(() => client.close());
		const res = await client.call("tools/list");
		expect(res.result.tools.map((t: { name: string }) => t.name)).toEqual([...LEGACY_TOOLS, ...ASYNC_TOOL_NAMES]);
		expect(readFileSync(join(ws.stateDir, "lock", "owner"), "utf8")).toMatch(new RegExp(`^pid ${client.child.pid} `));
	});

	test("pi_start returns before the turn settles and the final is recoverable", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_RPC_SETTLE_MS: "3000",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const started = Date.now();
		const res = await client.tool("pi_start", { prompt: "slow answer" });
		expect(Date.now() - started).toBeLessThan(2000);
		expect(res.isError).toBe(false);
		const { execution_id: exec, session_id: session } = res.structured;
		expect(exec).toMatch(/^[0-9a-f-]{36}$/);
		expect(session).toMatch(/^[0-9a-f-]{36}$/);
		expect(res.structured.state).toBe("queued");

		// The execution runs server-side with no client attached.
		await waitFor("succeeded", async () => {
			const status = await client.tool("pi_status", { execution_id: exec });
			return status.structured?.state === "succeeded";
		});

		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.isError).toBe(false);
		expect(result.structured.final.answer.text).toBe("RPC ANSWER");
		expect(result.structured.final.outcome).toBe("succeeded");
		expect(result.structured.state).toBe("succeeded");
		expect(startedCount(ws)).toBe(1);
	});

	test.each([
		{
			label: "a missing executable",
			mode: "missing",
			reason: "could not start pi (spawn error ENOENT)",
			diagnostics: { spawn_error_code: "ENOENT", rpc_prompt_acknowledged: false },
		},
		{
			label: "a non-executable file",
			mode: "not_executable",
			reason: "could not start pi (spawn error EACCES)",
			diagnostics: { spawn_error_code: "EACCES", rpc_prompt_acknowledged: false },
		},
		{
			label: "a timeout before prompt acknowledgement",
			mode: "timeout_before_ack",
			reason: "timed out after 2000 ms",
			diagnostics: { signal: "SIGTERM", rpc_prompt_acknowledged: false },
		},
		{
			label: "id-less and foreign prompt responses as unacknowledged",
			mode: "wrong_prompt_ack",
			reason:
				"pi exited cleanly without acknowledging the RPC prompt or settling the turn (no agent_settled observed)",
			diagnostics: { exit_code: 0, rpc_prompt_acknowledged: false },
		},
		{
			label: "a launcher exiting before RPC acknowledgement",
			mode: "startup_exit",
			reason: "pi terminated before acknowledging the RPC prompt (exit code 127)",
			diagnostics: { exit_code: 127, rpc_prompt_acknowledged: false },
		},
		{
			label: "a clean exit without settlement",
			mode: "exit_unsettled",
			reason: "pi exited cleanly without settling the turn (no agent_settled observed)",
			diagnostics: { exit_code: 0, rpc_prompt_acknowledged: true },
		},
		{
			label: "a signal after RPC acknowledgement",
			mode: "signal_unsettled",
			reason: "pi terminated by SIGTERM",
			diagnostics: { signal: "SIGTERM", rpc_prompt_acknowledged: true },
		},
	])("reports $label without retrying or leaking stderr", async ({ mode, reason, diagnostics }) => {
		const ws = track(makeAsyncWorkspace());
		const cannotSpawn = mode === "missing" || mode === "not_executable";
		const bin = cannotSpawn ? join(ws.dir, mode) : ws.bin;
		if (mode === "not_executable") writeFileSync(bin, "#!/bin/sh\nexit 0\n", { mode: 0o600 });
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_BIN: bin,
			PI_MCP_ABORT_GRACE_MS: "0",
			FAKE_MODE: mode,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", {
			prompt: "private startup test prompt",
			...(mode === "timeout_before_ack" ? { timeout_ms: 2000 } : {}),
		});
		const args = { execution_id: start.structured.execution_id };
		await waitFor("failed", async () => (await client.tool("pi_status", args)).structured.state === "failed");
		const result = await client.tool("pi_result", args);
		expect(result.structured.state).toBe("failed");
		expect(result.structured.final).toMatchObject({ outcome: "failed", failure_reason: reason, retries: 0 });
		expect(result.structured.final.process_diagnostics).toEqual(diagnostics);
		expect(result.structured.final.answer).toBeNull();
		expect(result.text).not.toContain("fake private startup stderr");
		expect(result.text).not.toContain("private startup test prompt");
		expect(startedCount(ws)).toBe(cannotSpawn ? 0 : 1);
	});

	test("a malformed line mid-stream fails closed even though the turn settles", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_GARBAGE_LINE: "1",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "corrupt stream test prompt" });
		const args = { execution_id: start.structured.execution_id };
		await waitFor("failed", async () => (await client.tool("pi_status", args)).structured.state === "failed");
		const result = await client.tool("pi_result", args);
		expect(result.structured.state).toBe("failed");
		expect(result.structured.final).toMatchObject({
			outcome: "failed",
			failure_reason: "pi emitted 1 malformed line on its protocol stdout; the event stream contract was violated",
			retries: 0,
		});
		// The corrupt line itself is never echoed, in the final or the projection.
		expect(JSON.stringify(result.structured)).not.toContain("this line is not json");
		expect(result.text).not.toContain("this line is not json");
		expect(startedCount(ws)).toBe(1);
	});

	test("a malformed line at EOF, unterminated by any newline, fails closed too", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			// The corruption is the last stdout content with no trailing newline:
			// only a reader that parses the EOF tail before snapshotting its
			// diagnostics can still count it.
			FAKE_GARBAGE_EOF: "1",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "corrupt eof tail test prompt" });
		const args = { execution_id: start.structured.execution_id };
		await waitFor("failed", async () => (await client.tool("pi_status", args)).structured.state === "failed");
		const result = await client.tool("pi_result", args);
		expect(result.structured.state).toBe("failed");
		expect(result.structured.final).toMatchObject({
			outcome: "failed",
			failure_reason: "pi emitted 1 malformed line on its protocol stdout; the event stream contract was violated",
			retries: 0,
		});
		expect(JSON.stringify(result.structured)).not.toContain("this line is not json");
		expect(result.text).not.toContain("this line is not json");
		expect(startedCount(ws)).toBe(1);
	});

	test("an over-long tail dropped unread fails the run too", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			// The tail exceeds the reader's capture limit: it is dropped before
			// parsing, and a line the reader never saw must still fail the run.
			PI_MCP_MAX_LINE: "100000",
			FAKE_GARBAGE_EOF: "1",
			FAKE_GARBAGE_EOF_SIZE: "110000",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "over-long eof tail test prompt" });
		const args = { execution_id: start.structured.execution_id };
		await waitFor("failed", async () => (await client.tool("pi_status", args)).structured.state === "failed");
		const result = await client.tool("pi_result", args);
		expect(result.structured.state).toBe("failed");
		expect(result.structured.final).toMatchObject({
			outcome: "failed",
			failure_reason:
				"pi dropped 1 line longer than the capture limit without reading it; the event stream contract was violated",
			retries: 0,
		});
		// The dropped content is never echoed, in the final or the projection.
		expect(JSON.stringify(result.structured)).not.toContain("xxxx");
		expect(result.text).not.toContain("xxxx");
		expect(startedCount(ws)).toBe(1);
	});

	test("a complete over-long line is dropped before parsing, and the lines after it still project", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			// A newline-terminated line over the capture limit: the drop happens
			// before the parser sees it, and parsing resumes at the next line.
			PI_MCP_MAX_LINE: "100000",
			FAKE_GARBAGE_COMPLETE: "1",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "complete over-long line test prompt" });
		const args = { execution_id: start.structured.execution_id };
		await waitFor("failed", async () => (await client.tool("pi_status", args)).structured.state === "failed");
		const result = await client.tool("pi_result", args);
		expect(result.structured.state).toBe("failed");
		expect(result.structured.final).toMatchObject({
			outcome: "failed",
			failure_reason:
				"pi dropped 1 line longer than the capture limit without reading it; the event stream contract was violated",
			retries: 0,
		});
		expect(JSON.stringify(result.structured)).not.toContain("malformed line");
		expect(JSON.stringify(result.structured)).not.toContain("xxxx");
		// The valid events after the dropped line still projected.
		expect(JSON.stringify(result.structured)).toContain("FINAL ANSWER");
		expect(startedCount(ws)).toBe(1);
	});

	test("a malformed persisted final is refused on restart, not served by pi_result", async () => {
		const ws = track(makeAsyncWorkspace());
		// Supervisor A: complete a job so a terminal final lands on disk.
		const a = new Client({ ...ws.asyncEnv, FAKE_STARTED_FILE: startedFile(ws) });
		cleanup.push(() => a.close());
		const start = await a.tool("pi_start", { prompt: "final validation test" });
		const exec: string = start.structured.execution_id;
		await waitFor(
			"succeeded",
			async () => (await a.tool("pi_status", { execution_id: exec })).structured.state === "succeeded",
		);
		a.child.kill("SIGTERM");
		await new Promise<void>((resolve) => a.child.on("exit", () => resolve()));

		// A clean restart still serves the valid record, final and all.
		const b = new Client(ws.asyncEnv);
		cleanup.push(() => b.close());
		const good = await b.tool("pi_result", { execution_id: exec });
		expect(good.structured.state).toBe("succeeded");
		expect(good.structured.final).toMatchObject({ outcome: "succeeded", answer: { text: "FINAL ANSWER" } });
		b.child.kill("SIGTERM");
		await new Promise<void>((resolve) => b.child.on("exit", () => resolve()));

		// Corrupt the final on disk: a numeric answer text and a missing usage block
		// — shapes the writer never emits.
		const file = join(ws.stateDir, "executions", `${exec}.json`);
		const record = JSON.parse(readFileSync(file, "utf8"));
		record.final.answer = 123;
		delete record.final.usage;
		writeFileSync(file, JSON.stringify(record));

		// The store refuses to open on the tampered shape, and the refusal is
		// typed — never a silently served garbage final. Valid records elsewhere
		// in the suite prove the reload path itself stays honest.
		const c = new Client(ws.asyncEnv);
		cleanup.push(() => c.close());
		expectErrorCode(await c.tool("pi_result", { execution_id: exec }), "store_unavailable");
		expectErrorCode(await c.tool("pi_status", { execution_id: exec }), "store_unavailable");
		expectErrorCode(await c.tool("pi_start", { prompt: "no new work on a poisoned store" }), "store_unavailable");
		// The synchronous tools are unaffected by an unusable async store.
		const sync = await c.tool("pi", { prompt: "still works", cwd: ws.dir });
		expect(sync.isError).toBe(false);
	});

	test("projections are safe, snapshots are clipped, cursors page without duplicates", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "snapshot_delay",
			FAKE_DELAY_MS: "2500",
			FAKE_SNAPSHOT_CHARS: "100000",
			PI_MCP_ASYNC_EVENT_TEXT_LIMIT: "2000",
			PI_MCP_ASYNC_FLUSH_MS: "150",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "watch me" });
		const exec: string = start.structured.execution_id;
		expect(start.structured.state).toBe("queued");

		// Poll while running: progress notes and a clipped snapshot must appear
		// before the final, and nothing unsafe may leak into the projection.
		let sawSnapshot = false;
		await waitFor("a snapshot projection while running", async () => {
			const res = await client.tool("pi_result", { execution_id: exec });
			for (const event of res.structured?.events ?? []) {
				expect(event.text).not.toContain("SECRET NARRATION");
				expect(event.text).not.toContain("a.txt");
				if (event.type === "snapshot") {
					sawSnapshot = true;
					expect(event.text.startsWith("PARTIAL SNAPSHOT BEFORE SETTLE")).toBe(true);
					expect(event.text.length).toBe(2000);
					expect(event.truncated).toBe(true);
				}
			}
			return sawSnapshot;
		});

		// The final supersedes the snapshot.
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		const final = await client.tool("pi_result", { execution_id: exec });
		expect(final.structured.final.answer.text).toBe("FINAL AFTER SNAPSHOT");

		// Page the whole history with limit=1: strictly increasing sequences,
		// no duplicates, has_more until drained, and repeating a cursor
		// returns the same page (the client deduplicates on seq).
		const seen: number[] = [];
		let after: string | undefined;
		for (;;) {
			const page = await client.tool("pi_result", {
				execution_id: exec,
				limit: 1,
				...(after !== undefined ? { after_cursor: after } : {}),
			});
			const events = page.structured?.events ?? [];
			expect(events.length).toBeLessThanOrEqual(1);
			for (const event of events) expect(event.seq).toBeGreaterThan(seen.at(-1) ?? 0);
			seen.push(...events.map((e: { seq: number }) => e.seq));

			const repeat = await client.tool("pi_result", {
				execution_id: exec,
				limit: 1,
				...(after !== undefined ? { after_cursor: after } : {}),
			});
			expect((repeat.structured?.events ?? []).map((e: { seq: number }) => e.seq)).toEqual(
				events.map((e: { seq: number }) => e.seq),
			);

			if (page.structured?.has_more !== true) break;
			after = page.structured.next_cursor;
		}
		expect(sawSnapshot).toBe(true);
		// Every projection event the run produced was paged out once.
		const whole = await client.tool("pi_result", { execution_id: exec });
		expect(seen.length).toBe(whole.structured.events.length);
	});

	test("a cursor past retention is reported, never silently dropped", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "snapshot_delay",
			FAKE_DELAY_MS: "500",
			PI_MCP_ASYNC_MAX_EVENTS: "2", // keeps only the newest events
			PI_MCP_ASYNC_FLUSH_MS: "150",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "expire my cursor" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		const whole = await client.tool("pi_result", { execution_id: exec });
		const floor: number = whole.structured.oldest_retained_seq;
		// The run produced more events than retention kept.
		expect(floor).toBeGreaterThan(1);

		const res = await client.tool("pi_result", { execution_id: exec, after_cursor: cursor(exec, 1) });
		expect(res.isError).toBe(false);
		expect(res.structured.cursor_expired).toBe(true);
		expect(res.structured.events.every((e: { seq: number }) => e.seq >= floor)).toBe(true);
		// The final is still there without replaying anything.
		expect(res.structured.final.answer.text).toBe("FINAL AFTER SNAPSHOT");
	});

	test("pi_wait returns the pi_status shape as soon as the execution is terminal", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, FAKE_RPC_SETTLE_MS: "60000", FAKE_STARTED_FILE: startedFile(ws) });
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "wait for me" });
		const exec: string = start.structured.execution_id;
		await waitFor("fake Pi to start", () => startedCount(ws) === 1);

		const began = performance.now();
		const pending = client.request("tools/call", {
			name: "pi_wait",
			arguments: { execution_id: exec, timeout_ms: 20000 },
		});
		let returned = false;
		void pending.promise.then(() => {
			returned = true;
		});
		await client.call("ping");
		expect(returned).toBe(false);
		const steer = await client.tool("pi_send", {
			session: start.structured.session_id,
			command: "steer",
			message: "finish",
		});
		expect(steer.isError).toBe(false);
		const { result: waited } = await pending.promise;
		expect(performance.now() - began).toBeLessThan(15000);
		expect(waited.isError).not.toBe(true);
		expect(waited.structuredContent.state).toBe("succeeded");

		// Same shape and content as pi_status once terminal.
		const status = await client.tool("pi_status", { execution_id: exec });
		expect(waited.structuredContent).toEqual(status.structured);
		expect(JSON.parse(waited.content[0].text)).toEqual(status.structured);

		// Already terminal: immediate, whatever the timeout.
		const again = Date.now();
		const second = await client.tool("pi_wait", { execution_id: exec, timeout_ms: 20000 });
		expect(Date.now() - again).toBeLessThan(5000);
		expect(second.structured).toEqual(status.structured);
	});

	test("pi_wait times out with the latest nonterminal status and leaves the execution alone", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, FAKE_MODE: "hang" });
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "never settles" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});

		const began = Date.now();
		const waited = await client.tool("pi_wait", { execution_id: exec, timeout_ms: 600 });
		const elapsed = Date.now() - began;
		expect(waited.isError).toBe(false);
		expect(waited.structured.state).toBe("running");
		expect(waited.structured.finished_at).toBeNull();
		expect(elapsed).toBeGreaterThanOrEqual(500);
		expect(Object.keys(waited.structured).sort()).toEqual(
			Object.keys((await client.tool("pi_status", { execution_id: exec })).structured).sort(),
		);

		// The timeout did not touch the run.
		const after = await client.tool("pi_status", { execution_id: exec });
		expect(after.structured.state).toBe("running");

		// A zero timeout is a plain status read.
		const zeroStarted = performance.now();
		const zero = await client.tool("pi_wait", { execution_id: exec, timeout_ms: 0 });
		expect(performance.now() - zeroStarted).toBeLessThan(4000);
		expect(zero.structured).toEqual(after.structured);

		await client.tool("pi_cancel", { execution_id: exec });
	});

	test("cancelling pi_wait releases the request and the execution can still succeed", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, FAKE_RPC_SETTLE_MS: "60000", FAKE_STARTED_FILE: startedFile(ws) });
		cleanup.push(() => client.close());
		const start = await client.tool("pi_start", { prompt: "keep running after the wait" });
		const args = { execution_id: start.structured.execution_id };
		await waitFor("fake Pi to start", () => startedCount(ws) === 1);

		const pending = client.request("tools/call", { name: "pi_wait", arguments: { ...args, timeout_ms: 20000 } });
		let returned = false;
		void pending.promise.then(() => {
			returned = true;
		});
		await client.call("ping");
		expect(returned).toBe(false);
		const began = performance.now();
		client.send({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: pending.id } });
		const { result } = await pending.promise;
		expect(performance.now() - began).toBeLessThan(10000);
		expect(result.structuredContent.state).toBe("running");
		expect((await client.tool("pi_status", args)).structured.state).toBe("running");

		const steer = await client.tool("pi_send", {
			session: start.structured.session_id,
			command: "steer",
			message: "finish",
		});
		expect(steer.isError).toBe(false);
		const waited = await client.tool("pi_wait", args);
		expect(waited.structured.state).toBe("succeeded");
		expect(startedCount(ws)).toBe(1);
	});

	test("pi_wait rejects out-of-bounds input and unknown executions with typed errors", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client(ws.asyncEnv);
		cleanup.push(() => client.close());

		const missing = "00000000-0000-4000-8000-000000000000";
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, timeout_ms: 20001 }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, timeout_ms: -1 }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, timeout_ms: 1.5 }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, timeout_ms: "5" }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, timeout_ms: null }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: missing, extra: 1 }), "invalid_input");
		expectErrorCode(await client.tool("pi_wait", { execution_id: "nope" }), "invalid_input");
		const unknown = await client.tool("pi_wait", { execution_id: missing });
		expectErrorCode(unknown, "unknown_execution");
		expect(unknown.structured).toEqual((await client.tool("pi_status", { execution_id: missing })).structured);

		const list = await client.call("tools/list");
		const tool = list.result.tools.find((t: { name: string }) => t.name === "pi_wait");
		expect(tool.inputSchema.properties.timeout_ms).toMatchObject({ minimum: 0, maximum: 20000 });
		expect(tool.annotations).toMatchObject({ readOnlyHint: true, idempotentHint: true, openWorldHint: false });
	});

	test("pi_cancel withdraws a queued execution before spawn", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "hang",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		// One execution holds the session lane forever.
		const first = await client.tool("pi_start", { prompt: "hold the lane" });
		const session: string = first.structured.session_id;
		expect(first.structured.state).toBe("queued");
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: first.structured.execution_id });
			return res.structured?.state === "running";
		});
		await waitFor("the lane holder to spawn", () => startedCount(ws) === 1);

		// A second turn on the same session can only be queued.
		const second = await client.tool("pi_start", { prompt: "wait behind", session_id: session });
		expect(second.structured.session_id).toBe(session);
		const secondExec: string = second.structured.execution_id;
		const status = await client.tool("pi_status", { execution_id: secondExec });
		expect(["queued", "running"]).toContain(status.structured.state);
		expect(startedCount(ws)).toBe(1);

		// Withdrawal: cancelled durably, no pi process launched for it.
		const cancelled = await client.tool("pi_cancel", { execution_id: secondExec });
		expect(cancelled.structured.state).toBe("cancelled");
		expect(startedCount(ws)).toBe(1);

		// Idempotent: cancelling a terminal execution is the same no-op.
		const again = await client.tool("pi_cancel", { execution_id: secondExec });
		expect(again.structured.state).toBe("cancelled");

		// And cancelling the lane holder works too.
		const firstCancel = await client.tool("pi_cancel", { execution_id: first.structured.execution_id });
		expect(firstCancel.structured.state).toBe("cancelled");
		expect(startedCount(ws)).toBe(1);
	});

	test("pi_cancel aborts a running execution and answers after cleanup", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "hang", // rpc still answers abort with a settled turn
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "cancel me running" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});

		const before = Date.now();
		const cancelled = await client.tool("pi_cancel", { execution_id: exec });
		expect(cancelled.structured.state).toBe("cancelled");
		expect(cancelled.structured.finished_at).toBeGreaterThanOrEqual(before);

		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.outcome).toBe("cancelled");
		expect(result.structured.state).toBe("cancelled");
	});

	test("pi_cancel escalates to signals when the child ignores abort", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "hang",
			FAKE_RPC_IGNORE_ABORT: "1",
			PI_MCP_KILL_GRACE_MS: "500",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "ignore the abort" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});

		const cancelled = await client.tool("pi_cancel", { execution_id: exec });
		expect(cancelled.structured.state).toBe("cancelled");
	});

	test("idempotency: same key returns the same execution, different request conflicts", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client(ws.asyncEnv);
		cleanup.push(() => client.close());

		const args = { prompt: "remember me", idempotency_key: "retry-1" };
		const first = await client.tool("pi_start", args);
		const second = await client.tool("pi_start", args);
		expect(second.structured.execution_id).toBe(first.structured.execution_id);
		expect(second.structured.session_id).toBe(first.structured.session_id);

		const conflict = await client.tool("pi_start", { ...args, prompt: "different work" });
		expectErrorCode(conflict, "idempotency_conflict");

		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: first.structured.execution_id });
			return res.structured?.state === "succeeded";
		});
		// The retry after completion still resolves to the same execution.
		const third = await client.tool("pi_start", args);
		expect(third.structured.execution_id).toBe(first.structured.execution_id);
		expect(third.structured.state).toBe("succeeded");
	});

	test("a retry after the original cwd is removed still hits the idempotency key", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, FAKE_STARTED_FILE: startedFile(ws) });
		cleanup.push(() => client.close());

		const gone = join(ws.dir, "gone-cwd");
		mkdirSync(gone, { recursive: true });
		const args = { prompt: "work in a doomed directory", cwd: gone, idempotency_key: "cwd-vanishes" };
		const first = await client.tool("pi_start", args);
		const exec: string = first.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		const spawned = startedCount(ws);
		rmSync(gone, { recursive: true, force: true });

		// An identical retry of the accepted request finds its execution: the cwd
		// is resolved on a key miss only, so its later removal cannot turn a
		// retry into invalid_input — and a terminal hit never re-spawns pi.
		const retry = await client.tool("pi_start", args);
		expect(retry.structured.execution_id).toBe(exec);
		expect(retry.structured.state).toBe("succeeded");
		expect(startedCount(ws)).toBe(spawned);

		// A fresh request for the missing directory is still rejected.
		expectErrorCode(await client.tool("pi_start", { ...args, idempotency_key: "fresh-cwd-1" }), "invalid_input");
	});

	test("idempotent retries resolve before the session is known to the adapter", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_RPC_SETTLE_MS: "60000",
			PI_MCP_MAX_CONCURRENT: "1", // one slot: the second job cannot spawn
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const holder = await client.tool("pi_start", { prompt: "hold the only slot" });
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: holder.structured.execution_id });
			return res.structured?.state === "running";
		});

		// This job is durably accepted but stuck in the queue: its session has
		// never been remembered (no spawn yet), and a network retry cannot know
		// more than the original request did. Only the idempotency record can
		// resolve the retry — and it must, before any mutable session check.
		const args = { prompt: "stuck behind the slot", idempotency_key: "evicted-session" };
		const queued = await client.tool("pi_start", args);
		expect(queued.structured.state).toBe("queued");
		const stuckExec: string = queued.structured.execution_id;

		// The honest retry: exactly the original request.
		const retry = await client.tool("pi_start", args);
		expect(retry.structured.execution_id).toBe(stuckExec);
		expect(retry.structured.state).toBe("queued");

		// Adding the session the response named is different content under the
		// same key: rejected, per the conflict rule.
		const conflicted = await client.tool("pi_start", {
			...args,
			session_id: queued.structured.session_id,
		});
		expectErrorCode(conflicted, "idempotency_conflict");

		// Withdraw both; nothing was spawned for the queued one.
		await client.tool("pi_cancel", { execution_id: stuckExec });
		await client.tool("pi_cancel", { execution_id: holder.structured.execution_id });
		await waitFor("no extra spawn", () => startedCount(ws) === 1);
	});

	test("an MCP cancellation before the acceptance commit starts nothing", async () => {
		const ws = track(makeAsyncWorkspace());
		const gate = join(ws.dir, "accept-gate");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_ASYNC_TEST_ACCEPT_GATE: gate,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => rmSync(gate, { force: true }));

		const pending = client.request("tools/call", {
			name: "pi_start",
			arguments: { prompt: "never accepted", idempotency_key: "cancelled-accept" },
		});
		// Ordering barrier: once this ping is answered, the pi_start line ahead of
		// it has been processed and its handler is parked inside the accept gate
		// (everything before the gate is synchronous, so no fixed sleep needed).
		await client.call("ping");
		client.send({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: pending.id } });
		// Ordering barrier: once this ping is answered, the cancellation line in
		// front of it has certainly been processed. Releasing the gate any
		// earlier would race the notification against the accept loop.
		await client.call("ping");
		writeFileSync(gate, "go\n"); // release the gate after the cancellation

		const res = await pending.promise;
		expect(res.result?.isError).toBe(true);
		expect(String(res.result?.content?.[0]?.text)).toContain("cancelled before the execution became durable");

		// Nothing became durable: the same key can still be used, and the work
		// it names runs and settles normally (no phantom queued execution).
		const fresh = await client.tool("pi_start", {
			prompt: "never accepted",
			idempotency_key: "cancelled-accept",
		});
		const exec: string = fresh.structured.execution_id;
		await waitFor("succeeded", async () => {
			const status = await client.tool("pi_status", { execution_id: exec });
			return status.structured?.state === "succeeded";
		});
		expect(startedCount(ws)).toBe(1);
		client.close();
	});

	test("typed errors for bad input, unknown executions, and foreign cursors", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client(ws.asyncEnv);
		cleanup.push(() => client.close());

		await client.handshake();

		expectErrorCode(await client.tool("pi_start", {}), "invalid_input");
		expectErrorCode(await client.tool("pi_start", { prompt: "x", session_id: "not-a-uuid" }), "invalid_input");
		expectErrorCode(
			await client.tool("pi_start", { prompt: "x", session_id: "0d0da423-c49f-46b7-8720-0f4b34fed123" }),
			"unknown_session",
		);
		expectErrorCode(await client.tool("pi_start", { prompt: "x", transport: "json" }), "invalid_input");

		const missing = "0d0da423-c49f-46b7-8720-0f4b34fed123";
		expectErrorCode(await client.tool("pi_status", { execution_id: missing }), "unknown_execution");
		expectErrorCode(await client.tool("pi_result", { execution_id: missing }), "unknown_execution");
		expectErrorCode(await client.tool("pi_cancel", { execution_id: missing }), "unknown_execution");

		const start = await client.tool("pi_start", { prompt: "cursor source" });
		const exec: string = start.structured.execution_id;
		expectErrorCode(
			await client.tool("pi_result", { execution_id: exec, after_cursor: "not-a-cursor" }),
			"invalid_cursor",
		);
		expectErrorCode(
			await client.tool("pi_result", {
				execution_id: exec,
				after_cursor: cursor("0d0da423-c49f-46b7-8720-0f4b34fed123", 1),
			}),
			"invalid_cursor",
		);
	});

	test("a store commit failure fails the async subsystem closed", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_ASYNC_TEST_FAIL_STATE: "running",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		// The acceptance commit (queued) succeeds; the dispatch commit
		// (running) is the injected failure, after which everything async
		// reports store_unavailable.
		const start = await client.tool("pi_start", { prompt: "trip the store" });
		expect(start.structured.state).toBe("queued");

		await waitFor("async tools fail closed", async () => {
			const status = await client.tool("pi_status", { execution_id: start.structured.execution_id });
			return status.structured?.error?.code === "store_unavailable";
		});

		expectErrorCode(await client.tool("pi_start", { prompt: "no more admissions" }), "store_unavailable");

		// The legacy tools are untouched by the async subsystem's failure.
		const legacy = await client.tool("pi", { prompt: "legacy still works" });
		expect(legacy.isError).toBe(false);
		expect(legacy.text).toContain("FINAL ANSWER");
	});

	test("a lost supervisor is reconciled as interrupted and the session survives", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-recovery");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));

		const base: Record<string, string> = {
			FAKE_RPC_SETTLE_MS: "60000",
			FAKE_STARTED_FILE: join(stateDir, "started.log"),
		};

		// Supervisor A starts a long turn, then goes away cleanly.
		const clientA = new Client(
			{
				...base,
				PI_MCP_BIN: makeFakeBin(stateDir),
				PI_MCP_STATE: join(stateDir, "sessions.json"),
				PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			},
			stateDir,
		);
		const start = await clientA.tool("pi_start", { prompt: "outlive the supervisor" });
		const exec: string = start.structured.execution_id;
		const session: string = start.structured.session_id;
		await waitFor("running", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});
		await waitFor("pi to have started once", () => startedCountOf(stateDir) === 1);
		clientA.child.kill("SIGTERM");
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));

		// Supervisor B, fresh process, same state dirs. Its own pi invocations
		// settle immediately (no FAKE_RPC_SETTLE_MS), so the resumption check
		// below is quick.
		const clientB = new Client(
			{
				PI_MCP_BIN: makeFakeBin(stateDir),
				PI_MCP_STATE: join(stateDir, "sessions.json"),
				PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			},
			stateDir,
		);
		cleanup.push(() => clientB.close());

		const status = await clientB.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("interrupted");
		const result = await clientB.tool("pi_result", { execution_id: exec });
		expect(result.structured.final).toBeNull();
		// No auto-relaunch of a prompt that may have edited files.
		expect(startedCountOf(stateDir)).toBe(1);

		// Routing repair: the session is known again, so it can be continued.
		const sessions = await clientB.tool("pi_sessions", {});
		const listed = sessions.text.includes(session);
		expect(listed).toBe(true);
		const resumed = await clientB.tool("pi_reply", { session, prompt: "continue" });
		expect(resumed.isError).toBe(false);
	});

	test("a crashed supervisor leaves the lock; removing it by hand recovers", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-crash");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));

		const env: Record<string, string> = {
			PI_MCP_BIN: makeFakeBin(stateDir),
			PI_MCP_STATE: join(stateDir, "sessions.json"),
			PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			FAKE_RPC_SETTLE_MS: "60000",
		};

		const clientA = new Client(env, stateDir);
		const start = await clientA.tool("pi_start", { prompt: "crash the supervisor" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});

		// A real crash: SIGKILL, no cleanup, the lock survives.
		clientA.child.kill("SIGKILL");
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));

		const clientB = new Client(env, stateDir);
		cleanup.push(() => clientB.close());
		const refused = await clientB.tool("pi_status", { execution_id: exec });
		expectErrorCode(refused, "store_unavailable");

		// The documented operator path: prove the owner is gone, remove the
		// lock, restart. Reconciliation then reports the orphan honestly.
		rmSync(join(stateDir, "state", "lock"), { recursive: true, force: true });
		const clientC = new Client(env, stateDir);
		cleanup.push(() => clientC.close());
		const recovered = await clientC.tool("pi_status", { execution_id: exec });
		expect(recovered.structured.state).toBe("interrupted");
	});

	test("mid-run shutdown with closed stdio releases the lock without manual removal", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-midrun-term");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));
		const env: Record<string, string> = {
			PI_MCP_BIN: makeFakeBin(stateDir),
			PI_MCP_STATE: join(stateDir, "sessions.json"),
			PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			FAKE_RPC_SETTLE_MS: "60000",
			FAKE_STARTED_FILE: join(stateDir, "started.log"),
			// The kill grace bounds the shutdown tail; the rest of the test does not
			// need the full five seconds.
			PI_MCP_KILL_GRACE_MS: "250",
		};

		// A job is genuinely mid-flight when the supervisor is torn down the way
		// mcp-auth-proxy tears a stdio child down: the signal arrives, then the
		// pipes close and nothing else holds the child's event loop.
		const clientA = new Client(env, stateDir);
		const start = await clientA.tool("pi_start", { prompt: "outlive a proxy teardown" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});
		await waitFor("fake pi spawned", () => startedCountOf(stateDir) === 1);
		const fakePid = Number(readFileSync(join(stateDir, "started.log"), "utf8").trim().split("\n")[0]);
		expect(pidAlive(fakePid)).toBe(true);

		clientA.child.kill("SIGTERM");
		clientA.child.stdin.end();
		clientA.child.stdout.destroy();
		clientA.child.stderr.destroy();
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));

		// The whole pi process group is gone — the supervisor reaped it itself.
		expect(pidAlive(fakePid)).toBe(false);

		// The lock was released by the supervisor before it exited; no operator
		// had to remove <state-dir>/lock by hand.
		expect(existsSync(join(stateDir, "state", "lock"))).toBe(false);

		// A fresh supervisor opens the same store and reads the job as interrupted.
		const clientB = new Client(env, stateDir);
		cleanup.push(() => clientB.close());
		const status = await clientB.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("interrupted");
	});
	test("cancelling in the pre-spawn window launches nothing (spawn gate)", async () => {
		const ws = track(makeAsyncWorkspace());
		const gate = join(ws.dir, "spawn-gate");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_ASYNC_TEST_SPAWN_GATE: gate,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => rmSync(gate, { force: true }));

		// The running commit is durable; the spawn is parked behind the gate.
		const start = await client.tool("pi_start", { prompt: "park before spawn" });
		const exec: string = start.structured.execution_id;
		await waitFor("running before spawn", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});
		expect(startedCount(ws)).toBe(0);

		// Cancel while parked; only then open the window.
		const cancel = client.request("tools/call", {
			name: "pi_cancel",
			arguments: { execution_id: exec },
		});
		await client.call("ping"); // the cancel is processed; the job is parked
		writeFileSync(gate, "go\n");
		const res = await cancel.promise;
		expect(res.result?.structuredContent?.state).toBe("cancelled");
		expect(startedCount(ws)).toBe(0);

		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.outcome).toBe("cancelled");
		expect(result.structured.final.failure_reason).toContain("before the turn started");
	});

	test("continuation retries resolve by key even after the session map evicted the entry", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, PI_MCP_MAX_SESSIONS: "2" });
		cleanup.push(() => client.close());

		// A real known session, established and used for a continuation.
		const seed = await client.tool("pi_start", { prompt: "seed", idempotency_key: "cont-seed" });
		const session: string = seed.structured.session_id;
		await waitFor("seeded", async () => {
			const res = await client.tool("pi_status", { execution_id: seed.structured.execution_id });
			return res.structured?.state === "succeeded";
		});
		const cont = await client.tool("pi_start", {
			prompt: "follow",
			idempotency_key: "cont-retry",
			session_id: session,
		});
		const contExec: string = cont.structured.execution_id;
		await waitFor("continued", async () => {
			const res = await client.tool("pi_status", { execution_id: contExec });
			return res.structured?.state === "succeeded";
		});

		// Evict the advisory entry: the session map keeps two, and two fresh
		// synchronous pi sessions push the seeded one out.
		await client.tool("pi", { prompt: "one" });
		await client.tool("pi", { prompt: "two" });
		const sessions = await client.tool("pi_sessions", {});
		expect(sessions.text.includes(session)).toBe(false);

		// Same key, same intent: the original execution, without any session
		// lookup — the entry is gone and this must still not be unknown_session.
		const retry = await client.tool("pi_start", {
			prompt: "follow",
			idempotency_key: "cont-retry",
			session_id: session,
		});
		expect(retry.structured.execution_id).toBe(contExec);
		expect(retry.structured.state).toBe("succeeded");

		// Same key, changed intent: conflict, still before any mutable check.
		const conflicted = await client.tool("pi_start", {
			prompt: "other work",
			idempotency_key: "cont-retry",
			session_id: session,
		});
		expectErrorCode(conflicted, "idempotency_conflict");

		// Fresh key naming an evicted session: honestly unknown.
		const fresh = await client.tool("pi_start", {
			prompt: "more work",
			idempotency_key: "cont-fresh",
			session_id: session,
		});
		expectErrorCode(fresh, "unknown_session");
	});

	test("a store write failure reaps every async child and fails closed", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "hang",
			FAKE_CHILD_TAG: CHILD_TAG,
			FAKE_STARTED_FILE: startedFile(ws),
			// The injected failure fires on the first `cancelling` commit, i.e. on
			// the cancel below — both children are already live by then.
			PI_MCP_ASYNC_TEST_FAIL_STATE: "cancelling",
		});
		cleanup.push(() => client.close());

		const first = await client.tool("pi_start", { prompt: "child one" });
		const second = await client.tool("pi_start", { prompt: "child two" });
		for (const exec of [first.structured.execution_id, second.structured.execution_id]) {
			await waitFor("running", async () => {
				const res = await client.tool("pi_status", { execution_id: exec });
				return res.structured?.state === "running";
			});
		}
		await waitFor("both children live", () => survivors().split("\n").filter(Boolean).length === 2);

		// The cancel's `cancelling` commit fails: the answer is the store error,
		// never a success for a state that was not committed.
		const cancelled = await client.tool("pi_cancel", { execution_id: first.structured.execution_id });
		expectErrorCode(cancelled, "store_unavailable");

		// Both children were reaped, not just the one being cancelled.
		await waitFor("both trees reaped", () => survivors() === "");

		// No admissions, no reads, no dispatch.
		expectErrorCode(
			await client.tool("pi_status", { execution_id: second.structured.execution_id }),
			"store_unavailable",
		);
		expectErrorCode(await client.tool("pi_start", { prompt: "no more" }), "store_unavailable");
	});

	test("an ambiguous acceptance is resolved by idempotent retry after restart", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-ambiguous");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));
		const env: Record<string, string> = {
			PI_MCP_BIN: makeFakeBin(stateDir),
			PI_MCP_STATE: join(stateDir, "sessions.json"),
			PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			// The acceptance rename lands, then the commit reports failure: the
			// record is durable while the response says it was not.
			PI_MCP_ASYNC_TEST_FAIL_STATE: "queued",
			PI_MCP_ASYNC_TEST_FAIL_POINT: "after_rename",
		};

		const clientA = new Client(env, stateDir);
		const res = await clientA.tool("pi_start", { prompt: "ambiguous", idempotency_key: "amb-1" });
		expectErrorCode(res, "store_unavailable");
		clientA.child.kill("SIGTERM");
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));

		// The retry after restart resolves the ambiguity instead of doubling the
		// work: same key, same intent, the original execution — reconciled as
		// interrupted, not a fresh queued run.
		const clientB = new Client(env, stateDir);
		cleanup.push(() => clientB.close());
		const retry = await clientB.tool("pi_start", { prompt: "ambiguous", idempotency_key: "amb-1" });
		expect(retry.isError).toBe(false);
		expect(retry.structured.state).toBe("interrupted");
		const result = await clientB.tool("pi_result", { execution_id: retry.structured.execution_id });
		expect(result.structured.state).toBe("interrupted");
		expect(result.structured.final).toBeNull();
	});

	test("the store lock is held until the pi trees are gone", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-lockhandoff");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));
		const env: Record<string, string> = {
			PI_MCP_BIN: makeFakeBin(stateDir),
			PI_MCP_STATE: join(stateDir, "sessions.json"),
			PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			FAKE_MODE: "hang",
			FAKE_CHILD_TAG: CHILD_TAG,
			// A wide grace window: the second supervisor must start inside it.
			PI_MCP_KILL_GRACE_MS: "4000",
		};

		const clientA = new Client(env, stateDir);
		const start = await clientA.tool("pi_start", { prompt: "hold the store" });
		const exec: string = start.structured.execution_id;
		await waitFor("running with a live child", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running" && survivors() !== "";
		});

		// Supervisor A starts shutting down; the trees are still alive.
		clientA.child.kill("SIGTERM");

		// Supervisor B starts inside the cleanup window: the park is written, the
		// trees are not gone, so the lock must still be A's.
		const clientB = new Client(env, stateDir);
		cleanup.push(() => clientB.close());
		expectErrorCode(await clientB.tool("pi_status", { execution_id: exec }), "store_unavailable");

		// A finishes: trees reaped, lock released, B's successor may acquire.
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));
		await waitFor("the tree reaped", () => survivors() === "");
		const clientC = new Client(env, stateDir);
		cleanup.push(() => clientC.close());
		const recovered = await clientC.tool("pi_status", { execution_id: exec });
		expect(recovered.structured.state).toBe("interrupted");
	});

	test("shutdown does not hand the store over while a TERM-immune descendant lives", async () => {
		const ws = track(makeAsyncWorkspace());
		const survivorPidFile = join(ws.dir, "survivor.pid");
		const env: Record<string, string> = {
			...ws.asyncEnv,
			// The leader hangs mid-turn; a descendant of its group closes its
			// inherited pipes and ignores SIGTERM, writing its pid for the test.
			FAKE_MODE: "hang",
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_SURVIVOR_PID_FILE: survivorPidFile,
			// A wide grace: supervisor B must start inside the window.
			PI_MCP_KILL_GRACE_MS: "8000",
		};

		const clientA = new Client(env, ws.dir);
		const start = await clientA.tool("pi_start", { prompt: "hold the store" });
		const exec: string = start.structured.execution_id;
		await waitFor("running with a survivor", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running" && existsSync(survivorPidFile);
		});
		const leaderPid = Number(readFileSync(startedFile(ws), "utf8").trim().split("\n")[0]);
		const survivorPid = Number(readFileSync(survivorPidFile, "utf8").trim());

		// SIGTERM: the leader dies at once — its pipes close, the run "finishes" —
		// but the descendant ignored the signal and is still in the group.
		clientA.child.kill("SIGTERM");
		await waitFor("the leader is gone", () => !pidAlive(leaderPid));
		expect(pidAlive(survivorPid)).toBe(true);

		// Supervisor B starts inside the cleanup window: the leader is gone, a
		// group member is not, so the lock must still be A's. Untracking the
		// tree at leader close would already have made this acquisition work.
		const clientB = new Client(env, ws.dir);
		cleanup.push(() => clientB.close());
		expectErrorCode(await clientB.tool("pi_status", { execution_id: exec }), "store_unavailable");

		// A finishes: the SIGKILL reaches the survivor through the still-tracked
		// tree, the group is confirmed empty, and only then is the lock released.
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));
		expect(pidAlive(survivorPid)).toBe(false);

		// The successor now acquires and reads the parked record.
		const clientC = new Client(env, ws.dir);
		cleanup.push(() => clientC.close());
		const recovered = await clientC.tool("pi_status", { execution_id: exec });
		expect(recovered.structured.state).toBe("interrupted");
	});

	test("pi_cancel settles only after the process group is confirmed empty", async () => {
		const ws = track(makeAsyncWorkspace());
		const survivorPidFile = join(ws.dir, "survivor.pid");
		const client = new Client({
			...ws.asyncEnv,
			// The leader sits mid-turn for a long time; a descendant of its
			// group ignores SIGTERM, so only the post-grace SIGKILL can empty
			// the group.
			FAKE_RPC_SETTLE_MS: "60000",
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_SURVIVOR_PID_FILE: survivorPidFile,
			PI_MCP_KILL_GRACE_MS: "1500",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "hold the turn" });
		const exec: string = start.structured.execution_id;
		const session: string = start.structured.session_id;
		await waitFor("running with a survivor", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running" && existsSync(survivorPidFile);
		});
		const leaderPid = Number(readFileSync(startedFile(ws), "utf8").trim().split("\n")[0]);
		const survivorPid = Number(readFileSync(survivorPidFile, "utf8").trim());

		// Ordinary cancel, no supervisor shutdown. The leader honors the
		// in-protocol abort and exits at once; the survivor ignores the SIGTERM
		// and holds the group.
		const cancel = client.request("tools/call", {
			name: "pi_cancel",
			arguments: { execution_id: exec },
		});
		await client.call("ping");
		await waitFor("the leader exited on the abort", () => !pidAlive(leaderPid));
		expect(pidAlive(survivorPid)).toBe(true);

		// The turn has not settled and the session lane is still held: a next
		// turn on the same session must not spawn its child while the leftover
		// is alive.
		const before = startedCount(ws);
		const next = client.request("tools/call", {
			name: "pi_start",
			arguments: { session_id: session, prompt: "next turn" },
		});
		await client.call("ping");
		await sleep(400);
		expect(pidAlive(survivorPid)).toBe(true); // still inside the SIGTERM grace
		expect(startedCount(ws)).toBe(before); // the lane stayed held

		// Only the escalation's SIGKILL empties the group, and the cancel —
		// and the next turn — could not return before that.
		const cancelled = await cancel.promise;
		expect(cancelled.result.structuredContent.state).toBe("cancelled");
		expect(pidAlive(survivorPid)).toBe(false);
		const nextView = await next.promise;
		expect(nextView.result.isError === true).toBe(false);
		expect(nextView.result.structuredContent.execution_id).toBeDefined();
		await waitFor("the next turn ran", () => startedCount(ws) === before + 1);
	});

	test("an unverifiable process group fails the cancel closed", async () => {
		const ws = track(makeAsyncWorkspace());
		const survivorPidFile = join(ws.dir, "survivor.pid");
		const client = new Client({
			...ws.asyncEnv,
			// The leader exits on the in-protocol abort — long before any
			// fallback signal — while the survivor stays in the group.
			FAKE_RPC_SETTLE_MS: "60000",
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_SURVIVOR_PID_FILE: survivorPidFile,
			// No grace and no verification window: SIGKILL goes out, but the
			// group cannot be confirmed empty in time.
			PI_MCP_KILL_GRACE_MS: "0",
			PI_MCP_TREE_GONE_MS: "0",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "hold again" });
		const exec: string = start.structured.execution_id;
		await waitFor("running with a survivor", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running" && existsSync(survivorPidFile);
		});

		// Fail closed: not a clean cancelled — the record reports the
		// unverified cleanup, so nothing downstream mistakes it for settled.
		const cancelled = await client.tool("pi_cancel", { execution_id: exec });
		expect(cancelled.structured.state).toBe("failed");
		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.failure_reason).toContain("could not be confirmed empty");
	});

	test("a session is quarantined until the leftover group is confirmed gone", async () => {
		const ws = track(makeAsyncWorkspace());
		// The lingering-group fixture: a zombie inside pi's process group,
		// held by a process outside it. No signal can remove a zombie, so the
		// turn ends unverified — deterministically, for exactly as long as
		// the test holds the holder alive.
		const holderPidFile = join(ws.dir, "holder.pid");
		const client = new Client({
			...ws.asyncEnv,
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_LINGERING_GROUP_FILE: holderPidFile,
			PI_MCP_KILL_GRACE_MS: "0",
			PI_MCP_TREE_GONE_MS: "0",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "leave a lingering group behind" });
		const exec: string = start.structured.execution_id;
		const session: string = start.structured.session_id;
		await waitFor("failed with an unverifiable group", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "failed" && existsSync(holderPidFile);
		});
		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.failure_reason).toContain("could not be confirmed empty");

		// While verification is unresolved, every follow-up on the session is
		// refused — async and sync — and nothing spawns.
		const before = startedCount(ws);
		const rejected = await client.tool("pi_start", { session_id: session, prompt: "next turn" });
		expect(rejected.structured.error.code).toBe("session_quarantined");
		const replyRejected = await client.tool("pi_reply", { session, prompt: "sync follow-up" });
		expect(replyRejected.isError).toBe(true);
		expect(replyRejected.text).toContain("blocked");
		expect(startedCount(ws)).toBe(before);

		// The holder's death lets the zombie be reaped; the sweep confirms the
		// group empty, the quarantine lifts, and the same follow-up runs.
		const holderPid = Number(readFileSync(holderPidFile, "utf8").trim());
		process.kill(holderPid, "SIGKILL");
		let accepted: string | undefined;
		await waitFor("the follow-up is accepted and runs", async () => {
			const res = await client.tool("pi_start", { session_id: session, prompt: "next turn, again" });
			accepted = res.structured?.execution_id;
			return accepted !== undefined && startedCount(ws) === before + 1;
		});
		await waitFor("the follow-up settles", async () => {
			const res = await client.tool("pi_status", { execution_id: accepted });
			return res.structured?.state === "succeeded";
		});
	});

	test("a sync pi turn that leaves an unverified group blocks its own session", async () => {
		const ws = track(makeAsyncWorkspace());
		const holderPidFile = join(ws.dir, "holder.pid");
		const client = new Client({
			...ws.asyncEnv,
			FAKE_LINGERING_GROUP_FILE: holderPidFile,
			PI_MCP_KILL_GRACE_MS: "0",
			PI_MCP_TREE_GONE_MS: "0",
		});
		cleanup.push(() => client.close());

		// The sync path takes the same fail-closed turn end: no clean success
		// while a leftover may live, and the session is blocked behind it.
		const res = await client.tool("pi", { prompt: "sync turn with a leftover" });
		expect(res.isError).toBe(true);
		expect(res.text).toContain("could not be confirmed empty");
		const session = /\[session: ([^\]]+)\]/.exec(res.text)?.[1];
		expect(session).toBeDefined();

		const blocked = await client.tool("pi_reply", { session, prompt: "follow-up" });
		expect(blocked.isError).toBe(true);
		expect(blocked.text).toContain("blocked");

		const holderPid = Number(readFileSync(holderPidFile, "utf8").trim());
		process.kill(holderPid, "SIGKILL");
		await waitFor("the sync follow-up runs once the group is confirmed gone", async () => {
			const ok = await client.tool("pi_reply", { session, prompt: "follow-up, again" });
			return ok.isError !== true;
		});
	});

	test("follow-ups queued before the quarantine lands refuse under the lane, and nothing spawns", async () => {
		const ws = track(makeAsyncWorkspace());
		const holderPidFile = join(ws.dir, "holder.pid");
		// A stretched settle holds the session lane long enough that both
		// follow-ups are queued behind the running turn before its unverified
		// end sets the quarantine — the exact window the door checks cannot
		// cover: they passed admission before the block existed.
		const client = new Client({
			...ws.asyncEnv,
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_LINGERING_GROUP_FILE: holderPidFile,
			FAKE_RPC_SETTLE_MS: "1200",
			PI_MCP_KILL_GRACE_MS: "0",
			PI_MCP_TREE_GONE_MS: "0",
		});
		cleanup.push(() => client.close());

		// Turn one holds the lane. While it runs, a sync pi_reply queues on the
		// session lock and an async continuation is accepted into the queue —
		// both before the quarantine exists.
		const start = await client.tool("pi_start", { prompt: "turn that leaves a leftover" });
		const exec: string = start.structured.execution_id;
		const session: string = start.structured.session_id;
		const replyQueued = client.tool("pi_reply", { session, prompt: "queued sync follow-up" });
		const contQueued = await client.tool("pi_start", {
			session_id: session,
			prompt: "queued async continuation",
		});
		const contExec: string = contQueued.structured.execution_id;

		await waitFor("turn one ends with an unverifiable group", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "failed";
		});

		// The queued follow-ups now take their turns under the lane: the sync
		// one refuses with the blocked-session error, the async one fails
		// closed with the typed reason — and neither spawns.
		const reply = await replyQueued;
		expect(reply.isError).toBe(true);
		expect(reply.text).toContain("blocked");
		await waitFor("the queued continuation fails closed without spawning", async () => {
			const res = await client.tool("pi_status", { execution_id: contExec });
			return res.structured?.state === "failed";
		});
		const contResult = await client.tool("pi_result", { execution_id: contExec });
		expect(contResult.structured.final.failure_reason).toContain("session_quarantined");
		expect(startedCount(ws)).toBe(1);

		// The holder's death lets the zombie be reaped; the sweep confirms the
		// group empty, the quarantine lifts, and a follow-up runs again.
		const holder2Pid = Number(readFileSync(holderPidFile, "utf8").trim());
		process.kill(holder2Pid, "SIGKILL");
		let accepted: string | undefined;
		await waitFor("the follow-up is accepted and runs", async () => {
			const res = await client.tool("pi_start", { session_id: session, prompt: "next turn, again" });
			accepted = res.structured?.execution_id;
			return accepted !== undefined && startedCount(ws) === 2;
		});
		await waitFor("the follow-up settles", async () => {
			const res = await client.tool("pi_status", { execution_id: accepted });
			return res.structured?.state === "succeeded";
		});
	});

	test("a sync turn's quarantine lands before its lane is released, so a queued follow-up refuses", async () => {
		const ws = track(makeAsyncWorkspace());
		const holderPidFile = join(ws.dir, "holder.pid");
		const client = new Client({
			...ws.asyncEnv,
			FAKE_STARTED_FILE: startedFile(ws),
			FAKE_LINGERING_GROUP_FILE: holderPidFile,
			FAKE_RPC_SETTLE_MS: "1200",
			PI_MCP_KILL_GRACE_MS: "0",
			PI_MCP_TREE_GONE_MS: "0",
		});
		cleanup.push(() => client.close());

		// A sync pi_reply on a caller-chosen session id (pi creates the session
		// under that id); a second pi_reply queues on the lane behind it while
		// the turn runs — before its unverified end sets the quarantine.
		const session = "sync-race-session";
		const runOne = client.tool("pi_reply", { session, prompt: "sync turn with a leftover" });
		const runTwo = client.tool("pi_reply", { session, prompt: "queued behind it" });
		const first = await runOne;
		expect(first.isError).toBe(true);
		expect(first.text).toContain("could not be confirmed empty");

		// The queued follow-up rechecks under the lane and refuses; the turn
		// set the block before releasing it, and nothing beyond turn one spawns.
		const second = await runTwo;
		expect(second.isError).toBe(true);
		expect(second.text).toContain("blocked");
		expect(startedCount(ws)).toBe(1);
	});

	test("a session lane is shared across the sync and async paths", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_RPC_SETTLE_MS: "6000",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		// An async turn holds session S's lane for a long time.
		const job = await client.tool("pi_start", { prompt: "hold the lane" });
		const session: string = job.structured.session_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: job.structured.execution_id });
			return res.structured?.state === "running";
		});
		await waitFor("the holder spawned", () => startedCount(ws) === 1);

		// pi_reply on the same session is parked on the same lane: no second
		// pi on S while the async turn owns it.
		const reply = client.request("tools/call", {
			name: "pi_reply",
			arguments: { session, prompt: "me too" },
		});
		await client.call("ping");
		await sleep(300);
		expect(startedCount(ws)).toBe(1);

		// The holder settles on its own; only then does the parked reply run.
		const replyRes = await reply.promise;
		expect(replyRes.result?.isError !== true).toBe(true);
		expect(String(replyRes.result?.content?.[0]?.text)).toContain("RPC ANSWER");
		await waitFor("the reply spawned after the lane freed", () => startedCount(ws) === 2);
	});

	test("pi_send abort routes an async turn through the cancel transition", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({ ...ws.asyncEnv, FAKE_MODE: "hang" });
		cleanup.push(() => client.close());

		const job = await client.tool("pi_start", { prompt: "abort me via pi_send" });
		const session: string = job.structured.session_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: job.structured.execution_id });
			return res.structured?.state === "running";
		});

		await client.tool("pi_send", { session, command: "abort" });
		await waitFor("cancelled via pi_send", async () => {
			const res = await client.tool("pi_status", { execution_id: job.structured.execution_id });
			return res.structured?.state === "cancelled";
		});
		const result = await client.tool("pi_result", { execution_id: job.structured.execution_id });
		expect(result.structured.final.outcome).toBe("cancelled");
	});

	test("synchronous pi runs beside an async turn without serializing", async () => {
		const ws = track(makeAsyncWorkspace());
		// rpc settles slowly (the async holder stays running); the synchronous
		// path uses the json transport, which settles immediately.
		const client = new Client({ ...ws.asyncEnv, FAKE_RPC_SETTLE_MS: "8000" });
		cleanup.push(() => client.close());

		const job = await client.tool("pi_start", { prompt: "hold a lane of my own" });
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: job.structured.execution_id });
			return res.structured?.state === "running";
		});

		// A fresh-session synchronous pi must not wait for the async turn. The
		// print transport settles immediately regardless of the rpc settle
		// delay the async holder is configured with.
		const started = Date.now();
		const res = await client.tool("pi", { prompt: "independent work", transport: "print" });
		expect(res.isError).toBe(false);
		expect(Date.now() - started).toBeLessThan(5000);

		// Leave nothing behind.
		await client.tool("pi_cancel", { execution_id: job.structured.execution_id });
	});

	test("pending overflow marks the loss; the committed sequence stays contiguous", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "snapshot_delay",
			FAKE_DELAY_MS: "500",
			// The unflushed buffer keeps only 4 events; the scenario pushes more,
			// so the memory bound must drop and say so. Retention stays generous so
			// the marker survives in the store.
			PI_MCP_ASYNC_PENDING_EVENTS: "4",
			// One commit at the end, so the drop and the marker land together.
			PI_MCP_ASYNC_FLUSH_MS: "10000",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "overflow the buffer" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		// Drain everything from the retention floor: contiguous sequences — the
		// dropped events never open an interior hole — and a gap marker carrying
		// the dropped count, in place of the events it stands for.
		const whole = await client.tool("pi_result", { execution_id: exec });
		const floor: number = whole.structured.oldest_retained_seq;
		const seqs: number[] = [];
		let gapMarker: { seq: number; dropped: number } | null = null;
		let after: string | undefined;
		for (;;) {
			const page = await client.tool("pi_result", {
				execution_id: exec,
				limit: 50,
				...(after !== undefined ? { after_cursor: after } : {}),
			});
			for (const event of page.structured?.events ?? []) {
				// Contiguous from the floor: no interior sequence gap, ever.
				expect(event.seq).toBe(seqs.length === 0 ? floor : seqs.at(-1)! + 1);
				if (event.type === "gap") gapMarker = event;
				seqs.push(event.seq);
			}
			if (page.structured?.has_more !== true) break;
			after = page.structured.next_cursor;
		}
		expect(gapMarker).not.toBeNull();
		expect(gapMarker?.dropped).toBeGreaterThan(0);
		// Nothing was silently invented: the marker occupies exactly one seq,
		// and the final is intact.
		expect(whole.structured.final.answer.text).toBe("FINAL AFTER SNAPSHOT");
	});

	test("the record bound refuses new work but never blocks terminal commits", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			// Long enough that no job can finish before the bound is reached.
			FAKE_RPC_SETTLE_MS: "3000",
			PI_MCP_ASYNC_MAX_RECORDS: "10",
		});
		cleanup.push(() => client.close());

		// Accepted jobs fill the record bound; all are still running.
		const execs: string[] = [];
		for (let i = 0; i < 10; i++) {
			const res = await client.tool("pi_start", { prompt: `fill ${i}` });
			execs.push(res.structured.execution_id);
		}
		await waitFor("all ten running", async () => {
			const states = await Promise.all(
				execs.map(async (exec) => (await client.tool("pi_status", { execution_id: exec })).structured?.state),
			);
			return states.every((state: string) => state === "running");
		});

		// At the bound there is no headroom for an eleventh.
		expectErrorCode(await client.tool("pi_start", { prompt: "one too many" }), "queue_full");

		// The accepted jobs still commit their terminals at the bound.
		for (const exec of execs) {
			await waitFor("terminal", async () => {
				const res = await client.tool("pi_status", { execution_id: exec });
				return res.structured?.state === "succeeded";
			});
		}
	});

	test("a large final answer does not stall event pagination", async () => {
		const ws = track(makeAsyncWorkspace());
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "big",
			FAKE_SIZE: "400000",
			// A page budget wide enough to carry the whole 400KB final alongside
			// its events. Under the default 128KB budget the same answer arrives
			// page-clipped — the next test covers that path.
			PI_MCP_ASYNC_PAGE_BYTES: "1000000",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "huge answer" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		// The final comes back whole on the very first call — direct lookup, no
		// replay — and the progress events page alongside it; the big answer
		// never blocks the small ones.
		const first = await client.tool("pi_result", { execution_id: exec });
		expect(first.structured.final.answer.text).toHaveLength(400000);
		expect(first.structured.final.answer.truncated).toBe(false);
		expect(first.structured.final.page_clipped).toBeUndefined();
		expect(first.structured.events.length).toBeGreaterThanOrEqual(1);
		let events = first.structured.events.length;
		if (first.structured.has_more === true) {
			let after: string | undefined = first.structured.next_cursor;
			for (;;) {
				const page = await client.tool("pi_result", { execution_id: exec, after_cursor: after });
				events += page.structured.events.length;
				expect(page.structured.events.every((e: { type: string }) => e.type !== "gap")).toBe(true);
				if (page.structured.has_more !== true) break;
				after = page.structured.next_cursor;
			}
		}
		expect(events).toBeGreaterThanOrEqual(1);
	});
	test("the wire-frame cap holds on every page, long string ids and a clipped final included", async () => {
		const ws = track(makeAsyncWorkspace());
		// A 400K-char final of multibyte + escape-heavy pattern: UTF-8 bytes plus
		// JSON escaping run it well past the default 128KB page budget, so the
		// terminal answer can only fit page-clipped — and every frame carries a
		// long multibyte string id, echoed back byte for byte.
		const pad = '🌸"\\\n\t';
		const LONG_ID = 'frame-budget-probe-🌸-escape-"quoted"-back\\slash-0123456789abcdefghijklmnopqrstuvwxyz';
		const PAGE_BYTES = 131072;
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "big",
			FAKE_SIZE: "400000",
			FAKE_FINAL_PAD: pad,
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "big multibyte final" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		const seqs: number[] = [];
		let after: string | undefined;
		let pages = 0;
		for (;;) {
			const msg = await client.toolWithId(LONG_ID, "pi_result", {
				execution_id: exec,
				limit: 50,
				...(after ? { after_cursor: after } : {}),
			});
			// The frame as it went over the wire: the same long id echoed back,
			// the same escaping, newline included.
			expect(msg.id).toBe(LONG_ID);
			expect(Buffer.byteLength(`${JSON.stringify(msg)}\n`, "utf8")).toBeLessThanOrEqual(PAGE_BYTES);
			const payload = msg.result.structuredContent;
			for (const event of payload.events) {
				expect(event.seq).toBeGreaterThan(seqs.at(-1) ?? 0);
				seqs.push(event.seq);
			}
			pages += 1;
			if (pages === 1) {
				// Direct final lookup on the first call, no replay: clipped to the
				// page budget and marked — pi did not truncate the answer itself.
				expect(payload.final).toBeDefined();
				expect(payload.final.page_clipped).toBe(true);
				expect(payload.final.answer.truncated).toBe(false);
				expect(payload.final.answer.text.length).toBeGreaterThan(0);
				expect(payload.final.answer.text.length).toBeLessThan(400000);
				// The clip never splits a surrogate pair.
				expect(Buffer.from(payload.final.answer.text, "utf8").toString("utf8")).toBe(payload.final.answer.text);
			}
			if (payload.has_more !== true) {
				// The terminal page carries the final too, still inside the cap.
				expect(payload.final.page_clipped).toBe(true);
				break;
			}
			// The cursor advances: the next page starts past everything seen.
			after = payload.next_cursor;
			expect(after).toBeDefined();
		}
		expect(pages).toBeGreaterThanOrEqual(1);
	});

	test("a page too small for even an empty answer refuses with a bounded error", async () => {
		const ws = track(makeAsyncWorkspace());
		// The reviewer's reproduction: the default fake settles normally, the
		// page cap is 1024, and a numeric id — the smallest envelope there is.
		// The success frame needs ~1459 bytes; the honest answer is a refusal
		// that itself fits the cap, not an oversized success.
		const client = new Client({ ...ws.asyncEnv, PI_MCP_ASYNC_PAGE_BYTES: "1024" });
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "small cap" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		const msg = await client.call2({
			jsonrpc: "2.0",
			id: 42,
			method: "tools/call",
			params: { name: "pi_result", arguments: { execution_id: exec } },
		});
		expect(msg.result.isError).toBe(true);
		expect(msg.result.structuredContent.error.code).toBe("page_too_small");
		expect(msg.result.structuredContent.error.message).toContain("PI_MCP_ASYNC_PAGE_BYTES=1024");
		// The refusal practices what the success could not: the whole frame —
		// payload twice, envelope, echoed id, newline — inside the cap.
		expect(Buffer.byteLength(`${JSON.stringify(msg)}\n`, "utf8")).toBeLessThanOrEqual(1024);
	});

	test("a request id that alone exceeds the cap is echoed and exempt; the refusal is bounded at the frame level", async () => {
		const ws = track(makeAsyncWorkspace());
		// A 4KB cap and a caller whose request id is larger than the cap
		// itself (4800 chars): the protocol requires echoing it back, so no
		// frame this server could build can fit. The exemption is explicit —
		// the id is echoed, and the refusal says which side of the frame is
		// over — and what the server adds stays bounded, measured on the
		// emitted frame, not just on the payload copies.
		const CAP = 4096;
		const client = new Client({ ...ws.asyncEnv, PI_MCP_ASYNC_PAGE_BYTES: String(CAP) });
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "huge id" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		// A string id larger than the cap itself: the protocol requires
		// echoing it back, so every frame carries it and none can fit the
		// cap. The refusal is what the server adds that stays bounded.
		const HUGE_ID = "id-probe-".repeat(480);
		expect(Buffer.byteLength(JSON.stringify(HUGE_ID), "utf8")).toBeGreaterThan(CAP);
		const msg = await client.toolWithId(HUGE_ID, "pi_result", { execution_id: exec });
		expect(msg.result.isError).toBe(true);
		expect(msg.result.structuredContent.error.code).toBe("page_too_small");
		expect(msg.id).toBe(HUGE_ID);
		// The refusal names the exempt side of the frame: the caller's id,
		// not the record.
		expect(msg.result.structuredContent.error.message).toContain("echoed request id");
		expect(msg.result.structuredContent.error.message).toContain("cap-exempt");
		// Bounded in both representations: the structured error and its text
		// copy are a few hundred bytes regardless of the id's size.
		expect(Buffer.byteLength(JSON.stringify(msg.result.structuredContent), "utf8")).toBeLessThan(300);
		expect(Buffer.byteLength(msg.result.content[0].text, "utf8")).toBeLessThan(300);
		// The emitted frame — not just the payload copies: the only bytes over
		// the cap are the echoed id, which JSON-RPC requires and the caller
		// chose. Everything the server wrote fits.
		const frame = Buffer.byteLength(`${JSON.stringify(msg)}\n`, "utf8");
		const idEcho = Buffer.byteLength(JSON.stringify(HUGE_ID), "utf8");
		expect(frame - idEcho).toBeLessThanOrEqual(CAP);
		expect(frame).toBeLessThanOrEqual(CAP + idEcho);
	});

	test("a page_clipped final is recovered whole through final_only, without replaying events", async () => {
		const ws = track(makeAsyncWorkspace());
		// A 330K-char multibyte, escape-heavy final under a 64KB page cap: no
		// page can carry it, so it ships page-clipped with the recovery hint —
		// and final_only pages the retained text byte by byte instead. (330K
		// chars of this pad serialize to ~792KB, inside the effective final cap
		// of MAX_FRAME/4, so the stored final stays whole.)
		const pad = '🌸"\\\n\t';
		const PAGE_BYTES = 65536;
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "big",
			FAKE_SIZE: "330000",
			FAKE_FINAL_PAD: pad,
			PI_MCP_ASYNC_PAGE_BYTES: String(PAGE_BYTES),
		});
		cleanup.push(() => client.close());

		// The fake pads by UTF-16 units (pad.length is 6), so FAKE_SIZE 330000
		// is 55000 pads; the server's answer selection trims the trailing
		// whitespace, which here bites the final "\\n\t".
		const stored = pad.repeat(55000).trimEnd();
		const start = await client.tool("pi_start", { prompt: "big multibyte final, small pages" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		// The cursorless page: clipped, and marked with the way back.
		const first = await client.tool("pi_result", { execution_id: exec });
		expect(first.structured.final.page_clipped).toBe(true);
		expect(first.structured.final.recovery_hint).toContain("final_only");
		expect(first.structured.events.length).toBeGreaterThanOrEqual(1);
		const eventCursor: string = first.structured.next_cursor;

		// Drain the final alone: no events on any chunk, every frame inside
		// the cap, offsets advancing by the bytes actually shipped.
		let text = "";
		let after: string | undefined;
		let pages = 0;
		let offset = 0;
		for (;;) {
			const msg = await client.toolWithId(`final-${pages}`, "pi_result", {
				execution_id: exec,
				final_only: true,
				...(after ? { after_cursor: after } : {}),
			});
			expect(Buffer.byteLength(`${JSON.stringify(msg)}\n`, "utf8")).toBeLessThanOrEqual(PAGE_BYTES);
			const p = msg.result.structuredContent;
			expect(p.events).toBeUndefined();
			expect(p.final.page_clipped).toBeUndefined(); // chunks, not clips
			expect(p.final_offset).toBe(offset);
			expect(p.final_bytes).toBe(Buffer.byteLength(stored, "utf8"));
			text += p.final.answer.text;
			offset += Buffer.byteLength(p.final.answer.text, "utf8");
			pages += 1;
			if (p.has_more !== true) {
				expect(p.next_cursor).toBeNull();
				break;
			}
			after = p.next_cursor;
			expect(pages).toBeLessThan(100);
		}
		// The chunks concatenate to the whole retained answer — no replay, no
		// cap raise, no restart — byte for byte and pair-safely. The retained
		// text is the fake's 55000 pads with the answer selection's trailing
		// whitespace trim applied.
		expect(pages).toBeGreaterThan(1);
		expect(text).toBe(stored);
		expect(Buffer.from(text, "utf8").toString("utf8")).toBe(text);
		expect(text.startsWith(pad)).toBe(true);

		// The two cursor namespaces never mix: an event cursor cannot page the
		// final, and the error says which cursors do.
		const bad = await client.tool("pi_result", {
			execution_id: exec,
			final_only: true,
			after_cursor: eventCursor,
		});
		expect(bad.structured.error.code).toBe("invalid_cursor");
	});
	test("pi_result pages bound the real serialized frame, multibyte and all", async () => {
		const ws = track(makeAsyncWorkspace());
		// Each snapshot repeats a multibyte + escape-heavy pattern, so the page
		// budget has to be spent on UTF-8 bytes with the real JSON escaping.
		// Counting code units would pack pages several times over the cap — the
		// text copy alone carries every quote and backslash twice.
		const pattern = '🌸"quoted"\\backslash\nnewline\ttab';
		const PAGE_BYTES = 2048;
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "snapshot_delay",
			FAKE_DELAY_MS: "2000",
			FAKE_SNAPSHOT_CHARS: "400",
			FAKE_SNAPSHOT_TEXT: pattern,
			PI_MCP_ASYNC_PAGE_BYTES: String(PAGE_BYTES),
			PI_MCP_ASYNC_EVENT_TEXT_LIMIT: "512",
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "page me by bytes" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});

		// Drain with a count limit that could take everything at once: the byte
		// cap must be what forces the extra pages.
		const seqs: number[] = [];
		const allEvents: { seq: number; type: string; text: string; truncated?: boolean }[] = [];
		let after: string | undefined;
		let pages = 0;
		let sawFinal = false;
		for (;;) {
			const msg = await client.request("tools/call", {
				name: "pi_result",
				arguments: { execution_id: exec, limit: 50, ...(after ? { after_cursor: after } : {}) },
			}).promise;
			// The frame as it went over the wire: same bytes, same escaping.
			expect(Buffer.byteLength(JSON.stringify(msg), "utf8")).toBeLessThanOrEqual(PAGE_BYTES);
			const payload = msg.result.structuredContent;
			expect(payload.events.length).toBeGreaterThanOrEqual(1);
			for (const event of payload.events) {
				expect(event.seq).toBeGreaterThan(seqs.at(-1) ?? 0);
				seqs.push(event.seq);
				allEvents.push(event);
			}
			pages += 1;
			if (payload.final !== undefined && payload.final !== null) sawFinal = true;
			if (payload.has_more !== true) break;
			// The cursor advances: the next page starts past everything seen.
			after = payload.next_cursor;
			expect(after).toBeDefined();
		}
		expect(pages).toBeGreaterThan(1); // the byte cap did the cutting
		expect(sawFinal).toBe(true); // the final rides the pages it fits on
		// The multibyte survived the byte-accurate clip, and the clip is marked.
		const snapshot = allEvents.find((event) => event.type === "snapshot");
		expect(snapshot?.text).toContain("🌸");
		expect(snapshot?.truncated).toBe(true);
	});
	test("admission reserves the encoded record the accepted jobs will grow into", async () => {
		const ws = track(makeAsyncWorkspace());
		// The STORE_CAP bounds what the executions directory may hold on disk.
		// Each accepted job grows into a serialized final of multibyte +
		// escape-heavy text (UTF-8 bytes plus JSON escaping, twice the raw
		// character count), and the terminal commit writes the grown record
		// twice — tmp file plus the renamed one — so admission reserves two
		// growth bounds per live job: ring + final cap + scaffolding, doubled.
		const STORE_CAP = 2_000_000;
		const pad = '🌸"\\\n\t';
		const client = new Client({
			...ws.asyncEnv,
			FAKE_MODE: "big",
			FAKE_SIZE: "200000",
			FAKE_FINAL_PAD: pad,
			PI_MCP_ASYNC_MAX_FINAL_BYTES: "250000",
			PI_MCP_ASYNC_MAX_EXEC_EVENT_BYTES: "5000",
			PI_MCP_ASYNC_MAX_STORE_BYTES: String(STORE_CAP),
			// Wide enough pages to read the clipped finals back in one call;
			// the page budget clips oversized finals instead of exempting them.
			PI_MCP_ASYNC_PAGE_BYTES: "1000000",
		});
		cleanup.push(() => client.close());

		// Three fit (3 × 2 × ~259KB ≈ 1.55MB); the fourth would spend past the
		// cap before doing any work.
		const execs: string[] = [];
		for (let i = 0; i < 3; i++) {
			const res = await client.tool("pi_start", { prompt: `grow ${i}` });
			execs.push(res.structured.execution_id);
		}
		expectErrorCode(await client.tool("pi_start", { prompt: "one too many" }), "queue_full");

		// Everything accepted still lands: every terminal commits inside the
		// cap, its final clipped to the serialized byte cap and marked.
		for (const exec of execs) {
			await waitFor("succeeded", async () => {
				const res = await client.tool("pi_status", { execution_id: exec });
				return res.structured?.state === "succeeded";
			});
		}
		for (const exec of execs) {
			const result = await client.tool("pi_result", { execution_id: exec });
			const answer = result.structured.final.answer;
			expect(answer.truncated).toBe(true); // the serialized cap did the cutting
			expect(answer.text.length).toBeGreaterThan(0);
			expect(answer.text.length).toBeLessThan(200002);
			expect(answer.text).toContain("🌸");
			// The clip never split a surrogate pair.
			expect(Buffer.from(answer.text, "utf8").toString("utf8")).toBe(answer.text);
		}
		// Actual store usage — every byte under the executions directory, tmp
		// writes included — stays within the documented bound.
		const onDisk = storeBytesOnDisk(ws.stateDir);
		expect(onDisk).toBeGreaterThan(0);
		expect(onDisk).toBeLessThanOrEqual(STORE_CAP);
	});
	test("a client can disconnect and reconnect against the same supervisor", async () => {
		const ws = track(makeAsyncWorkspace());
		const gateway = new Gateway(
			{
				...ws.asyncEnv,
				FAKE_RPC_SETTLE_MS: "3000",
				FAKE_STARTED_FILE: startedFile(ws),
			},
			ws.dir,
			join(ws.dir, "gateway.sock"),
		);
		cleanup.push(() => gateway.close());
		await gateway.start();

		// Client A starts the execution, then drops the connection. The
		// gateway — not the client — owns the server's stdin, so the supervisor
		// and its child keep running.
		const a = await gateway.connect();
		const start = await a.tool("pi_start", { prompt: "outlive my client", idempotency_key: "reconnect-1" });
		const exec: string = start.structured.execution_id;
		const supervisorPid = gateway.child.pid;
		a.close();
		await sleep(500);

		// Client B reconnects: same supervisor process (no restart, no
		// re-spawn), same store, and the execution kept its state across the
		// disconnect.
		const b = await gateway.connect();
		const status = await b.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).not.toBe("interrupted");
		expect(startedCount(ws)).toBe(1);

		await waitFor("succeeded after reconnect", async () => {
			const res = await b.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		const result = await b.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.answer.text).toBe("RPC ANSWER");

		// The retry through the reconnect also resolves by key.
		const retry = await b.tool("pi_start", { prompt: "outlive my client", idempotency_key: "reconnect-1" });
		expect(retry.structured.execution_id).toBe(exec);
		expect(gateway.child.pid).toBe(supervisorPid);
		expect(startedCount(ws)).toBe(1);
	});
	test("the completion hook fires once, with the terminal record already durable", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		const hook = makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog));
		const env = { ...ws.asyncEnv, PI_MCP_COMPLETION_HOOK: hook, FAKE_STARTED_FILE: startedFile(ws) };
		const client = new Client(env);
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "notify me" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		// The hook saw the terminal record on disk: the commit (fsync, rename,
		// directory fsync) was durable before the invocation, exactly once.
		await waitFor("the hook to fire", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);

		// Reads, waits, pages and repeated cancels never re-invoke it.
		for (const name of ["pi_status", "pi_wait", "pi_result", "pi_cancel", "pi_cancel"]) {
			await client.tool(name, { execution_id: exec, ...(name === "pi_wait" ? { timeout_ms: 0 } : {}) });
		}
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);

		// A restart over the same store loads the already-terminal record and
		// stays silent: loading is not a transition. The first supervisor's exit
		// is the barrier — its shutdown waits out every in-flight hook before
		// the process goes away, so a missed invocation cannot hide behind it.
		client.close();
		await new Promise<void>((resolve) => client.child.on("exit", () => resolve()));
		await waitFor("the store lock to be released", () => !existsSync(join(ws.stateDir, "lock")));
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);

		const clientB = new Client(env);
		cleanup.push(() => clientB.close());
		const status = await clientB.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("succeeded");
		clientB.close();
		await new Promise<void>((resolve) => clientB.child.on("exit", () => resolve()));
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);
	});

	test("a failed execution fires the hook once", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog)),
			PI_MCP_ABORT_GRACE_MS: "0",
			FAKE_MODE: "exit_unsettled",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "fail me" });
		const exec: string = start.structured.execution_id;
		await waitFor("failed", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "failed";
		});
		await waitFor("the hook to fire", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} failed`]);
	});

	test("a cancelled execution fires the hook once, repeated cancels included", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog)),
			FAKE_MODE: "hang", // rpc answers abort with a settled turn
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "cancel me" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});

		const cancelled = await client.tool("pi_cancel", { execution_id: exec });
		expect(cancelled.structured.state).toBe("cancelled");
		await waitFor("the hook to fire", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} cancelled`]);

		// Idempotent cancel on the terminal record: still one invocation.
		const again = await client.tool("pi_cancel", { execution_id: exec });
		expect(again.structured.state).toBe("cancelled");
		expect(hookLines(hookLog)).toEqual([`${exec} cancelled`]);
	});

	test("startup recovery fires the hook with interrupted, once, without a live pi", async () => {
		const stateDir = join(tmpdir(), "pi-cli-mcp-async-hook-recovery");
		rmSync(stateDir, { recursive: true, force: true });
		mkdirSync(stateDir, { recursive: true });
		cleanup.push(() => rmSync(stateDir, { recursive: true, force: true }));
		const hookLog = join(stateDir, "hook.log");
		const startedLog = join(stateDir, "started.log");
		const env: Record<string, string> = {
			PI_MCP_BIN: makeFakeBin(stateDir),
			PI_MCP_STATE: join(stateDir, "sessions.json"),
			PI_MCP_ASYNC_STATE_DIR: join(stateDir, "state"),
			PI_MCP_COMPLETION_HOOK: makeHook(stateDir, durableRecordHook(join(stateDir, "state"), hookLog)),
			// The spawn gate parks the job after its `running` commit and before
			// any process exists, so the crashing supervisor below leaves nothing
			// behind: the record is durable and nonterminal, no pi ever spawned.
			PI_MCP_ASYNC_TEST_SPAWN_GATE: join(stateDir, "never-created-gate"),
			FAKE_STARTED_FILE: startedLog,
		};

		// Supervisor A dies outright (SIGKILL) with the record parked at
		// `running`: no notification happened, no child was ever spawned.
		const clientA = new Client(env, stateDir);
		const start = await clientA.tool("pi_start", { prompt: "outlive the supervisor" });
		const exec: string = start.structured.execution_id;
		await waitFor("running", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running";
		});
		expect(startedCountOf(stateDir)).toBe(0);
		clientA.child.kill("SIGKILL");
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));
		expect(hookLines(hookLog)).toEqual([]);
		expect(startedCountOf(stateDir)).toBe(0);

		// The documented crash path: the lock survives, the operator clears it.
		rmSync(join(stateDir, "state", "lock"), { recursive: true, force: true });
		const clientB = new Client(env, stateDir);
		cleanup.push(() => clientB.close());
		const status = await clientB.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("interrupted");
		// Reconciliation's interrupted commit is a qualifying transition: it
		// fires exactly once, with the record already durable.
		await waitFor("the hook to fire", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} interrupted`]);

		// And reads after recovery never re-invoke it.
		await clientB.tool("pi_result", { execution_id: exec });
		expect(hookLines(hookLog)).toEqual([`${exec} interrupted`]);
	});

	test("no hook is invoked when the opt-in is unset or not an absolute path", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		// An executable that logs on entry exists in the workspace; only the
		// server's gating can keep it from running.
		makeHook(ws.dir, `echo "$1 $2" >> "${hookLog}"\n`);

		const plain = new Client({ ...ws.asyncEnv, FAKE_STARTED_FILE: startedFile(ws) });
		const start = await plain.tool("pi_start", { prompt: "no hook configured" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await plain.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		// The supervisor's exit is the deterministic barrier: its shutdown
		// waits out every in-flight hook group before the process goes away,
		// so an erroneous invocation would have logged by now.
		plain.close();
		await new Promise<void>((resolve) => plain.child.on("exit", () => resolve()));
		expect(hookLines(hookLog)).toEqual([]);

		const relative = new Client({ ...ws.asyncEnv, PI_MCP_COMPLETION_HOOK: "notify-hook.sh" });
		cleanup.push(() => relative.close());
		const start2 = await relative.tool("pi_start", { prompt: "relative path is refused" });
		const exec2: string = start2.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await relative.tool("pi_status", { execution_id: exec2 });
			return res.structured?.state === "succeeded";
		});
		relative.close();
		await new Promise<void>((resolve) => relative.child.on("exit", () => resolve()));
		expect(hookLines(hookLog)).toEqual([]);
	});

	test.each([{ point: "before_rename" }, { point: "after_rename" }])(
		"a terminal commit that fails ($point) never invokes the hook",
		async ({ point }) => {
			const ws = track(makeAsyncWorkspace());
			const hookLog = join(ws.dir, "hook.log");
			const env = {
				...ws.asyncEnv,
				PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog)),
				PI_MCP_ASYNC_TEST_FAIL_STATE: "succeeded",
				PI_MCP_ASYNC_TEST_FAIL_POINT: point,
			};
			const client = new Client(env);
			cleanup.push(() => client.close());

			const start = await client.tool("pi_start", { prompt: "fail the terminal commit" });
			const exec: string = start.structured.execution_id;
			await waitFor("the store to fail closed", async () => {
				const res = await client.tool("pi_status", { execution_id: exec });
				return res.structured?.error?.code === "store_unavailable";
			});
			// A state that was never accepted as committed — nothing is
			// notified, not even in the after_rename case where the bytes are
			// already on disk. The exit barrier rules out a late spawn.
			client.close();
			await new Promise<void>((resolve) => client.child.on("exit", () => resolve()));
			expect(hookLines(hookLog)).toEqual([]);

			// Restart over the same store. before_rename: the record is still
			// nonterminal, so recovery reconciles it to `interrupted` — a real
			// qualifying transition, and exactly one notification. after_rename:
			// the record loads already terminal, so startup stays silent forever.
			await waitFor("the store lock to be released", () => !existsSync(join(ws.stateDir, "lock")));
			const clientB = new Client(env);
			cleanup.push(() => clientB.close());
			const status = await clientB.tool("pi_status", { execution_id: exec });
			clientB.close();
			await new Promise<void>((resolve) => clientB.child.on("exit", () => resolve()));
			if (point === "before_rename") {
				expect(status.structured.state).toBe("interrupted");
				await waitFor("the recovery notification", () => hookLines(hookLog).length === 1);
				expect(hookLines(hookLog)).toEqual([`${exec} interrupted`]);
			} else {
				expect(status.structured.state).toBe("succeeded");
				expect(hookLines(hookLog)).toEqual([]);
			}
		},
	);

	test("a hanging hook with a TERM-immune descendant never blocks retrieval, and the timeout reaps its whole group", async () => {
		const ws = track(makeAsyncWorkspace());
		const spawnLog = join(ws.dir, "hook-spawn.log");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, hangingHook(spawnLog)),
			PI_MCP_COMPLETION_HOOK_TIMEOUT_MS: "400",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "hang the hook" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		await waitFor("the hook to spawn", () => hookLines(spawnLog).length === 1);
		const hookPid = hookPidOf(spawnLog);
		expect(pidAlive(hookPid)).toBe(true);
		expect(hookSurvivors()).not.toBe("");

		// The hanging hook never delays a tool result: retrieval answers
		// while the wrapper is still running.
		const began = Date.now();
		const status = await client.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("succeeded");
		expect(Date.now() - began).toBeLessThan(1000);
		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.answer.text).toBe("FINAL ANSWER");

		// The timeout escalates: the whole group — the wrapper and its
		// TERM-immune delivery descendant — is gone, and the record stays
		// retrievable afterwards.
		await waitFor("the hook group to be reaped", () => !pidAlive(hookPid) && hookSurvivors() === "");
		const after = await client.tool("pi_result", { execution_id: exec });
		expect(after.structured.state).toBe("succeeded");
		expect(hookLines(spawnLog)).toHaveLength(1);
	});

	test("shutdown force-reaps an in-flight hook and its descendants, leaving no orphans", async () => {
		const ws = track(makeAsyncWorkspace());
		const spawnLog = join(ws.dir, "hook-spawn.log");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, hangingHook(spawnLog)),
			// Only shutdown may reap it: the hook's own timeout is far away.
			PI_MCP_COMPLETION_HOOK_TIMEOUT_MS: "60000",
			PI_MCP_KILL_GRACE_MS: "500",
			FAKE_STARTED_FILE: startedFile(ws),
		});

		const start = await client.tool("pi_start", { prompt: "outlive the notification" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		await waitFor("the hook to spawn", () => hookLines(spawnLog).length === 1);
		const hookPid = hookPidOf(spawnLog);
		expect(pidAlive(hookPid)).toBe(true);
		expect(hookSurvivors()).not.toBe("");

		// The supervisor goes away the way a gateway teardown does: the signal
		// arrives, then the pipes close and nothing else holds the loop.
		const began = Date.now();
		client.child.kill("SIGTERM");
		client.child.stdin.end();
		client.child.stdout.destroy();
		client.child.stderr.destroy();
		await new Promise<void>((resolve) => client.child.on("exit", () => resolve()));

		// Bounded: the hooks-only shutdown tail is the kill grace, and the
		// exit already implies the force-reap has run.
		// The exit itself is the boundary: the shutdown tail exits only after
		// the force-reaped hook groups are confirmed empty (or says so on
		// stderr and exits anyway), so nothing needs polling past the exit.
		expect(Date.now() - began).toBeLessThan(10_000);
		expect(hookSurvivors()).toBe("");
		expect(pidAlive(hookPid)).toBe(false);
	});

	test("mixed shutdown: parking's own hook and a live pi tree are both confirmed at exit", async () => {
		const ws = track(makeAsyncWorkspace());
		const spawnLog = join(ws.dir, "hook-spawn.log");
		const env: Record<string, string> = {
			...ws.asyncEnv,
			// A pi tree is mid-turn at shutdown: it gets parked, never settled.
			FAKE_MODE: "hang",
			FAKE_CHILD_TAG: CHILD_TAG,
			FAKE_STARTED_FILE: startedFile(ws),
			// The park commit itself spawns the interrupted hook, which hangs
			// with a TERM-immune tagged descendant: only the mixed shutdown
			// reap and its group-empty confirmation can clear it.
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, hangingHook(spawnLog)),
			PI_MCP_COMPLETION_HOOK_TIMEOUT_MS: "60000",
			PI_MCP_KILL_GRACE_MS: "500",
		};

		const client = new Client(env, ws.dir);
		cleanup.push(() => client.close());
		const start = await client.tool("pi_start", { prompt: "hold the tree" });
		const exec: string = start.structured.execution_id;
		await waitFor("running with a live pi tree", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "running" && survivors() !== "";
		});

		// The park has not committed yet: nothing terminal, no hook fired.
		expect(hookLines(spawnLog)).toEqual([]);

		// SIGTERM: the park commits the interrupted record — a qualifying
		// transition, so the hook is invoked from the shutdown path itself —
		// while the pi tree is still alive. The exit must be gated on BOTH
		// proofs, not on the trees' verdict alone.
		const began = Date.now();
		client.child.kill("SIGTERM");
		client.child.stdin.end();
		client.child.stdout.destroy();
		client.child.stderr.destroy();
		await new Promise<void>((resolve) => client.child.on("exit", () => resolve()));

		// The exit is the boundary: the store lock was handed over only after
		// the pi trees AND the parking hook's group were confirmed empty —
		// direct assertions, no polling past an exit that already gated on
		// both confirmations.
		expect(Date.now() - began).toBeLessThan(10_000);
		expect(survivors()).toBe("");
		expect(hookSurvivors()).toBe("");
		expect(existsSync(join(ws.stateDir, "lock"))).toBe(false);
		// And the park really did fire the hook, with the state it committed.
		expect(hookLines(spawnLog).some((line) => line.endsWith(`${exec} interrupted`))).toBe(true);
	});

	test("a same-state retention commit on a terminal record never re-fires the hook", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog)),
			FAKE_MODE: "snapshot_delay",
			FAKE_DELAY_MS: "500",
			PI_MCP_ASYNC_FLUSH_MS: "150",
			// Partial retention expires a millisecond after finish, so the
			// very next lookup performs a real same-state retention commit.
			PI_MCP_ASYNC_PARTIAL_RETENTION_MS: "1",
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "expire my events" });
		const exec: string = start.structured.execution_id;
		// While the run is live, events are committed and readable (no
		// expiry applies to a nonterminal record).
		await waitFor("committed events while running", async () => {
			const res = await client.tool("pi_result", { execution_id: exec });
			return res.structured?.state === "running" && (res.structured?.events ?? []).length > 0;
		});
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		await waitFor("the hook to fire", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);

		// The first lookup after finish commits the retention expiry — events
		// dropped, same terminal state — a real store mutation, but not a
		// transition: the hook count must not move.
		await waitFor("the events to expire", async () => {
			const res = await client.tool("pi_result", { execution_id: exec });
			return (res.structured?.events ?? []).length === 0;
		});
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);
	});
	test("a failed retention commit fails the store closed: typed refusal, no dispatch, nothing uncommitted", async () => {
		const ws = track(makeAsyncWorkspace());
		const gate = join(ws.dir, "spawn-gate");
		const client = new Client({
			...ws.asyncEnv,
			// The window elapses a second after finish, so the next lookup is a
			// real retention mutation — and the injected failure lands on it.
			PI_MCP_ASYNC_PARTIAL_RETENTION_MS: "1000",
			PI_MCP_ASYNC_TEST_FAIL_RETENTION: "1",
			PI_MCP_ASYNC_TEST_SPAWN_GATE: gate,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => rmSync(gate, { force: true }));

		// Job 1 runs through the open gate and settles; job 2 parks at the gate
		// as a live running execution with no child yet.
		writeFileSync(gate, "go\n");
		const first = await client.tool("pi_start", { prompt: "will expire under a failing store" });
		const exec1: string = first.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec1 });
			return res.structured?.state === "succeeded";
		});
		expect(startedCount(ws)).toBe(1);
		rmSync(gate, { force: true });
		const second = await client.tool("pi_start", { prompt: "live when the store dies", idempotency_key: "live-2" });
		const exec2: string = second.structured.execution_id;
		await waitFor("running before spawn", async () => {
			const res = await client.tool("pi_status", { execution_id: exec2 });
			return res.structured?.state === "running";
		});
		expect(startedCount(ws)).toBe(1);

		// The retention window elapses; the triggering lookup fails closed and
		// typed — no stale record, and never an expiry result that was never
		// committed.
		await sleep(1200);
		expectErrorCode(await client.tool("pi_result", { execution_id: exec1 }), "store_unavailable");
		expectErrorCode(await client.tool("pi_status", { execution_id: exec1 }), "store_unavailable");

		// No further dispatch: every entry point refuses, and the live job's
		// cancelled token means opening the spawn window spawns nothing.
		expectErrorCode(await client.tool("pi_status", { execution_id: exec2 }), "store_unavailable");
		expectErrorCode(await client.tool("pi_start", { prompt: "no work on a dead store" }), "store_unavailable");
		writeFileSync(gate, "go\n");
		await sleep(400);
		expect(startedCount(ws)).toBe(1);

		// The refused expiry was never committed: the record on disk still
		// carries its events and its final, exactly as it settled.
		const raw = JSON.parse(readFileSync(join(ws.stateDir, "executions", `${exec1}.json`), "utf8"));
		expect(raw.state).toBe("succeeded");
		expect(raw.events.length).toBeGreaterThan(0);
		expect(raw.finals_expired).toBeUndefined();
	});

	test("a failed retention commit while a pi child is mid-flight reaps it and refuses everything", async () => {
		const ws = track(makeAsyncWorkspace());
		const retentionEnv: Record<string, string> = {
			PI_MCP_ASYNC_PARTIAL_RETENTION_MS: "4000",
			PI_MCP_ASYNC_TEST_FAIL_RETENTION: "1",
			PI_MCP_KILL_GRACE_MS: "250",
			FAKE_STARTED_FILE: startedFile(ws),
		};

		// Phase A: a fast fake settles job 1, starting the retention clock that
		// phase B will cross with a live child on another execution.
		const clientA = new Client({ ...ws.asyncEnv, ...retentionEnv });
		const first = await clientA.tool("pi_start", { prompt: "will expire under a failing store" });
		const exec1: string = first.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await clientA.tool("pi_status", { execution_id: exec1 });
			return res.structured?.state === "succeeded";
		});
		expect(startedCount(ws)).toBe(1);
		clientA.child.kill("SIGTERM");
		await new Promise<void>((resolve) => clientA.child.on("exit", () => resolve()));

		// Phase B: fresh supervisor on the same state, slow fake, injection armed.
		// Its boot and admission sweeps run well inside job 1's retention window,
		// so the store opens clean and job 2 gets a genuinely mid-flight child.
		const clientB = new Client({ ...ws.asyncEnv, ...retentionEnv, FAKE_RPC_SETTLE_MS: "60000" });
		const second = await clientB.tool("pi_start", { prompt: "mid-flight child when the store dies" });
		const exec2: string = second.structured.execution_id;
		await waitFor("running", async () => {
			const res = await clientB.tool("pi_status", { execution_id: exec2 });
			return res.structured?.state === "running";
		});
		await waitFor("second fake pi spawned", () => startedCount(ws) === 2);
		const runningPid = Number(readFileSync(startedFile(ws), "utf8").trim().split("\n").pop());
		expect(pidAlive(runningPid)).toBe(true);

		// The retention window elapses with the child live: the triggering
		// lookup fails closed and typed, and the cancelled token reaps the
		// running pi process instead of leaving it behind.
		await sleep(4500);
		expectErrorCode(await clientB.tool("pi_result", { execution_id: exec1 }), "store_unavailable");
		await waitFor("running child reaped", () => !pidAlive(runningPid));

		// No further dispatch: every entry point refuses the dead store.
		expectErrorCode(await clientB.tool("pi_status", { execution_id: exec2 }), "store_unavailable");
		expectErrorCode(await clientB.tool("pi_start", { prompt: "no work on a dead store" }), "store_unavailable");
		await sleep(400);
		expect(startedCount(ws)).toBe(2);

		// Nothing uncommitted was served either way: the refused expiry left
		// job 1 exactly as it settled, and job 2's ambiguous end was never
		// written as a terminal it did not reach durably.
		const raw1 = JSON.parse(readFileSync(join(ws.stateDir, "executions", `${exec1}.json`), "utf8"));
		expect(raw1.state).toBe("succeeded");
		expect(raw1.events.length).toBeGreaterThan(0);
		expect(raw1.finals_expired).toBeUndefined();
		const raw2 = JSON.parse(readFileSync(join(ws.stateDir, "executions", `${exec2}.json`), "utf8"));
		expect(raw2.state).toBe("running");
	});

	test("a failing hook logs a bounded status, never its output, and blocks nothing", async () => {
		const ws = track(makeAsyncWorkspace());
		// A wrapper that fails loudly: nonzero exit with a sentinel on stderr,
		// shaped like a credential-bearing delivery target.
		const hook = makeHook(
			ws.dir,
			`
echo "SECRET-HOOK-OUTPUT apprise://secret-token@example/bogus" >&2
exit 7
`,
		);
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: hook,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());
		let serverLog = "";
		client.child.stderr.setEncoding("utf8");
		client.child.stderr.on("data", (chunk: string) => {
			serverLog += chunk;
		});

		const start = await client.tool("pi_start", { prompt: "fail the delivery" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		// Deterministic barrier: the close handler writes the bounded status
		// line as soon as the wrapper's nonzero exit is observed.
		await waitFor("the bounded failure status", () => serverLog.includes("exited with code 7"));
		expect(serverLog).toContain(`(${exec} succeeded)`);
		// Never the wrapper's output, never the target it would have used,
		// never the configured hook path itself.
		expect(serverLog).not.toContain("SECRET-HOOK-OUTPUT");
		expect(serverLog).not.toContain("apprise://");
		expect(serverLog).not.toContain(hook);
		// And the failure blocks nothing: the result is fully retrievable.
		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.isError).toBe(false);
		expect(result.structured.final.answer.text).toBe("FINAL ANSWER");
	});

	test("a wrapper that exits while its delivery descendant lingers is swept and confirmed gone", async () => {
		const ws = track(makeAsyncWorkspace());
		const spawnLog = join(ws.dir, "hook-spawn.log");
		// The Apprise-shaped case with the opposite shape: the wrapper hands
		// off and exits 0 at once, leaving the delivery child running.
		const hook = makeHook(
			ws.dir,
			`
echo "$$ $1 $2" >> "${spawnLog}"
sh -c "sleep 600; true # ${HOOK_TAG}" &
exit 0
`,
		);
		const client = new Client({
			...ws.asyncEnv,
			PI_MCP_COMPLETION_HOOK: hook,
			FAKE_STARTED_FILE: startedFile(ws),
		});
		cleanup.push(() => client.close());

		const start = await client.tool("pi_start", { prompt: "leave a descendant behind" });
		const exec: string = start.structured.execution_id;
		await waitFor("succeeded", async () => {
			const res = await client.tool("pi_status", { execution_id: exec });
			return res.structured?.state === "succeeded";
		});
		await waitFor("the hook to spawn", () => hookLines(spawnLog).length === 1);
		const hookPid = hookPidOf(spawnLog);
		await waitFor("the wrapper to exit", () => !pidAlive(hookPid));

		// The wrapper is gone; its delivery descendant lingers, and retrieval
		// is unaffected while it does.
		expect(hookSurvivors()).not.toBe("");
		const result = await client.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.answer.text).toBe("FINAL ANSWER");

		// The deferred sweep escalates to a group SIGKILL and only forgets the
		// run on confirmed group-empty: the descendant is gone, bounded.
		await waitFor("the lingering descendant to be swept", () => hookSurvivors() === "");
	});

	test("the hook fires once while a gateway holds the supervisor across a client disconnect", async () => {
		const ws = track(makeAsyncWorkspace());
		const hookLog = join(ws.dir, "hook.log");
		const gateway = new Gateway(
			{ ...ws.asyncEnv, PI_MCP_COMPLETION_HOOK: makeHook(ws.dir, durableRecordHook(ws.stateDir, hookLog)) },
			ws.dir,
			join(ws.dir, "gateway.sock"),
		);
		cleanup.push(() => gateway.close());
		await gateway.start();

		// The client that started the work drops the connection; the gateway,
		// not the client, owns the supervisor's stdin.
		const a = await gateway.connect();
		const start = await a.tool("pi_start", { prompt: "outlive my client" });
		const exec: string = start.structured.execution_id;
		a.close();

		// Nobody is attached when the turn settles. The hook log line is the
		// deterministic barrier: it is written by the wrapper itself, proving
		// the notification happened with no client connected at all.
		await waitFor("the hook to fire with nobody attached", () => hookLines(hookLog).length === 1);
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);

		// A reconnecting client sees the settled execution, and reads do not
		// re-invoke anything.
		const b = await gateway.connect();
		const status = await b.tool("pi_status", { execution_id: exec });
		expect(status.structured.state).toBe("succeeded");
		const result = await b.tool("pi_result", { execution_id: exec });
		expect(result.structured.final.answer.text).toBe("FINAL ANSWER");
		expect(hookLines(hookLog)).toEqual([`${exec} succeeded`]);
	});

	// --- completion hook (opt-in PI_MCP_COMPLETION_HOOK) --------------------
});

function startedCountOf(dir: string): number {
	const file = join(dir, "started.log");
	if (!existsSync(file)) return 0;
	return readFileSync(file, "utf8").trim().split("\n").filter(Boolean).length;
}
