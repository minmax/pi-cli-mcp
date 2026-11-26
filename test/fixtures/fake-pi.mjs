#!/usr/bin/env node

// A stand-in for the pi binary. Emits a pi-shaped `--mode json` event stream so
// the tests can drive paths a live model cannot produce on demand.
//
// Controlled by env:
//   FAKE_MODE=answer       (default) narration + tool step, then a final answer
//   FAKE_MODE=big          one huge answer, size from FAKE_SIZE
//   FAKE_FINAL_PAD         repeat pattern for the big answer (default "X") —
//                                FAKE_SIZE counts characters of the padded text
//   FAKE_MODE=hang         never settles, so the test can cancel or time it out
//   FAKE_MODE=slow         settles after FAKE_DELAY_MS
//   FAKE_MODE=startup_exit (rpc) exits 127 with stderr before reading a command
//   FAKE_MODE=exit_unsettled / signal_unsettled (rpc) end after prompt ack with no agent_settled
//   FAKE_MODE=timeout_before_ack / wrong_prompt_ack (rpc) no valid prompt acknowledgement
//   FAKE_MODE=snapshot_delay  (rpc) narration + tool step + a provisional snapshot,
//                                 settles FAKE_DELAY_MS later
//   FAKE_SNAPSHOT_CHARS    pads the provisional snapshot to test event clipping
//   FAKE_MODE=empty_final  settles with no text at all
//   FAKE_MODE=no_stop      final message carries no stopReason
//   FAKE_MODE=garbage      non-JSON output, i.e. a broken event contract
//   FAKE_GARBAGE_LINE=1     one non-JSON line mid-stream, valid events on both sides
//   FAKE_GARBAGE_EOF=1      the stream ends on a non-JSON line with no trailing newline
//   FAKE_GARBAGE_EOF_SIZE   pad that tail to N chars (over-long drop path)
//   FAKE_GARBAGE_COMPLETE=1  one complete over-long line mid-stream (newline included)
//   FAKE_GARBAGE_SIZE       its size in chars (default 110000)
//   FAKE_GARBAGE_COMPLETE_SPLIT=1  its newline travels in a second flush, after the head
//   FAKE_MODE=no_newline   last event is EOF-terminated, not newline-terminated
//   FAKE_MODE=overlap      detects concurrent runs on one session via a lockfile
//   FAKE_STOP=<reason>     stopReason on the final message (default "stop")
//   FAKE_EXIT=<code>       exit code (default 0)
//   FAKE_STARTED_FILE      appended to with one line per invocation
//
// Note: never call process.exit() after writing — stdout to a pipe is async and
// exiting truncates the stream. Set process.exitCode instead.

import { spawn } from "node:child_process";
import { appendFileSync, existsSync, unlinkSync, writeFileSync } from "node:fs";

const out = (o) => process.stdout.write(`${JSON.stringify(o)}\n`);
const mode = process.env.FAKE_MODE ?? "answer";
const stop = process.env.FAKE_STOP ?? "stop";
const exitCode = Number(process.env.FAKE_EXIT ?? 0);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Set by the scenarios that are supposed to never finish. They return as soon as
 * their timers are armed, so without this flag the rpc path would treat "the
 * scenario function returned" as "the turn is over" and exit — turning a hang
 * into a clean, instant exit.
 */
let holding = false;

if (process.env.FAKE_STARTED_FILE) {
	appendFileSync(process.env.FAKE_STARTED_FILE, `${process.pid}\n`);
}

// FAKE_SURVIVOR_PID_FILE: a descendant in pi's own process group that closes
// its inherited pipes and ignores SIGTERM. The leader dying says nothing about
// this one — exactly the case where untracking the tree at leader close would
// leave it unreachable by the shutdown SIGKILL.
if (process.env.FAKE_SURVIVOR_PID_FILE) {
	const pidFile = process.env.FAKE_SURVIVOR_PID_FILE;
	spawn("sh", ["-c", `echo $$ > '${pidFile}'; trap '' TERM; exec sleep 300 <&- 1>&- 2>&-`], {
		stdio: "ignore",
	});
}

// FAKE_LINGERING_GROUP_FILE: a member of pi's own process group that no signal
// can remove — a zombie. Signals are no-ops on zombies, so the group cannot be
// confirmed empty until the holder outside it is killed and the zombie is
// reaped. The holder leaves the group (setpgid to its own) but stays in pi's
// session, so its child can join the pi group before exiting; it then never
// waits, which is what keeps the zombie on the books. Deterministic and
// test-controlled: kill the holder's pid and the group empties.
//
// python3 for setpgid — node has no binding for it, and no shell has a builtin.
// First run only: the holder's pid file is the marker, so follow-up runs of
// the same client (which inherit this env) do not each leave their own
// zombie — the test kills the holder before asking for a follow-up.
if (process.env.FAKE_LINGERING_GROUP_FILE && !existsSync(process.env.FAKE_LINGERING_GROUP_FILE)) {
	const pidFile = process.env.FAKE_LINGERING_GROUP_FILE;
	const holder = spawn(
		"python3",
		[
			"-c",
			`import os, sys, time
os.setpgid(0, 0)
pid = os.fork()
if pid == 0:
    os.setpgid(0, ${process.pid})
    os._exit(0)
open(${JSON.stringify(pidFile)}, "w").write(str(os.getpid()))
while True:
    time.sleep(60)
`,
		],
		{ stdio: "ignore" },
	);
	holder.unref();
	// The zombie must be in place before this process exits, or the group id
	// is gone and the child's setpgid has nothing to join. The holder signals
	// readiness by writing the pid file; wait for it, bounded.
	const deadline = Date.now() + 2000;
	while (!existsSync(pidFile) && Date.now() < deadline) {
		await sleep(20);
	}
}

/** Exit only once every stdout write has reached the pipe: a hard
 * process.exit mid-flush truncates the stream, and under concurrent load the
 * reader can fall behind far enough that a 20ms gamble loses a 400KB line. */
function exitWhenDrained(code) {
	// The EOF-tail variant of the corruption probe: whatever exit path runs,
	// the last stdout bytes are one malformed, unterminated line.
	garbageEof();
	process.stdout.write("", () => {
		setTimeout(() => process.exit(code), 0);
	});
}

let garbageEofDone = false;
/** FAKE_GARBAGE_EOF=1: write the unterminated malformed tail exactly once,
 * whether the scenario already ended with it (print mode) or the exit path
 * appends it (rpc mode). FAKE_GARBAGE_EOF_SIZE pads the tail to that many
 * characters, for the over-long-line drop path. */
function garbageEof() {
	if (garbageEofDone) return;
	garbageEofDone = true;
	if (process.env.FAKE_GARBAGE_EOF === "1") {
		const size = Number(process.env.FAKE_GARBAGE_EOF_SIZE ?? 0);
		process.stdout.write(size > 0 ? "x".repeat(size) : "this line is not json");
	}
}

// `--mode rpc`: pi stays up, reads JSONL commands on stdin, and ends the turn
// with agent_settled. Modelled on pi's own rpc-types.ts.
if (process.argv.includes("rpc")) {
	await runRpc();
} else if (process.argv.includes("--list-models")) {
	const filter = process.argv[process.argv.indexOf("--list-models") + 1];
	const rows = [
		"provider  model                 context  max-out  thinking  images",
		"fake      fake/alpha            1.0M     32.8K    yes       no",
		"fake      fake/beta             200K     32.8K    no        no",
	];
	const kept =
		filter && !filter.startsWith("-") ? [rows[0], ...rows.slice(1).filter((r) => r.includes(filter))] : rows;
	process.stdout.write(`${kept.join("\n")}\n`);
	process.exitCode = exitCode;
} else {
	await runAgent();
}

async function runAgent() {
	out({ type: "session", id: "fake-session", cwd: process.cwd() });
	out({ type: "agent_start" });
	await runScenario();
}

async function runScenario() {
	// Real pi prints this on stderr when --session-id names a session it does not
	// have; the server turns it into an explicit warning instead of pretending the
	// conversation continued.
	if (process.env.FAKE_UNKNOWN_SESSION === "1") {
		const idx = process.argv.indexOf("--session-id");
		const id = idx === -1 ? "?" : process.argv[idx + 1];
		process.stderr.write(`Warning: No project session found with id '${id}'; creating a new session with that id.\n`);
	}

	if (mode === "garbage") {
		// A stream that is not the documented contract at all.
		process.stdout.write("this is not json\nneither is this\n");
		process.exitCode = exitCode;
		return;
	}

	if (mode === "overlap") {
		// Concurrency detector: if another instance holds the lock, say so loudly.
		const lock = process.env.FAKE_LOCK_FILE;
		const held = lock && existsSync(lock);
		if (lock && !held) writeFileSync(lock, String(process.pid));
		out({ type: "session", id: "fake-session", cwd: process.cwd() });
		out({ type: "turn_start" });
		await sleep(Number(process.env.FAKE_DELAY_MS ?? 300));
		finalMessage(held ? "OVERLAP DETECTED" : "EXCLUSIVE");
		if (lock && !held) unlinkSync(lock);
		process.exitCode = exitCode;
		return;
	}

	if (mode === "hang") {
		holding = true;
		out({ type: "turn_start" });
		// A child of its own, to assert the whole tree dies with the parent.
		spawnMarkedChild();
		setInterval(() => {}, 1000);
		return;
	}

	if (mode === "hang_after_work") {
		// Did real work and said something, then hangs before settling — the shape
		// of a task killed at the deadline with its result unreported.
		out({ type: "turn_start" });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				stopReason: "toolUse",
				usage: { input: 10, output: 4, cost: { total: 0 } },
				content: [
					{ type: "text", text: "PARTIAL: 43 tests pass, now showing the failure" },
					{ type: "toolCall", id: "c1", name: "write", arguments: { path: "note.md", content: "hi" } },
				],
			},
		});
		out({ type: "tool_execution_start", toolName: "write" });
		out({ type: "tool_execution_end", toolName: "write", isError: false });
		out({ type: "turn_start" });
		holding = true;
		spawnMarkedChild();
		setInterval(() => {}, 1000);
		return;
	}

	if (mode === "noisy_stderr") {
		// A failing run that dumps its session payload to stderr, prompt included.
		const message = (role, text) =>
			JSON.stringify({ type: "message_start", message: { role, content: [{ type: "text", text }] } });
		process.stderr.write(`${message("user", "SECRET_PROMPT_BODY")}\n`);
		process.stderr.write(`${message("assistant", "SECRET_PROMPT_BODY echoed")}\n`);
		process.stderr.write(`${JSON.stringify({ type: "message_end", message: { role: "user" } })}\n`);
		process.stderr.write("Error: upstream refused the request\n");
		process.exitCode = exitCode;
		return;
	}

	if (mode === "slow") {
		out({ type: "turn_start" });
		await sleep(Number(process.env.FAKE_DELAY_MS ?? 1500));
		finalMessage("SLOW ANSWER");
		process.exitCode = exitCode;
		return;
	}

	if (mode === "big") {
		out({ type: "turn_start" });
		// FAKE_FINAL_PAD sets the repeated pattern (multibyte/escape-heavy
		// content for byte-budget tests); FAKE_SIZE stays the character count.
		const pad = process.env.FAKE_FINAL_PAD ?? "X";
		finalMessage(pad.repeat(Math.ceil(Number(process.env.FAKE_SIZE ?? 200_000) / pad.length)));
		// A legal turn settles; the rpc path needs these to call it done.
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "empty_final") {
		// A settled message with no text: the answer is genuinely missing, and an
		// earlier narration must not be promoted into its place.
		out({ type: "turn_start" });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				stopReason: "toolUse",
				usage: { input: 10, output: 2, cost: { total: 0 } },
				content: [
					{ type: "text", text: "NARRATION: about to do it." },
					{ type: "toolCall", id: "c1", name: "read", arguments: { path: "a.txt" } },
				],
			},
		});
		out({ type: "tool_execution_start", toolName: "read" });
		out({ type: "tool_execution_end", toolName: "read", isError: false });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				stopReason: "stop",
				usage: { input: 20, output: 0, cost: { total: 0 } },
				content: [],
			},
		});
		out({ type: "agent_end", willRetry: false });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "child_then_answer") {
		// Leaves a detached grandchild behind and then answers normally — a pi that
		// does not clean up after itself. The server must not rely on it doing so.
		out({ type: "turn_start" });
		spawnMarkedChild();
		finalMessage("ANSWERED WITH CHILD LEFT");
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "empty_completion") {
		// stopReason "stop" with an entirely empty content array: the stream is
		// well-formed and pi settles the turn, the model just said nothing.
		out({ type: "turn_start" });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				stopReason: "stop",
				usage: { input: 4748, output: 0, cost: { total: 0 } },
				content: [],
			},
		});
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "error_turn") {
		// pi's error-termination contract: stopReason "error" plus errorMessage, and
		// no text at all. This is what a failed turn actually looks like.
		out({ type: "turn_start" });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				stopReason: "error",
				errorMessage: process.env.FAKE_ERROR_MESSAGE ?? "context window exceeded: 1048576 > 1000000 tokens",
				diagnostics: [{ type: "provider_error", timestamp: 0, error: { message: "upstream 400" } }],
				usage: { input: 12, output: 0, cost: { total: 0 } },
				content: [],
			},
		});
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "no_stop") {
		out({ type: "turn_start" });
		out({
			type: "message_end",
			message: {
				role: "assistant",
				provider: "fake",
				model: "fake-1",
				usage: { input: 20, output: 5, cost: { total: 0 } },
				content: [{ type: "text", text: "ANSWER WITHOUT STOP REASON" }],
			},
		});
		out({ type: "agent_end", willRetry: false });
		process.exitCode = exitCode;
		return;
	}

	if (mode === "no_newline") {
		out({ type: "turn_start" });
		// Deliberately no trailing newline on the final event.
		process.stdout.write(
			JSON.stringify({
				type: "message_end",
				message: {
					role: "assistant",
					provider: "fake",
					model: "fake-1",
					stopReason: "stop",
					usage: { input: 20, output: 5, cost: { total: 0 } },
					content: [{ type: "text", text: "EOF TERMINATED ANSWER" }],
				},
			}),
		);
		process.exitCode = exitCode;
		return;
	}

	// Default: narration + tool call in one message, then the real answer. This is
	// the shape that makes "last text block" the wrong definition of the answer.
	out({ type: "turn_start" });
	out({
		type: "message_end",
		message: {
			role: "assistant",
			provider: "fake",
			model: "fake-1",
			stopReason: "toolUse",
			usage: { input: 10, output: 2, reasoning: 7, cost: { total: 0 } },
			content: [
				{ type: "text", text: "NARRATION: I'll check that for you." },
				{ type: "toolCall", id: "c1", name: "write", arguments: { path: "note.md", content: "hi" } },
			],
		},
	});
	out({ type: "tool_execution_start", toolName: "write" });
	out({ type: "tool_execution_end", toolName: "write", isError: false });
	if (process.env.FAKE_GARBAGE_LINE === "1") {
		// A nonblank, non-JSON line with valid events on both sides: partial stream
		// corruption. Everything else settles cleanly, so only fail-closed corruption
		// tracking can keep this run from reporting success.
		process.stdout.write("this line is not json\n");
	}
	// The tail of the default scenario. The split over-long variant waits for
	// the oversized head to flush before sending its newline, so everything after
	// it must still parse once the dropped line is behind the reader.
	const finishScenario = () => {
		out({ type: "turn_start" });
		finalMessage("FINAL ANSWER");
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
		// Print mode exits by falling off the end here: the tail lands as the last
		// stdout bytes before the natural exit.
		garbageEof();
		process.exitCode = exitCode;
	};

	if (process.env.FAKE_GARBAGE_COMPLETE === "1") {
		const size = Number(process.env.FAKE_GARBAGE_SIZE ?? 110000);
		if (process.env.FAKE_GARBAGE_COMPLETE_SPLIT === "1") {
			// The oversized head flushes before its newline travels: the reader must
			// drop the head, discard the rest of the line through that newline, and
			// still parse the valid lines that follow.
			process.stdout.write("x".repeat(size - 10), () => {
				setTimeout(() => {
					process.stdout.write("x".repeat(10) + "\n", finishScenario);
				}, 0);
			});
			return;
		}
		// One complete over-long line, newline included: the capture limit
		// applies before parsing, not only to unterminated leftovers.
		process.stdout.write("x".repeat(size) + "\n");
	}
	finishScenario();
}

/**
 * The rpc side of the fake: read JSONL commands and answer them.
 *
 * On `prompt` it plays the same FAKE_MODE scenario as print mode, because real pi
 * emits the same event stream in both. FAKE_RPC_WAIT=1 makes the turn sit and
 * wait for a steer or an abort instead, which is what the mid-run message tests
 * need; FAKE_RPC_SETTLE_MS answers after a delay.
 */
async function runRpc() {
	if (mode === "startup_exit") {
		process.stderr.write("fake private startup stderr\n");
		process.exitCode = 127;
		return;
	}
	out({ type: "session", id: "fake-session", cwd: process.cwd() });
	out({ type: "agent_start" });

	const settleMs = process.env.FAKE_RPC_SETTLE_MS;
	let settled = false;
	const received = [];

	const settle = (text) => {
		if (settled) return;
		settled = true;
		out({ type: "turn_start" });
		finalMessage(text);
		out({ type: "agent_end", willRetry: false });
		out({ type: "agent_settled" });
	};

	let buffer = "";
	process.stdin.setEncoding("utf8");
	process.stdin.on("data", (chunk) => {
		buffer += chunk;
		let nl = buffer.indexOf("\n");
		while (nl !== -1) {
			const line = buffer.slice(0, nl).trim();
			buffer = buffer.slice(nl + 1);
			nl = buffer.indexOf("\n");
			if (!line) continue;

			let cmd;
			try {
				cmd = JSON.parse(line);
			} catch {
				continue;
			}
			received.push(cmd.type);
			if (mode === "timeout_before_ack") continue;
			if (mode === "wrong_prompt_ack" && cmd.type === "prompt") {
				out({ type: "response", command: "prompt", success: true });
				out({ type: "response", id: `${cmd.id}-foreign`, command: "prompt", success: true });
				exitWhenDrained(0);
				return;
			}
			out({ type: "response", id: cmd.id, command: cmd.type, success: true });

			if (cmd.type === "prompt") {
				if (mode === "exit_unsettled" || mode === "signal_unsettled") {
					if (mode === "signal_unsettled") {
						process.stdout.write("", () => process.kill(process.pid, "SIGTERM"));
					} else {
						exitWhenDrained(0);
					}
					return;
				}
				if (mode === "snapshot_delay") {
					// Narration and a tool step land first (they must never reach the
					// projection), then a settled message_end acts as a provisional
					// answer snapshot, and the turn settles only FAKE_DELAY_MS later —
					// the window async polling reads progress and a snapshot before
					// the final.
					out({ type: "turn_start" });
					out({
						type: "message_end",
						message: {
							role: "assistant",
							provider: "fake",
							model: "fake-1",
							stopReason: "toolUse",
							usage: { input: 10, output: 2, cost: { total: 0 } },
							content: [
								{ type: "text", text: "SECRET NARRATION: checking the thing." },
								{ type: "toolCall", id: "c1", name: "read", arguments: { path: "a.txt" } },
							],
						},
					});
					out({ type: "tool_execution_start", toolName: "read" });
					out({ type: "tool_execution_end", toolName: "read", isError: false });
					finalMessage(
						`PARTIAL SNAPSHOT BEFORE SETTLE${(process.env.FAKE_SNAPSHOT_TEXT ?? "x").repeat(
							Number(process.env.FAKE_SNAPSHOT_CHARS ?? 0),
						)}`,
					);
					setTimeout(() => settle("FINAL AFTER SNAPSHOT"), Number(process.env.FAKE_DELAY_MS ?? 2000));
				} else if (settleMs) {
					out({ type: "turn_start" });
					setTimeout(() => settle("RPC ANSWER"), Number(settleMs));
				} else if (process.env.FAKE_RPC_WAIT === "1") {
					out({ type: "turn_start" });
					spawnMarkedChild();
				} else {
					settled = true;
					void runScenario().then(() => {
						// Real pi ends a turn with agent_settled, and the server closes stdin on
						// that event. Scenarios that deliberately break the message contract emit
						// no such event, so the fake exits by itself rather than sit until the
						// deadline — unless it is a scenario whose whole point is to hang. The
						// delay lets stdout flush.
						if (holding) return;
						exitWhenDrained(exitCode);
					});
				}
			} else if (cmd.type === "steer") {
				// A steered turn reports what it was told, so the test can prove the
				// message reached a turn that was already running.
				settle(`STEERED: ${cmd.message}`);
			} else if (cmd.type === "follow_up") {
				settle(`FOLLOW_UP QUEUED: ${cmd.message}`);
			} else if (cmd.type === "abort") {
				// Mirrors pi closely enough for the tests that matter: an aborted turn
				// still reports through the event stream. FAKE_RPC_IGNORE_ABORT makes it
				// unresponsive instead, so the signal fallback can be exercised.
				if (process.env.FAKE_RPC_IGNORE_ABORT === "1") continue;
				settle("ABORTED");
			}
		}
	});
	process.stdin.on("end", () => {
		process.exitCode = exitCode;
		// Mirrors pi: closing stdin is how rpc mode is asked to exit.
		exitWhenDrained(exitCode);
	});
	// Keep the process alive while it waits for commands.
	setInterval(() => {}, 1000);
}

/**
 * A long-lived grandchild, so a test can assert the whole process tree dies.
 * Tagged with FAKE_CHILD_TAG: test files run in parallel, and a bare `sleep 120`
 * would be found by every test's pgrep, not just the one that spawned it.
 */
function spawnMarkedChild() {
	const tag = process.env.FAKE_CHILD_TAG ?? "pi-cli-mcp-child";
	// Two commands, deliberately: with a single one `sh -c` execs it and replaces
	// its own image, which drops the tag from the command line that pgrep -f
	// matches on. The trailing `true` keeps the shell — and the tag — alive.
	spawn("sh", ["-c", `sleep 120; true # ${tag}`], { stdio: "ignore" });
}

function finalMessage(text) {
	out({
		type: "message_end",
		message: {
			role: "assistant",
			provider: "fake",
			model: "fake-1",
			stopReason: stop,
			usage: { input: 20, output: 5, cost: { total: 0.0012 } },
			content: [{ type: "text", text }],
		},
	});
}
