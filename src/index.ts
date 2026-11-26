#!/usr/bin/env node

// Minimal stdio MCP server that exposes the locally installed `pi` CLI
// (@earendil-works/pi-coding-agent) as a delegatable sub-agent.
//
// No runtime dependencies: newline-delimited JSON-RPC 2.0 is spoken directly, so
// there is no SDK to keep in sync. pi's own types are used at compile time only
// (see src/types.ts), which keeps the wire contract honest without shipping them.

import {
	asyncConfigured,
	asyncInit,
	asyncPark,
	asyncRelease,
	callAsyncTool,
	completionHookGone,
	completionHookInFlight,
	completionHookKill,
	notifyAsyncAbort,
} from "./async/index.ts";
import { ASYNC_TOOLS } from "./async/tools.ts";
import { FALLBACK_PROTOCOL, KILL_GRACE_MS, MAX_FRAME, PROTOCOL_VERSIONS, SERVER_INFO, TREE_GONE_MS } from "./config.ts";
import { killAllTrees, makeCancelToken, treeCount, waitForTreesGone } from "./pi-process.ts";
import { loadSessions } from "./sessions.ts";
import {
	callPi,
	callPiModels,
	callPiReply,
	callPiRunning,
	callPiSend,
	callPiSessions,
	TOOLS,
	toolResult,
} from "./tools.ts";
import type { CallContext, CancelToken, JsonRpcId, JsonRpcMessage, ToolResult } from "./types.ts";

function send(message: unknown): void {
	process.stdout.write(`${JSON.stringify(message)}\n`);
}

function reply(id: JsonRpcId, result: unknown): void {
	send({ jsonrpc: "2.0", id, result });
}

function replyError(id: JsonRpcId, code: number, message: string): void {
	send({ jsonrpc: "2.0", id, error: { code, message } });
}

/**
 * requestId -> cancel token. Registered before the request queues for a slot or
 * a session lock, so a cancellation that arrives while waiting is not lost.
 */
const inFlight = new Map<string, CancelToken>();

/**
 * JSON-RPC ids 1 and "1" are distinct ids; keying by String(id) alone would let
 * them overwrite each other's cancel token.
 */
function requestKey(id: unknown): string {
	return `${typeof id}:${String(id)}`;
}

function makeContext(token: CancelToken, meta: Record<string, unknown> | undefined, replyId: JsonRpcId): CallContext {
	const progressToken = meta?.progressToken;
	let counter = 0;
	if (progressToken === undefined || progressToken === null) return { token, replyId };
	return {
		token,
		replyId,
		progress: (message: string) => {
			counter += 1;
			send({
				jsonrpc: "2.0",
				method: "notifications/progress",
				params: { progressToken, progress: counter, message },
			});
		},
	};
}

async function dispatchTool(
	name: unknown,
	args: Record<string, unknown>,
	ctx: CallContext,
): Promise<ToolResult | null> {
	if (name === "pi") return callPi(args, ctx);
	if (name === "pi_reply") return callPiReply(args, ctx);
	if (name === "pi_models") return callPiModels(args, ctx);
	if (name === "pi_send") {
		const result = callPiSend(args);
		// An async turn aborted through pi_send takes the same cancellation
		// transition as pi_cancel; without this the abort would end the child
		// cleanly and the execution would be misreported as a success.
		if (args.command === "abort" && typeof args.session === "string") notifyAsyncAbort(String(args.session));
		return result;
	}
	if (name === "pi_running") return callPiRunning();
	if (name === "pi_sessions") return callPiSessions();
	if (asyncConfigured()) {
		// Async handlers render their own typed errors; null means "not one of
		// the four async tools", not a failure.
		const result = await callAsyncTool(name, args, ctx.token, ctx.replyId);
		if (result !== null) return result;
	}
	return null;
}

function asRecordParam(value: unknown): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return {};
	return value as Record<string, unknown>;
}

/**
 * A crash in the adapter, not in pi — say so plainly and give a trace that is
 * actually readable: the error class, the message, the frames from our own code,
 * and the cause chain. A one-line `err.stack` dump buries the useful frames
 * under node internals, and swallowing `cause` hides the real failure.
 */
function describeInternalError(err: unknown, tool: string): string {
	const lines = [`pi-mcp internal error while handling \`${tool}\` — this is a bug in the adapter, not in pi.`];

	let current: unknown = err;
	let depth = 0;
	while (current !== null && current !== undefined && depth < 5) {
		const error = current instanceof Error ? current : undefined;
		const label = depth === 0 ? "error" : "caused by";
		if (error === undefined) {
			lines.push(`\n${label}: ${typeof current === "string" ? current : JSON.stringify(current)}`);
			break;
		}

		lines.push(`\n${label}: ${error.name}: ${error.message}`);
		const frames = (error.stack ?? "")
			.split("\n")
			.slice(1)
			.map((line) => line.trim())
			// Our own frames first: node internals and dependency frames say nothing
			// about a defect that lives in this file.
			.filter((line) => line.includes("/pi-cli-mcp/") || line.includes("src/"))
			.slice(0, 8);
		if (frames.length > 0) lines.push(frames.map((f) => `  ${f}`).join("\n"));

		current = error.cause;
		depth += 1;
	}

	return lines.join("\n");
}

async function handle(msg: JsonRpcMessage): Promise<void> {
	// A notification is a request without an `id` member. `id: null` is a
	// (discouraged) real id, not the absence of one.
	const hasId = Object.hasOwn(msg, "id");
	const id = msg.id ?? null;
	const method = msg.method;
	const params = asRecordParam(msg.params);

	if (msg.jsonrpc !== "2.0" || typeof method !== "string") {
		if (hasId) replyError(id, -32600, "Invalid Request");
		return;
	}
	if (hasId && msg.id !== null && typeof msg.id !== "string" && typeof msg.id !== "number") {
		replyError(null, -32600, "Invalid Request: id must be a string, number, or null");
		return;
	}

	switch (method) {
		case "initialize": {
			if (!hasId) return;
			const requested = params.protocolVersion;
			const protocolVersion =
				typeof requested === "string" && (PROTOCOL_VERSIONS as readonly string[]).includes(requested)
					? requested
					: FALLBACK_PROTOCOL;
			reply(id, { protocolVersion, capabilities: { tools: {} }, serverInfo: SERVER_INFO });
			return;
		}

		case "notifications/initialized":
		case "initialized":
			return;

		case "notifications/cancelled": {
			inFlight.get(requestKey(params.requestId))?.cancel();
			return;
		}

		case "ping":
			if (hasId) reply(id, {});
			return;

		case "tools/list":
			if (hasId) {
				// Async tools are appended after the six, never interleaved; with
				// the feature off the list is exactly the original six.
				reply(id, { tools: asyncConfigured() ? [...TOOLS, ...ASYNC_TOOLS] : TOOLS });
			}
			return;

		case "tools/call": {
			if (!hasId) return;
			const token = makeCancelToken();
			const key = requestKey(msg.id);
			inFlight.set(key, token);
			try {
				const ctx = makeContext(token, asRecordParam(params._meta), id);
				const result = await dispatchTool(params.name, asRecordParam(params.arguments), ctx);
				if (result === null) replyError(id, -32602, `Unknown tool: ${String(params.name)}`);
				else reply(id, result);
			} catch (err) {
				reply(id, toolResult(describeInternalError(err, String(params.name)), true));
			} finally {
				inFlight.delete(key);
			}
			return;
		}

		default:
			if (hasId) replyError(id, -32601, `Method not found: ${method}`);
	}
}

// --- shutdown -------------------------------------------------------------

// Shutdown must take the detached process groups with it. They are in their own
// groups precisely so we can kill their whole trees — which also means nothing
// else will ever clean them up if this process just exits.
let shuttingDown = false;

function shutdown(reason: string, code = 0): void {
	if (shuttingDown) return;
	shuttingDown = true;
	// Phase 1: park the async supervisor's uncompleted executions durably and
	// stop everything — but keep the store ownership lock until the pi process
	// groups below are gone, so a supervisor starting inside the cleanup window
	// cannot acquire the store while this one's children may still be running.
	asyncPark();
	const trees = treeCount();
	const hooks = completionHookInFlight();
	if (trees > 0 || hooks > 0) {
		if (trees > 0) {
			process.stderr.write(`pi-mcp: ${reason} — terminating ${trees} running pi process(es)\n`);
			for (const token of inFlight.values()) token.cancel();
			killAllTrees("SIGTERM");
		} else {
			// No pi tree to tear down, but a completion hook is still delivering.
			// It gets the same kill-grace window to finish on its own; only what is
			// still running at the end of it is force-reaped, and the reap counts
			// only when every hook group is confirmed empty (ESRCH on the probe)
			// — a SIGKILL sent is not a SIGKILL confirmed. The poll and
			// confirmation timers stay ref'd: the client's pipes are often already
			// closed by now, and nothing else may hold the event loop — an unref'd
			// wait would let node exit and orphan the detached hook group.
			process.stderr.write(`pi-mcp: ${reason} — waiting out ${hooks} in-flight completion hook(s)\n`);
			const deadline = Date.now() + KILL_GRACE_MS;
			const poll = (): void => {
				if (completionHookInFlight() > 0 && Date.now() < deadline) {
					setTimeout(poll, 50);
					return;
				}
				completionHookKill();
				completionHookGone(TREE_GONE_MS)
					.then((gone) => {
						if (!gone) {
							process.stderr.write(
								"pi-mcp: could not confirm every completion hook group exited; check for leftover hook processes before trusting the last delivery.\n",
							);
						}
						asyncRelease();
						process.exit(code);
					})
					.catch(() => process.exit(code));
			};
			poll();
			return;
		}
		// Give SIGTERM a moment, then make sure nothing is left behind. Sending
		// SIGKILL is not proof either: a group member can outlive the signal by a
		// tick, and a descendant that ignored SIGTERM was unreachable while the
		// leader was alive. Only a confirmed-empty group hands over the store — and
		// if emptiness cannot be confirmed, the lock stays put and the next
		// supervisor is told to remove it by hand once the leftovers are gone.
		//
		// The timer and the tree-gone polls below stay ref'd on purpose: once the
		// client's pipes close (a stdio proxy dying), nothing else holds the event
		// loop, and an unref'd wait would let node exit before the SIGKILL, the
		// group-empty confirmation, and the lock release ever run — orphaning the
		// store lock on every mid-run teardown. The wait stays bounded: this
		// timer plus TREE_GONE_MS in the sweep.
		setTimeout(() => {
			killAllTrees("SIGKILL");
			completionHookKill();
			// Both proofs are awaited together, whichever settles first. The
			// pi trees own the store lock; the hook groups only their own
			// cleanup — and parking above may itself have spawned an
			// `interrupted` hook, so exiting on the trees' verdict alone would
			// strand it. An unconfirmable group of either kind is reported
			// honestly, never assumed; only the trees keep the lock rule.
			Promise.all([waitForTreesGone(TREE_GONE_MS), completionHookGone(TREE_GONE_MS)])
				.then(([treesGone, hooksGone]) => {
					if (treesGone) {
						asyncRelease();
					} else {
						process.stderr.write(
							"pi-mcp: could not confirm every pi process group exited; keeping the async store lock. Remove <state-dir>/lock by hand once the leftover processes are gone.\n",
						);
					}
					if (!hooksGone) {
						process.stderr.write(
							"pi-mcp: could not confirm every completion hook group exited; check for leftover hook processes before trusting the last delivery.\n",
						);
					}
					process.exit(code);
				})
				.catch(() => process.exit(code));
		}, KILL_GRACE_MS);
		return;
	}
	asyncRelease();
	process.exit(code);
}

for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"] as const) {
	process.on(signal, () => shutdown(`received ${signal}`, 0));
}
// A dead stdout means the client is gone; keeping pi alive would strand it.
process.stdout.on("error", (err: NodeJS.ErrnoException) => {
	if (err?.code === "EPIPE") shutdown("stdout closed", 0);
});
// A dead stderr must not crash the process out of its shutdown cleanup: an
// unhandled EPIPE here would exit before the park/kill/release tail runs,
// orphaning the async store lock. The log line is simply lost.
process.stderr.on("error", (err: NodeJS.ErrnoException) => {
	if (err?.code === "EPIPE") return;
	throw err;
});

// --- stdin loop -----------------------------------------------------------

loadSessions();
// Opens the async store when the operator opted in; otherwise a no-op. Failures
// fail that subsystem closed with a stderr note, never the legacy tools.
asyncInit();

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
	buffer += chunk;
	let newline = buffer.indexOf("\n");
	while (newline !== -1) {
		const line = buffer.slice(0, newline).trim();
		buffer = buffer.slice(newline + 1);
		newline = buffer.indexOf("\n");
		if (!line) continue;

		let parsed: unknown;
		try {
			parsed = JSON.parse(line);
		} catch {
			send({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });
			continue;
		}
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
			// Valid JSON, but not a Request Object. Batches are not supported.
			send({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } });
			continue;
		}

		const msg = parsed as JsonRpcMessage;
		handle(msg).catch((err: unknown) => {
			if (Object.hasOwn(msg, "id")) {
				replyError(msg.id ?? null, -32603, `Internal error: ${(err as Error).message ?? String(err)}`);
			}
		});
	}
	// Refuse an unbounded frame rather than growing until the process dies and
	// every pending response is lost with it.
	if (buffer.length > MAX_FRAME) {
		buffer = "";
		send({
			jsonrpc: "2.0",
			id: null,
			error: { code: -32600, message: `Invalid Request: frame exceeded ${MAX_FRAME} chars` },
		});
	}
});
process.stdin.on("end", () => shutdown("stdin closed", 0));
