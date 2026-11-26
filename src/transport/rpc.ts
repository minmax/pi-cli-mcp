// `pi --mode rpc`: pi stays up and reads JSONL commands on stdin.
//
// The turn is driven by three protocol facts, all from pi's rpc-types.ts:
//   - `{type:"prompt", message}` starts it;
//   - the event stream is the same one `--mode json` emits;
//   - `agent_settled` marks the end of the turn, and closing stdin shuts pi down.
//
// While the turn runs it is registered as live, so a caller can send a message
// into it. Sending is always the caller's decision — nothing is sent from here.

import type { RpcCommand, RpcResponse } from "@earendil-works/pi-coding-agent";
import { asRecord } from "../parse.ts";
import type { PiHandle, RunResult } from "../pi-process.ts";
import { runPi } from "../pi-process.ts";
import type { CallContext } from "../types.ts";
import { overrideArgs } from "./args.ts";
import { registerRun } from "./registry.ts";
import type { RunPlan, Transport } from "./types.ts";

// One initial command per child; correlate only its response, not a later
// command from pi_send. A missing acknowledgement does not prove non-execution.
const PROMPT_ACK = {
	id: "pi-mcp-initial-prompt",
	type: "response",
	command: "prompt",
	success: true,
} as const satisfies Pick<RpcResponse, "id" | "type" | "command" | "success">;

export const rpcTransport: Transport = {
	name: "rpc",
	acceptsMidRunMessages: true,

	async run(plan: RunPlan, ctx: CallContext, onEvent: (event: unknown) => void): Promise<RunResult> {
		// The prompt travels inside a command, so neither argv size nor a leading
		// dash is a problem here — no temp file needed.
		const args = ["--mode", "rpc", "--session-id", plan.sessionId, ...overrideArgs(plan.overrides)];

		let handle: PiHandle | undefined;
		let unregister = (): void => {};
		let settled = false;
		let rpcPromptAcknowledged = false;

		return runPi(args, plan.cwd, {
			token: ctx.token,
			timeoutMs: plan.timeoutMs,
			stdin: "pipe",
			// A deadline or a cancellation ends the turn with pi's own `abort` first.
			// Signalling straight away costs the tail of the stream: pi deliberately
			// skips its stdout flush on SIGTERM, so the answer it was writing can be
			// lost. After `abort` the turn closes through the normal path and the
			// events arrive; SIGTERM/SIGKILL still follow if it does not.
			gracefulStop: () => {
				handle?.send({ type: "abort" });
			},
			onTreeGone: ctx.onTreeGone,
			onStart: (piHandle) => {
				handle = piHandle;
				unregister = registerRun({
					sessionId: plan.sessionId,
					cwd: plan.cwd,
					startedAt: Date.now(),
					handle: piHandle,
				});
				piHandle.send({ id: PROMPT_ACK.id, type: "prompt", message: plan.prompt } satisfies RpcCommand);
			},
			onEvent: (event) => {
				if (!rpcPromptAcknowledged) {
					const response = asRecord(event);
					rpcPromptAcknowledged =
						response?.id === PROMPT_ACK.id &&
						response.type === PROMPT_ACK.type &&
						response.command === PROMPT_ACK.command &&
						response.success === PROMPT_ACK.success;
				}
				onEvent(event);
				// `agent_settled` is pi's end-of-turn marker. Closing stdin is how its
				// rpc mode is asked to exit; without it the process would sit idle
				// until the hard deadline.
				if (!settled && isSettled(event)) {
					settled = true;
					unregister();
					handle?.endInput();
				}
			},
		})
			.then((result) => ({ ...result, rpcPromptAcknowledged }))
			.finally(() => unregister());
	},
};

function isSettled(event: unknown): boolean {
	if (typeof event !== "object" || event === null) return false;
	return (event as { type?: unknown }).type === "agent_settled";
}
