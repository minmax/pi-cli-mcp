// `pi -p --mode json`: one process per turn, pi exits when the turn is done.

import { EXTRA_ARGS } from "../config.ts";
import type { RunResult } from "../pi-process.ts";
import { runPi } from "../pi-process.ts";
import type { CallContext } from "../types.ts";
import { overrideArgs, promptArgs } from "./args.ts";
import type { RunPlan, Transport } from "./types.ts";

export const printTransport: Transport = {
	name: "print",
	acceptsMidRunMessages: false,

	async run(plan: RunPlan, ctx: CallContext, onEvent: (event: unknown) => void): Promise<RunResult> {
		const { args: tail, cleanup } = promptArgs(plan.prompt);
		// The operator's tail goes straight after -p, never next to the prompt: an
		// unknown extension flag sitting directly before a positional takes that
		// positional as its value, and pi drops it (verified against pi's own
		// parseArgs). After -p the next token is always a flag pi knows.
		const args = [
			"-p",
			...EXTRA_ARGS,
			"--mode",
			"json",
			"--session-id",
			plan.sessionId,
			...overrideArgs(plan.overrides),
			...tail,
		];
		try {
			return await runPi(args, plan.cwd, {
				token: ctx.token,
				timeoutMs: plan.timeoutMs,
				onEvent,
				onTreeGone: ctx.onTreeGone,
			});
		} finally {
			cleanup();
		}
	},
};
