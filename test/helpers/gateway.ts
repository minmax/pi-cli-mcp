// A stand-in for what an MCP gateway (MCP Proxy and friends) does for a stdio
// server: it owns the server process and lets clients come and go over a unix
// socket, so a client disconnect never closes the server's stdin. The async
// supervisor is exactly the feature whose reconnect story needs this proven
// locally, without any cloud account in front of it.

import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { createServer, type Server, Socket, type Socket as SocketType } from "node:net";
import { SERVER } from "./client.ts";

/** Node 22 needs a flag to run TypeScript; from 23 it is on by default. */
const NODE_TS_FLAGS = Number(process.versions.node.split(".")[0] ?? 0) >= 23 ? [] : ["--experimental-strip-types"];

interface JsonRpcResponse {
	id?: unknown;
	result?: any;
	error?: { code: number; message: string };
}

/** One client connection through the gateway; same call/tool surface as Client. */
export class SocketClient {
	private readonly pending = new Map<number, (msg: JsonRpcResponse) => void>();
	private readonly socket: SocketType;
	private nextId = 1;

	constructor(socket: SocketType) {
		this.socket = socket;
		socket.setEncoding("utf8");
		let buffer = "";
		socket.on("data", (chunk: string) => {
			buffer += chunk;
			let newline = buffer.indexOf("\n");
			while (newline !== -1) {
				const line = buffer.slice(0, newline).trim();
				buffer = buffer.slice(newline + 1);
				newline = buffer.indexOf("\n");
				if (!line) continue;
				const msg = JSON.parse(line) as JsonRpcResponse;
				if (typeof msg.id === "number") {
					const resolver = this.pending.get(msg.id);
					if (resolver) {
						this.pending.delete(msg.id);
						resolver(msg);
					}
				}
			}
		});
	}

	send(msg: unknown): void {
		this.socket.write(`${JSON.stringify(msg)}\n`);
	}

	async call(method: string, params?: unknown): Promise<JsonRpcResponse> {
		const id = this.nextId++;
		const promise = new Promise<JsonRpcResponse>((resolve) => this.pending.set(id, resolve));
		this.send({ jsonrpc: "2.0", id, method, params });
		return promise;
	}

	async tool(name: string, args: unknown = {}): Promise<{ text: string; structured: any; isError: boolean }> {
		const res = await this.call("tools/call", { name, arguments: args });
		return {
			text: res.result?.content?.[0]?.text ?? "",
			structured: res.result?.structuredContent,
			isError: res.result?.isError === true,
		};
	}

	/** Disconnect without touching the server's stdin. */
	close(): void {
		this.socket.destroy();
	}
}

export class Gateway {
	readonly child: ChildProcessWithoutNullStreams;
	private readonly server: Server;
	private current: SocketType | null = null;
	private readonly socketPath: string;

	constructor(env: Record<string, string>, cwd: string, socketPath: string) {
		this.socketPath = socketPath;
		this.child = spawn(process.execPath, [...NODE_TS_FLAGS, SERVER], {
			cwd,
			env: { ...process.env, ...env },
			stdio: ["pipe", "pipe", "pipe"],
		}) as ChildProcessWithoutNullStreams;
		this.child.stderr.pipe(process.stderr);

		// Route the server's stdout to whichever client is connected; with none,
		// lines are dropped — the server never sees stdin close either way.
		let buffer = "";
		this.child.stdout.setEncoding("utf8");
		this.child.stdout.on("data", (chunk: string) => {
			buffer += chunk;
			let newline = buffer.indexOf("\n");
			while (newline !== -1) {
				const line = buffer.slice(0, newline).trim();
				buffer = buffer.slice(newline + 1);
				newline = buffer.indexOf("\n");
				if (line && this.current !== null && !this.current.destroyed) {
					this.current.write(`${line}\n`);
				}
			}
		});

		this.server = createServer((socket) => {
			this.current?.destroy();
			this.current = socket;
			socket.setEncoding("utf8");
			// The client's frames go to the server's stdin; the connection — not
			// the client — owns that stdin, which is the whole point of a gateway.
			socket.on("data", (chunk: string) => {
				this.child.stdin.write(chunk);
			});
		});
	}

	async start(): Promise<void> {
		await new Promise<void>((resolve) => this.server.listen(this.socketPath, resolve));
	}

	async connect(): Promise<SocketClient> {
		const socket = await new Promise<Socket>((resolve, reject) => {
			const s = new Socket();
			s.on("connect", () => resolve(s));
			s.on("error", reject);
			s.connect(this.socketPath);
		});
		return new SocketClient(socket);
	}

	close(): void {
		this.current?.destroy();
		this.server.close();
		this.child.kill("SIGTERM");
	}
}
