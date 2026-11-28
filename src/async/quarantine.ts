/**
 * Sessions whose previous pi turn ended with an unverifiable process group.
 *
 * The run is over — its execution record is terminal (`failed`, with the
 * unverified-cleanup reason) — but a leftover process may still be alive in
 * the old group. Until the group is confirmed empty, starting a new turn on
 * the session would mean two processes could be editing the same conversation
 * at once. Every entry point that could start a turn on the session — async
 * `pi_start`, sync `pi_reply` — must refuse while the quarantine stands.
 *
 * The quarantine is lifted by the run's tree-gone callback: pi-process keeps
 * probing the group after the fail-closed resolve, and the moment the group is
 * confirmed empty the session is released. If the supervisor itself goes down
 * first, shutdown's escalation owns the group and holds the async store lock
 * until it is confirmed empty, so nothing can hand the session over either.
 */

export interface SessionQuarantine {
	/** The async execution that ended unverified, or null for a sync run. */
	executionId: string | null;
	since: number;
}

const quarantined = new Map<string, SessionQuarantine>();

export function quarantineSession(sessionId: string, executionId: string | null): void {
	if (quarantined.has(sessionId)) return;
	quarantined.set(sessionId, { executionId, since: Date.now() });
}

export function sessionQuarantine(sessionId: string): SessionQuarantine | undefined {
	return quarantined.get(sessionId);
}

/**
 * Release the quarantine. With an execution id, only the run that set the
 * quarantine can lift it — a stale callback from an earlier turn must not
 * release a newer one. (One active turn per session makes the mismatch
 * unreachable today; the guard keeps it unreachable.)
 */
export function releaseQuarantine(sessionId: string, executionId: string | null): boolean {
	const entry = quarantined.get(sessionId);
	if (entry === undefined || entry.executionId !== executionId) return false;
	return quarantined.delete(sessionId);
}
