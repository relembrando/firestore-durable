import type {
    DocumentReference,
    Firestore,
    Transaction,
} from "firebase-admin/firestore";

/**
 * Once-only markers for external events (webhook deliveries, inbound
 * messages): one document per event id records that the event was handled, and
 * a redelivery that finds it is reported as a duplicate. Two modes, because the
 * app has two honest shapes of "exactly once":
 *
 * - {@link Ledger.applyOnce}: the event's whole effect is Firestore writes that
 *   fit in ONE transaction. The marker is created in that same transaction, so
 *   effect and marker commit together or not at all. This is the only mode
 *   that is exactly-once by itself.
 * - {@link Ledger.runAndRecord}: the effect cannot run inside a transaction
 *   (provider API calls, its own transactions, task enqueues) and is itself
 *   idempotent. The marker is written AFTER the work succeeds and only
 *   short-circuits later redeliveries. It is at-least-once: two concurrent
 *   deliveries can both run the work, and a crash after the work but before
 *   the marker re-runs it on the retry. Correctness comes from the work's own
 *   idempotency, never from the marker.
 *
 * Invariants:
 * - `applyOnce` creates the marker with `tx.create`, never get-then-set: the
 *   in-transaction read only skips the work early, and the create is what
 *   makes a concurrent commit of the same id fail the whole transaction
 *   (effect included) instead of applying twice. That failure surfaces as
 *   ALREADY_EXISTS and is reported as `duplicate` once the marker is confirmed
 *   to exist.
 * - `applyOnce`'s `fn` follows the transaction rules: Firestore may run it more
 *   than once, so it must not have effects outside `tx`.
 * - A thrown `fn` leaves no marker in either mode, so the platform's retry
 *   (a 5xx back to the provider, a Cloud Tasks retry) runs the event again.
 * - Every marker carries the TTL field (`ttl.field`, a `Date` of `now +
 *   ttl.ms`; Firestore stores it as a Timestamp) so the ledger stays bounded.
 *   `ttl.ms` must outlast the provider's redelivery window, or a late
 *   redelivery finds no marker and runs again. The TTL policy itself is
 *   configured outside this code (a `firestore.indexes.json` field override).
 * - Adopting a hand-rolled ledger keeps its collection, id and field names, so
 *   markers written by the code being replaced keep deduping across a rolling
 *   deploy: any existing document at the id is a duplicate, whatever its shape.
 */

/** Injectable effects, so tests (and other runtimes) control time. */
export interface LedgerRuntime {
    now: () => number;
}

export interface LedgerConfig {
    db: Firestore;
    /** The marker document for an event id. */
    at: (id: string) => DocumentReference;
    /** The TTL field every marker carries and how far ahead it points. */
    ttl: { field: string; ms: number };
    runtime?: Partial<LedgerRuntime>;
}

export interface MarkerOptions {
    /**
     * Extra fields stored on the marker (e.g. the event type, a server
     * timestamp). The TTL field is added by the ledger and wins over a field of
     * the same name.
     */
    fields?: Record<string, unknown>;
}

export type LedgerOutcome<T> = { status: "applied"; value: T } | { status: "duplicate" };

export interface Ledger {
    /** Run `fn` and create the marker in ONE transaction; see the header. */
    applyOnce<T>(id: string, fn: (tx: Transaction) => Promise<T>, options?: MarkerOptions): Promise<LedgerOutcome<T>>;
    /** Run idempotent `fn` unless marked, then write the marker; see the header. */
    runAndRecord<T>(id: string, fn: () => Promise<T>, options?: MarkerOptions): Promise<LedgerOutcome<T>>;
}

const defaultRuntime: LedgerRuntime = {
    now: () => Date.now(),
};

function isAlreadyExists(err: unknown): boolean {
    const code = (err as { code?: number | string } | null)?.code;
    return code === 6 || code === "already-exists";
}

export function defineLedger(config: LedgerConfig): Ledger {
    const { db, at, ttl } = config;
    if (!ttl.field) {
        throw new Error("defineLedger: ttl.field must name the TTL field");
    }
    if (!Number.isFinite(ttl.ms) || ttl.ms <= 0) {
        throw new Error(`defineLedger: ttl.ms must be a positive number, got ${ttl.ms}`);
    }
    const runtime: LedgerRuntime = { ...defaultRuntime, ...config.runtime };

    const marker = (options: MarkerOptions | undefined): Record<string, unknown> => ({
        ...options?.fields,
        [ttl.field]: new Date(runtime.now() + ttl.ms),
    });

    return {
        async applyOnce(id, fn, options) {
            const ref = at(id);
            try {
                return await db.runTransaction(async (tx) => {
                    const seen = await tx.get(ref);
                    if (seen.exists) return { status: "duplicate" as const };
                    const value = await fn(tx);
                    tx.create(ref, marker(options));
                    return { status: "applied" as const, value };
                });
            } catch (err) {
                // A concurrent delivery committed the marker between our read
                // and our commit. Confirm it is OUR marker that collided, not
                // a create inside `fn`, before calling it a duplicate.
                if (isAlreadyExists(err) && (await ref.get()).exists) {
                    return { status: "duplicate" };
                }
                throw err;
            }
        },

        async runAndRecord(id, fn, options) {
            const ref = at(id);
            if ((await ref.get()).exists) return { status: "duplicate" };
            const value = await fn();
            // `set`, not `create`: a concurrent delivery may have finished the
            // same idempotent work and written the marker first, which is not
            // a failure of this one.
            await ref.set(marker(options));
            return { status: "applied", value };
        },
    };
}
