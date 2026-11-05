import { randomUUID } from "crypto";
import { setTimeout as sleepFor } from "timers/promises";
import type {
    DocumentReference,
    DocumentSnapshot,
    Firestore,
    Transaction,
} from "firebase-admin/firestore";

/**
 * A lease with a fencing token on a Firestore document: at most one holder per
 * key at a time, a crashed holder's lease lapses after `ttlMs`, and every
 * write made through {@link HeldLease.tx} commits only while the lease is
 * still this acquisition's.
 *
 * Invariants:
 * - Acquire and release are short transactions; the lease is held ACROSS the
 *   caller's work, so external calls (HTTP, LLM, payment APIs) never run
 *   inside a transaction.
 * - `ttlMs` must outlive the caller's whole operation (normally: exceed the
 *   function timeout, since the platform kills the handler at its timeout).
 *   If it does not, a second holder can acquire while the first still runs;
 *   the fence then makes the first holder's later `tx()` fail with
 *   {@link LeaseLostError} instead of committing behind the second one.
 * - The fence compares the per-acquisition `token`, not the expiry: an expired
 *   lease nobody took over is still exclusive, so committing through it is
 *   safe.
 * - Expiry is wall-clock (`runtime.now`, default `Date.now`) compared across
 *   instances. A contender whose clock runs AHEAD of the holder's sees the
 *   lease expire early by that skew, so size `ttlMs` as the longest operation
 *   plus the worst skew. There is deliberately no "expiry too far in the
 *   future" check: under skew it would read a live lease as free.
 * - A redelivered task carrying the same {@link LeaseOwner} `id` with a HIGHER
 *   `epoch` (e.g. the Cloud Tasks retry count) takes a live lease over: the
 *   platform only retries after the previous attempt failed. The same epoch
 *   while live is a duplicate delivery and is busy like any other owner.
 * - An acquire transaction that Firestore gives up on under contention
 *   (gRPC ABORTED after its own retries) counts as a busy attempt: it is
 *   retried within `wait` and surfaces as {@link LeaseBusyError}, never as a
 *   raw Firestore error, so callers handle one "try later" outcome.
 * - Adopting a hand-rolled lease ({@link LegacyLeaseFormat}): during a rolling
 *   deploy old and new code hold the SAME key, so the new code must honour the
 *   old shape as busy and keep writing it alongside its own. Drop `legacy`
 *   once no deployed code reads the old shape.
 * - Release keeps the document with a null holder, one shape for "free" whatever
 *   the history. With `deleteOnRelease` it deletes the document instead, for a key
 *   that names a person or anything else that should not outlive its use: a
 *   document then exists only while an acquisition runs, or after a holder
 *   crashed (release is best effort). A missing document is free everywhere, so
 *   mixed releases still exclude each other.
 */

/** What a lease document (or lease field) stores while held. */
export interface LeaseState {
    holder: string;
    epoch: number;
    token: string;
    expiresAtMs: number;
}

/** Who is acquiring. Defaults to a fresh random id with epoch 0. */
export interface LeaseOwner {
    id: string;
    epoch: number;
}

/**
 * The shape a replaced implementation stored, so both can share one key while
 * a deploy rolls out. `read` gets the raw lease value (the document data, or
 * the field value in `field` mode) and returns a live-looking legacy holder,
 * which blocks acquisition until it expires and can never be taken over or
 * fenced through. `write` returns the legacy fields to store next to the
 * lease (called with `null` on release) so the old code sees it as held. With
 * `deleteOnRelease` the release writes nothing, so the old code must read a
 * missing document as free.
 */
export interface LegacyLeaseFormat {
    read: (raw: unknown) => { holder: string; expiresAtMs: number } | undefined;
    write: (state: LeaseState | null) => Record<string, unknown>;
}

/** Injectable effects, so tests (and other runtimes) control time and ids. */
export interface LeaseRuntime {
    now: () => number;
    sleep: (ms: number) => Promise<void>;
    newToken: () => string;
}

export interface LeaseConfig<K> {
    db: Firestore;
    /** The document that holds the lease for `key`. */
    at: (key: K) => DocumentReference;
    /**
     * Keep the lease in this top-level field of an EXISTING document (e.g. the
     * story being processed) instead of owning the whole document. The
     * document must exist; acquiring against a missing one throws
     * {@link LeaseTargetMissingError}.
     */
    field?: string;
    ttlMs: number;
    /** Release deletes the document. Document mode only: a `field` lease does not own its document. */
    deleteOnRelease?: boolean;
    legacy?: LegacyLeaseFormat;
    runtime?: Partial<LeaseRuntime>;
}

export interface RunOptions {
    owner?: LeaseOwner;
    /** Retry while another owner holds a live lease. Default: one attempt. */
    wait?: { attempts: number; backoffMs: number };
}

export interface HeldLease {
    readonly path: string;
    readonly token: string;
    readonly owner: LeaseOwner;
    /**
     * Run a transaction that commits only while this acquisition still holds
     * the lease. `snapshot` is the lease document as read inside the
     * transaction (in `field` mode, the domain document itself).
     */
    tx<T>(fn: (tx: Transaction, snapshot: DocumentSnapshot) => Promise<T>): Promise<T>;
}

export interface Lease<K> {
    run<T>(key: K, fn: (lease: HeldLease) => Promise<T>, options?: RunOptions): Promise<T>;
}

export class LeaseBusyError extends Error {
    constructor(readonly path: string, readonly holder: string) {
        super(`Lease ${path} is held by ${holder}`);
        this.name = "LeaseBusyError";
    }
}

export class LeaseLostError extends Error {
    constructor(readonly path: string) {
        super(`Lease ${path} is no longer held by this acquisition`);
        this.name = "LeaseLostError";
    }
}

export class LeaseTargetMissingError extends Error {
    constructor(readonly path: string) {
        super(`Lease target ${path} does not exist`);
        this.name = "LeaseTargetMissingError";
    }
}

/** Holder reported when acquisition lost to transaction contention. */
export const CONTENDED_HOLDER = "(contended)";

// gRPC ABORTED, as the Admin SDK reports it (numeric) or as a string code.
function isContention(err: unknown): boolean {
    const code = (err as { code?: unknown } | null)?.code;
    return code === 10 || code === "aborted" || code === "ABORTED";
}

const defaultRuntime: LeaseRuntime = {
    now: () => Date.now(),
    sleep: (ms) => sleepFor(ms).then(() => undefined),
    newToken: () => randomUUID(),
};

function decode(raw: unknown): LeaseState | undefined {
    if (!raw || typeof raw !== "object") return undefined;
    const s = raw as Record<string, unknown>;
    if (
        typeof s.holder !== "string" ||
        typeof s.epoch !== "number" ||
        typeof s.token !== "string" ||
        typeof s.expiresAtMs !== "number"
    ) {
        return undefined;
    }
    return { holder: s.holder, epoch: s.epoch, token: s.token, expiresAtMs: s.expiresAtMs };
}

export function defineLease<K>(config: LeaseConfig<K>): Lease<K> {
    const { db, at, field, ttlMs, legacy, deleteOnRelease } = config;
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
        throw new Error(`defineLease: ttlMs must be a positive number, got ${ttlMs}`);
    }
    if (deleteOnRelease && field !== undefined) {
        throw new Error("defineLease: deleteOnRelease needs document mode, a field lease does not own its document");
    }
    const runtime: LeaseRuntime = { ...defaultRuntime, ...config.runtime };

    const rawOf = (snap: DocumentSnapshot): unknown => (field === undefined ? snap.data() : snap.get(field));

    const read = (snap: DocumentSnapshot): LeaseState | undefined => decode(rawOf(snap));

    const isLive = (expiresAtMs: number, nowMs: number): boolean => expiresAtMs > nowMs;

    const write = (tx: Transaction, ref: DocumentReference, state: LeaseState | null): void => {
        const extra = legacy?.write(state) ?? {};
        if (field === undefined) {
            tx.set(ref, { ...(state ?? { holder: null }), ...extra });
        } else {
            const hasExtra = Object.keys(extra).length > 0;
            tx.update(ref, { [field]: state ? { ...state, ...extra } : hasExtra ? { holder: null, ...extra } : null });
        }
    };

    async function tryAcquire(
        ref: DocumentReference,
        owner: LeaseOwner,
        token: string,
    ): Promise<{ acquired: true } | { acquired: false; holder: string }> {
        return db.runTransaction(async (tx) => {
            const snap = await tx.get(ref);
            if (field !== undefined && !snap.exists) throw new LeaseTargetMissingError(ref.path);
            const nowMs = runtime.now();
            const held = read(snap);
            if (held && isLive(held.expiresAtMs, nowMs)) {
                const retryOfHolder = held.holder === owner.id && owner.epoch > held.epoch;
                if (!retryOfHolder) return { acquired: false as const, holder: held.holder };
            } else if (!held && legacy) {
                const old = legacy.read(rawOf(snap));
                if (old && isLive(old.expiresAtMs, nowMs)) return { acquired: false as const, holder: old.holder };
            }
            write(tx, ref, { holder: owner.id, epoch: owner.epoch, token, expiresAtMs: nowMs + ttlMs });
            return { acquired: true as const };
        });
    }

    async function release(ref: DocumentReference, token: string): Promise<void> {
        await db.runTransaction(async (tx) => {
            const snap = await tx.get(ref);
            if (!snap.exists || read(snap)?.token !== token) return;
            if (deleteOnRelease) tx.delete(ref);
            else write(tx, ref, null);
        });
    }

    return {
        async run(key, fn, options = {}) {
            const ref = at(key);
            const owner = options.owner ?? { id: runtime.newToken(), epoch: 0 };
            const token = runtime.newToken();
            const attempts = Math.max(1, options.wait?.attempts ?? 1);

            for (let attempt = 1; ; attempt++) {
                const result = await tryAcquire(ref, owner, token).catch((err: unknown) => {
                    if (isContention(err)) return { acquired: false as const, holder: CONTENDED_HOLDER };
                    throw err;
                });
                if (result.acquired) break;
                if (attempt >= attempts) throw new LeaseBusyError(ref.path, result.holder);
                await runtime.sleep(options.wait?.backoffMs ?? 0);
            }

            const held: HeldLease = {
                path: ref.path,
                token,
                owner,
                tx: (work) =>
                    db.runTransaction(async (tx) => {
                        const snap = await tx.get(ref);
                        if (read(snap)?.token !== token) throw new LeaseLostError(ref.path);
                        return work(tx, snap);
                    }),
            };

            try {
                return await fn(held);
            } finally {
                // Best effort: a failed release only delays the next holder
                // until the lease expires, it never lets two hold it at once.
                await release(ref, token).catch(() => undefined);
            }
        },
    };
}
