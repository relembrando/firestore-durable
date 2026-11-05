import { randomUUID } from "crypto";
import type {
    DocumentData,
    DocumentReference,
    Firestore,
    Query,
    Transaction,
} from "firebase-admin/firestore";

/**
 * A transactional outbox: the producer records an item in the SAME
 * transaction as the domain change that implies it, and a dispatcher delivers
 * it later to an external system (an HTTP API) with retries, without any
 * business transaction ever waiting on the network.
 *
 * One item is one document. Its lifecycle lives in fixed fields:
 *
 *     state          "pending" while waiting, "sending" while claimed, or a
 *                    caller state (terminal or parked)
 *     attempts       claims so far (a claim is a delivery attempt)
 *     nextAttemptAt  when the item is next due; absent = never again
 *     leaseToken     the current claim's fencing token; absent = unclaimed
 *     createdAt      when `put` created it
 *     <ttl.field>    when Firestore's TTL policy may delete it
 *
 * A dispatch is three steps:
 * 1. Claim (one transaction): the item must exist and be due
 *    (`nextAttemptAt <= now`). The caller's `claim` hook reads whatever else
 *    it needs in the same transaction and either decides the item without
 *    delivering (an {@link OutboxOutcome}) or claims it. A claim writes
 *    `state: "sending"`, a fresh `leaseToken`, `attempts + 1` and
 *    `nextAttemptAt = now + leaseMs`.
 * 2. Deliver (no transaction): the caller's `deliver` hook does the network
 *    call and returns an {@link OutboxOutcome}.
 * 3. Finalize (one transaction): the outcome is written only if the item
 *    still carries this claim's `leaseToken`.
 *
 * Invariants:
 * - The lease IS `nextAttemptAt`: a claimed item becomes due again `leaseMs`
 *   after the claim. A dispatcher that crashes (or throws from `deliver`)
 *   therefore needs no cleanup; the next sweep reclaims the item. `leaseMs`
 *   must outlive `deliver` (its HTTP timeout plus the hooks' reads), or a
 *   second claim can start a second delivery while the first is in flight.
 * - The fence compares `leaseToken`, never the time: a finalize whose token was
 *   replaced (a later claim) or removed (a claim-time decision by another
 *   dispatcher) writes nothing and reports `"lost"`. So two dispatchers,
 *   including an old and a new release during a rolling deploy, never both
 *   commit an outcome for one claim.
 * - Every write the outbox makes after a claim-time read clears `leaseToken`
 *   (a decision) or replaces it (a claim), so a slow holder of a lapsed lease
 *   is fenced out by ANY later transition, not only by a later claim.
 * - At-least-once, not exactly-once: the external call can succeed and the
 *   finalize then fail (crash, lost fence), and the item is delivered again
 *   after the lease. Receivers must dedupe on a stable id derived from the
 *   item id.
 * - `put` uses `tx.create`: an item id is written once. Re-materializing the
 *   same id (a replayed trigger, a backstop sweep) fails the producer's
 *   transaction with ALREADY_EXISTS instead of resetting a delivered item;
 *   producers that may replay read the id first and skip.
 * - Max age is enforced at claim: an item whose `maxAge.from(data)` is older
 *   than `maxAge.ms` is never delivered. The `claim` hook sees `ctx.expired`
 *   and may order other decisions before it (e.g. "delete for an account
 *   being deleted" before "mark expired"); if it claims an expired item
 *   anyway, the outbox applies `maxAge.outcome` instead.
 * - Field names, state values and time arithmetic are the wire format shared
 *   with any other deployed code that reads or writes the same collection.
 *   Changing them is a data migration, not a refactor.
 */

/** A patch the caller adds to an outbox write (domain fields, field deletes). */
export type OutboxPatch = Record<string, unknown>;

/** What happens to an item after a decision or a delivery. */
export type OutboxOutcome =
    /** Due again after capped exponential backoff on `attempts`; state "pending". */
    | { kind: "retry"; patch?: OutboxPatch }
    /** Terminal: `state` is set and `nextAttemptAt` removed. */
    | { kind: "settle"; state: string; patch?: OutboxPatch }
    /** Parked in `state` and due again after `afterMs`, attempts unchanged. */
    | { kind: "park"; state: string; afterMs: number; patch?: OutboxPatch }
    /** Delete the item (e.g. its subject no longer exists or was erased). */
    | { kind: "drop" };

/** The claim hook's verdict: decide now, or claim and deliver `value`. */
export type OutboxClaimDecision<C> = OutboxOutcome | { kind: "claim"; value: C; patch?: OutboxPatch };

export interface OutboxClaimContext {
    tx: Transaction;
    data: DocumentData;
    /** True when the item is past `maxAge`; see the header. */
    expired: boolean;
}

export interface OutboxDeliveryContext {
    /** Attempts including this one. */
    attempts: number;
    token: string;
}

export interface OutboxHandlers<C> {
    /** Inside the claim transaction. Reads only (the outbox writes after it). */
    claim: (ctx: OutboxClaimContext) => Promise<OutboxClaimDecision<C>>;
    /** Outside any transaction. Throwing leaves the claim to lapse (a retry). */
    deliver: (value: C, ctx: OutboxDeliveryContext) => Promise<OutboxOutcome>;
}

/**
 * - `missing`: no such item.
 * - `not_due`: not due yet (or already claimed by a live lease).
 * - `decided`: the claim hook decided without delivering.
 * - `delivered`: delivered and the outcome committed under the fence.
 * - `lost`: delivered (or attempted), but the fence refused the outcome.
 */
export type OutboxDispatchResult = "missing" | "not_due" | "decided" | "delivered" | "lost";

/** Injectable effects, so tests (and other runtimes) control time and tokens. */
export interface OutboxRuntime {
    now: () => number;
    newToken: () => string;
}

export interface OutboxConfig {
    db: Firestore;
    /** The document for an item id. */
    at: (id: string) => DocumentReference;
    /** Every item lives here; {@link Outbox.sweep} queries it for due items. */
    items: () => Query;
    /** How long a claim keeps other dispatchers off the item. */
    leaseMs: number;
    /** `retry` waits `min(maxMs, baseMs * 2 ** attempts)`. */
    backoff: { baseMs: number; maxMs: number };
    /** The TTL field `put` stamps and how far ahead it points. */
    ttl: { field: string; ms: number };
    maxAge?: { ms: number; from: (data: DocumentData) => number; outcome: OutboxOutcome };
    /**
     * The SDK's field-delete sentinel (`() => FieldValue.delete()`), injected so
     * this module needs no runtime Firebase import.
     */
    deleteField: () => unknown;
    runtime?: Partial<OutboxRuntime>;
}

export interface PutOptions {
    /** Initial state. Default "pending". */
    state?: string;
    /** Whether the item is due now. Default true; false = recorded, never sent. */
    due?: boolean;
}

export interface SweepOptions {
    /** Due items read per sweep (oldest `nextAttemptAt` first). */
    limit: number;
    /** Items handled at once. */
    concurrency: number;
}

export interface Outbox {
    /** Create the item inside the producer's transaction; see the header. */
    put(tx: Transaction, id: string, fields: DocumentData, options?: PutOptions): void;
    /** Claim, deliver and finalize one item; see the header. */
    dispatch<C>(id: string, handlers: OutboxHandlers<C>): Promise<OutboxDispatchResult>;
    /**
     * Read up to `limit` due items and run `fn` on each with bounded
     * concurrency. Rejects like `Promise.all` if any `fn` does. Returns the
     * number of items read.
     */
    sweep(options: SweepOptions, fn: (id: string) => Promise<void>): Promise<number>;
    /** The `retry` delay after `attempts` attempts. */
    backoffMs(attempts: number): number;
}

const defaultRuntime: OutboxRuntime = {
    now: () => Date.now(),
    newToken: () => randomUUID(),
};

/** Firestore Timestamp, JS Date or epoch ms. */
function toMillis(value: unknown): number {
    if (value && typeof (value as { toMillis?: unknown }).toMillis === "function") {
        return (value as { toMillis: () => number }).toMillis();
    }
    return value instanceof Date ? value.getTime() : Number(value);
}

function positive(name: string, value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
        throw new Error(`defineOutbox: ${name} must be a positive number, got ${value}`);
    }
}

export function defineOutbox(config: OutboxConfig): Outbox {
    const { db, at, leaseMs, backoff, ttl, maxAge, deleteField } = config;
    positive("leaseMs", leaseMs);
    positive("backoff.baseMs", backoff.baseMs);
    positive("backoff.maxMs", backoff.maxMs);
    positive("ttl.ms", ttl.ms);
    if (!ttl.field) throw new Error("defineOutbox: ttl.field must name the TTL field");
    if (maxAge) positive("maxAge.ms", maxAge.ms);
    const runtime: OutboxRuntime = { ...defaultRuntime, ...config.runtime };

    // The exponent is clamped only to keep the product finite; any clamp above
    // log2(maxMs / baseMs) gives the same delays.
    const backoffMs = (attempts: number): number =>
        Math.min(backoff.maxMs, backoff.baseMs * 2 ** Math.min(Math.max(attempts, 0), 52));

    /** The write an outcome makes. `null` = delete the item. */
    const writeFor = (outcome: OutboxOutcome, attempts: number, nowMs: number): Record<string, unknown> | null => {
        switch (outcome.kind) {
            case "drop":
                return null;
            case "retry":
                return { ...outcome.patch, state: "pending", leaseToken: deleteField(),
                    nextAttemptAt: new Date(nowMs + backoffMs(attempts)) };
            case "settle":
                return { ...outcome.patch, state: outcome.state, leaseToken: deleteField(), nextAttemptAt: deleteField() };
            case "park":
                return { ...outcome.patch, state: outcome.state, leaseToken: deleteField(),
                    nextAttemptAt: new Date(nowMs + outcome.afterMs) };
        }
    };

    const apply = (tx: Transaction, ref: DocumentReference, outcome: OutboxOutcome, attempts: number, nowMs: number) => {
        const write = writeFor(outcome, attempts, nowMs);
        if (write === null) tx.delete(ref);
        else tx.update(ref, write);
    };

    return {
        backoffMs,

        put(tx, id, fields, options = {}) {
            const nowMs = runtime.now();
            const due = options.due ?? true;
            tx.create(at(id), {
                ...fields,
                state: options.state ?? "pending",
                attempts: 0,
                [ttl.field]: new Date(nowMs + ttl.ms),
                createdAt: new Date(nowMs),
                ...(due ? { nextAttemptAt: new Date(nowMs) } : {}),
            });
        },

        async dispatch<C>(id: string, handlers: OutboxHandlers<C>): Promise<OutboxDispatchResult> {
            const ref = at(id);
            const token = runtime.newToken();
            type Claimed = { value: C; attempts: number };
            const claim = await db.runTransaction(async (tx): Promise<Claimed | "missing" | "not_due" | "decided"> => {
                const snap = await tx.get(ref);
                if (!snap.exists) return "missing";
                const data = snap.data() ?? {};
                const nowMs = runtime.now();
                if (data.nextAttemptAt === undefined || data.nextAttemptAt === null ||
                    toMillis(data.nextAttemptAt) > nowMs) return "not_due";
                const attempts = Number(data.attempts) || 0;
                const expired = maxAge !== undefined && maxAge.from(data) < nowMs - maxAge.ms;
                let decision = await handlers.claim({ tx, data, expired });
                if (decision.kind === "claim" && expired) decision = maxAge!.outcome;
                if (decision.kind !== "claim") {
                    apply(tx, ref, decision, attempts, nowMs);
                    return "decided";
                }
                tx.update(ref, { ...decision.patch, state: "sending", leaseToken: token,
                    attempts: attempts + 1, nextAttemptAt: new Date(nowMs + leaseMs) });
                return { value: decision.value, attempts: attempts + 1 };
            });
            if (typeof claim === "string") return claim;

            const outcome = await handlers.deliver(claim.value, { attempts: claim.attempts, token });

            return db.runTransaction(async (tx) => {
                const snap = await tx.get(ref);
                if (!snap.exists || snap.data()?.leaseToken !== token) return "lost" as const;
                apply(tx, ref, outcome, claim.attempts, runtime.now());
                return "delivered" as const;
            });
        },

        async sweep({ limit, concurrency }, fn) {
            const due = await config.items()
                .where("nextAttemptAt", "<=", new Date(runtime.now()))
                .orderBy("nextAttemptAt")
                .limit(limit)
                .get();
            const ids = due.docs.map((doc) => doc.id);
            let next = 0;
            await Promise.all(Array.from({ length: Math.max(1, concurrency) }, async () => {
                while (next < ids.length) await fn(ids[next++]);
            }));
            return ids.length;
        },
    };
}
