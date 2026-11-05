import { createHash, randomUUID } from "crypto";
import type {
    DocumentData,
    DocumentReference,
    DocumentSnapshot,
    Firestore,
    Transaction,
} from "firebase-admin/firestore";
import { defineLease, LeaseBusyError, LeaseLostError, LeaseTargetMissingError } from "./lease";
import type { HeldLease, LeaseOwner } from "./lease";
import { defineLedger } from "./ledger";
import type { TaskQueue } from "./task_queue";

/**
 * A keyed object (Restate's "virtual object"): a per-key mailbox with at most
 * one active handler per key. Producers `send` messages to a key; a task
 * queue kicks `drain`, which takes the key's lease and runs the handler on
 * the key's messages one at a time, in order.
 *
 * Storage, for an object named `name` (also the task-id prefix):
 *
 *     <name>/{key}                     lastSeq, processedSeq, state, lease
 *     <name>/{key}/mailbox/{seq|latest} one pending message: seq, id, msg,
 *                                       attempts, notBefore, lastError,
 *                                       journal, createdAt, <ttl field>
 *     <name>/{key}/mailboxSent/{hash}  send-id dedupe marker: id, <ttl>
 *     <name>/{key}/mailboxDead/{seq}   dead letters: the message + lastError
 *
 * FIFO messages live at a zero-padded `{seq}`; `latest` mode keeps one
 * document, `latest`, holding only the newest payload.
 *
 * Modes:
 * - `fifo`: every accepted message is handled (or dead-lettered), in `seq`
 *   order. A failing head blocks the key until it succeeds or dead-letters.
 * - `latest`: a send replaces the pending payload; the handler sees the
 *   newest one and older ones are superseded (never handled). A message being
 *   handled when a newer one arrives finishes; the newer one runs next.
 *
 * Invariants:
 * - `send` is ONE transaction (`defineLedger.applyOnce`): the create-only
 *   dedupe marker, the message, and `lastSeq + 1`. Seqs are therefore gap-free
 *   and in commit order. The drain kick is enqueued AFTER the commit, with a
 *   task id derived from (key, seq); a duplicate send while messages are
 *   pending re-kicks the newest one's id, so a send retried after a failed
 *   kick heals.
 * - One active handler per key: `drain` holds the key's lease (field mode on
 *   the object document, owner = the task, epoch = its retry count, so a
 *   platform retry of a crashed drain takes over at once). Contention is
 *   `busy`, not an error: the holder re-reads the mailbox after every message
 *   and, after releasing, re-checks it and kicks again if a send raced the
 *   release (the lost-wakeup window). A refused drain also arms one delayed
 *   kick per lease window, past the expiry, so a holder that vanished without
 *   releasing cannot strand the key.
 * - Every drain-side write (attempt counts, `ctx.tx`, `ctx.once` journal,
 *   seq advance, dead letters) commits through the lease fence: a drain whose
 *   lease was taken over writes nothing more and reports `lost`.
 * - At-least-once per message, not exactly-once: the attempt count is written
 *   BEFORE the handler runs, so a crash counts as an attempt and the message
 *   is redelivered (`ctx.attempt` > 1). There is no deterministic replay;
 *   `ctx.once` journals a step's JSON result on the message so a redelivery
 *   returns it instead of re-running the step (a crash between the step and
 *   its journal write still re-runs it).
 * - `processedSeq` advances in the same commit as `ctx.tx`'s writes, or, if
 *   the handler never calls `ctx.tx`, right after it returns. A handler that
 *   throws after its `ctx.tx` committed has still processed the message; the
 *   drain stops and rethrows (the platform retry continues the backlog).
 * - A throw is a failed attempt: the message gets `notBefore = now +
 *   backoff(attempts)` and a delayed retry kick; drains before then report
 *   `waiting` (and re-arm that kick) instead of retrying early. After
 *   `maxAttempts` the message moves to `mailboxDead`, `onDeadLetter` runs,
 *   and the key continues with the next message.
 * - `leaseMs` must exceed the worker's timeout (the platform kills a drain at
 *   its timeout), and `drainBudget` bounds one drain (fairness across keys,
 *   and staying under that timeout): when spent, the drain re-kicks the key.
 * - Liveness rests on kicks: a key whose every pending kick was lost (queue
 *   retries exhausted, queue purged) waits for its next send with a new id.
 * - Messages, markers and dead letters carry the TTL field; Firestore's TTL
 *   policy (configured outside this code, per collection group) bounds them,
 *   so `inboxTtl.ms` is also the dedupe window and the longest a message may
 *   wait. A message the TTL deleted before it ran is skipped (`onExpired`). The
 *   object document has no TTL by default: deleting it restarts `seq`, and a
 *   reused seq collides with live children and with kick ids Cloud Tasks still
 *   remembers (up to 24 h). An object that must disappear with its data sets
 *   `objectTtl`: a generation then starts its seqs from the clock, so neither
 *   the TTL policy nor `purge` can make a later send reuse one.
 * - Field names, document ids and the kick-id tags are the wire format shared
 *   with every deployed version. Changing them is a data migration.
 */

export type KeyedObjectMode = "fifo" | "latest";

/** The task payload a drain kick carries. */
export interface KeyedObjectKick {
    key: string;
}

/** Injectable effects, so tests (and other runtimes) control time and tokens. */
export interface KeyedObjectRuntime {
    now: () => number;
    newToken: () => string;
}

export interface KeyedDeadLetter<M> {
    key: string;
    seq: number;
    id: string;
    msg: M;
    attempts: number;
    lastError: string;
}

export interface KeyedObjectConfig<M> {
    db: Firestore;
    /** Collection of the object documents, and the kick task-id prefix. */
    name: string;
    mode: KeyedObjectMode;
    /** Delivers drain kicks; the worker calls `drain(payload.key, ...)`. */
    queue: TaskQueue<KeyedObjectKick>;
    /** Lease TTL; must exceed the worker's timeout. */
    leaseMs: number;
    /** Attempts per message before it is dead-lettered. */
    maxAttempts: number;
    /** A failed attempt waits `min(maxMs, baseMs * 2 ** (attempts - 1))`. */
    backoff: { baseMs: number; maxMs: number };
    /** The TTL field on messages, markers and dead letters, and its horizon. */
    inboxTtl: { field: string; ms: number };
    /** Handler runs and wall time one drain may spend before re-kicking. */
    drainBudget: { messages: number; ms: number };
    /** Called after a dead letter commits. Errors are ignored. */
    onDeadLetter?: (letter: KeyedDeadLetter<M>) => void | Promise<void>;
    /** Called when the drain skips past a pending message that is gone. Errors are ignored. */
    onExpired?: (gone: { key: string; seq: number }) => void | Promise<void>;
    /**
     * Keep the object document only this long after its last send (same TTL
     * field, its own TTL policy) and allow `purge`. Must exceed `2 * inboxTtl.ms`,
     * so a message and its dead letter expire first.
     */
    objectTtl?: { ms: number };
    /** The SDK's field-delete sentinel (`() => FieldValue.delete()`). */
    deleteField: () => unknown;
    runtime?: Partial<KeyedObjectRuntime>;
}

export interface SendOptions {
    /** Dedupe id: a second send with the same id (within `inboxTtl`) is a duplicate. */
    id: string;
    /** Enqueue the drain kick after the commit. Default true. */
    kick?: boolean;
}

export type SendOutcome = "accepted" | "duplicate";

export interface KeyedContext<S> {
    readonly key: string;
    readonly seq: number;
    /** The send id of this message. */
    readonly id: string;
    /** Attempts including this one. */
    readonly attempt: number;
    /** The object state when the message was picked. */
    readonly state: S | undefined;
    /**
     * Lease-fenced transaction that also marks this message processed. `fn`
     * follows the transaction rules (reads first, no outside effects) and
     * returns the new state (`undefined` leaves it unchanged). Once per message.
     */
    tx(fn: (tx: Transaction, state: S | undefined) => Promise<S | undefined> | S | undefined): Promise<S | undefined>;
    /**
     * Run `fn` once per message; a redelivery returns its stored JSON result.
     * The result lives on the message document, so keep it small.
     */
    once<T>(name: string, fn: () => Promise<T>): Promise<T>;
}

export type KeyedHandler<M, S> = (ctx: KeyedContext<S>, msg: M) => Promise<void>;

/**
 * - `idle`: the mailbox was empty.
 * - `busy`: another drain holds the key; it will see this kick's message.
 * - `budget`: the drain budget ran out with messages left; the key was re-kicked.
 * - `retrying`: the head message failed and a delayed retry kick is armed.
 * - `waiting`: the head message is backing off; its retry kick is armed.
 * - `lost`: the lease was taken over; this drain's later writes were refused.
 */
export type DrainOutcome = "idle" | "busy" | "budget" | "retrying" | "waiting" | "lost";

export interface DrainResult {
    outcome: DrainOutcome;
    /** Messages whose handler completed. */
    processed: number;
    deadLettered: number;
}

export interface DrainOptions {
    /** The lease owner: the task name, epoch = its retry count. */
    owner?: LeaseOwner;
}

export interface KeyedObject<M, S> {
    send(key: string, msg: M, options: SendOptions): Promise<SendOutcome>;
    drain(key: string, handler: KeyedHandler<M, S>, options?: DrainOptions): Promise<DrainResult>;
    /** The retry delay after `attempts` failed attempts. */
    backoffMs(attempts: number): number;
    /**
     * Delete the key's object document and every message, marker and dead letter
     * (needs `objectTtl`). Idempotent, returns the children deleted. The object
     * goes first: a live drain stops at its next fence and a later send starts a
     * new generation. A send landing during the sweep may lose its message; the
     * drain then skips past it (`onExpired`).
     */
    purge(key: string): Promise<number>;
}

const MAILBOX = "mailbox";
const SENT = "mailboxSent";
const DEAD = "mailboxDead";
const LATEST = "latest";
const LEASE_FIELD = "lease";
const STATE_FIELD = "state";
/** A due time this close is due: absorbs instance clock skew against Cloud Tasks. */
const DUE_TOLERANCE_MS = 1_000;
const PURGE_PAGE = 200;
const ONCE_NAME = /^[A-Za-z0-9_-]{1,100}$/;
const OBJECT_NAME = /^[A-Za-z0-9_-]{1,100}$/;

const defaultRuntime: KeyedObjectRuntime = {
    now: () => Date.now(),
    newToken: () => randomUUID(),
};

function positive(name: string, value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
        throw new Error(`defineKeyedObject: ${name} must be a positive number, got ${value}`);
    }
}

function count(value: unknown): number {
    return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

/** Firestore Timestamp, JS Date, epoch ms, or absent (0). */
function toMillis(value: unknown): number {
    if (value && typeof (value as { toMillis?: unknown }).toMillis === "function") {
        return (value as { toMillis: () => number }).toMillis();
    }
    if (value instanceof Date) return value.getTime();
    return typeof value === "number" ? value : 0;
}

function describeError(err: unknown): string {
    const text = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    return text.slice(0, 1_000);
}

type JournalEntry = { json?: string };

type Picked<M, S> =
    | { kind: "empty" }
    | { kind: "skipped"; seq: number }
    | { kind: "dead"; letter: KeyedDeadLetter<M> }
    | { kind: "waiting"; seq: number; attempts: number; notBeforeMs: number }
    | { kind: "run"; seq: number; id: string; msg: M; attempts: number; journal: Record<string, JournalEntry>; state: S | undefined };

export function defineKeyedObject<M, S = DocumentData>(config: KeyedObjectConfig<M>): KeyedObject<M, S> {
    const { db, name, mode, queue, leaseMs, maxAttempts, backoff, inboxTtl, drainBudget, deleteField, objectTtl } = config;
    if (!OBJECT_NAME.test(name)) throw new Error(`defineKeyedObject: name must match ${OBJECT_NAME}, got "${name}"`);
    if (mode !== "fifo" && mode !== "latest") throw new Error(`defineKeyedObject: unknown mode "${String(mode)}"`);
    positive("leaseMs", leaseMs);
    positive("backoff.baseMs", backoff.baseMs);
    positive("backoff.maxMs", backoff.maxMs);
    positive("inboxTtl.ms", inboxTtl.ms);
    positive("drainBudget.ms", drainBudget.ms);
    if (!inboxTtl.field) throw new Error("defineKeyedObject: inboxTtl.field must name the TTL field");
    if (objectTtl && !(objectTtl.ms > 2 * inboxTtl.ms)) {
        throw new Error(`defineKeyedObject: objectTtl.ms must exceed 2 * inboxTtl.ms, got ${objectTtl.ms}`);
    }
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
        throw new Error(`defineKeyedObject: maxAttempts must be a positive integer, got ${maxAttempts}`);
    }
    if (!Number.isInteger(drainBudget.messages) || drainBudget.messages < 1) {
        throw new Error(`defineKeyedObject: drainBudget.messages must be a positive integer, got ${drainBudget.messages}`);
    }
    const runtime: KeyedObjectRuntime = { ...defaultRuntime, ...config.runtime };

    const objectRef = (key: string): DocumentReference => {
        if (!key || key.includes("/")) throw new Error(`defineKeyedObject(${name}): invalid key "${key}"`);
        return db.collection(name).doc(key);
    };
    const seqId = (seq: number): string => String(seq).padStart(12, "0");
    const itemRef = (key: string, seq: number): DocumentReference =>
        objectRef(key).collection(MAILBOX).doc(mode === "fifo" ? seqId(seq) : LATEST);
    const sentRef = (key: string, id: string): DocumentReference =>
        objectRef(key).collection(SENT).doc(createHash("sha256").update(id).digest("hex").slice(0, 32));
    const deadRef = (key: string, seq: number): DocumentReference => objectRef(key).collection(DEAD).doc(seqId(seq));

    const sentLedger = (key: string) =>
        defineLedger({
            db,
            at: (id) => sentRef(key, id),
            ttl: inboxTtl,
            runtime: { now: runtime.now },
        });

    const lease = defineLease<string>({
        db,
        at: objectRef,
        field: LEASE_FIELD,
        ttlMs: leaseMs,
        runtime: { now: runtime.now, newToken: runtime.newToken },
    });

    const counters = (snap: DocumentSnapshot) => ({
        lastSeq: count(snap.get("lastSeq")),
        processedSeq: count(snap.get("processedSeq")),
    });
    const expiresAt = (nowMs: number) => new Date(nowMs + inboxTtl.ms);
    const objectExpiry = (nowMs: number) => (objectTtl ? { [inboxTtl.field]: new Date(nowMs + objectTtl.ms) } : {});
    /**
     * `processedSeq` of a new object: the clock, in ms, when the object can restart.
     * Reaching the next generation's seqs would take a generation sending more than
     * one message per millisecond.
     */
    const originSeq = (nowMs: number): number => (objectTtl ? nowMs : 0);
    const backoffMs = (attempts: number): number =>
        Math.min(backoff.maxMs, backoff.baseMs * 2 ** Math.min(Math.max(attempts - 1, 0), 52));
    /** `processedSeq` after `seq` is done: in `latest` mode a newer seq may already be done. */
    const advanceTo = (processedSeq: number, seq: number): number => (mode === "fifo" ? seq : Math.max(processedSeq, seq));
    /** The message document holds `seq` (in `latest` mode it may hold a newer one). */
    const holds = (snap: DocumentSnapshot, seq: number): boolean => snap.exists && snap.get("seq") === seq;

    /**
     * Kick ids are (key, tag): `s<seq>` per send, `r<seq>.<attempts>` per
     * retry, `n<processedSeq>.<lastSeq>` per re-kick of a backlog position,
     * `w<lease window>` per wake-up past a lease that refused a drain.
     */
    const kick = async (key: string, tag: string, delayMs = 0): Promise<void> => {
        const delaySeconds = delayMs > 0 ? Math.ceil(delayMs / 1000) + 1 : undefined;
        await queue.enqueueOnce(
            { key },
            { prefix: name, key: `${key}\n${tag}`, window: "forever", ...(delaySeconds ? { delaySeconds } : {}) },
        );
    };

    const deadLetter = (
        tx: Transaction,
        key: string,
        object: DocumentSnapshot,
        item: DocumentSnapshot,
        lastError: string,
    ): KeyedDeadLetter<M> => {
        const data = item.data() ?? {};
        const seq = count(data.seq);
        const nowMs = runtime.now();
        tx.set(deadRef(key, seq), { ...data, lastError, deadAt: new Date(nowMs), [inboxTtl.field]: expiresAt(nowMs) });
        tx.delete(item.ref);
        tx.update(object.ref, { processedSeq: advanceTo(counters(object).processedSeq, seq) });
        return { key, seq, id: String(data.id), msg: data.msg as M, attempts: count(data.attempts), lastError };
    };

    const pickNext = (held: HeldLease, key: string): Promise<Picked<M, S>> =>
        held.tx(async (tx, object): Promise<Picked<M, S>> => {
            const { lastSeq, processedSeq } = counters(object);
            if (lastSeq <= processedSeq) return { kind: "empty" };
            const ref = itemRef(key, mode === "fifo" ? processedSeq + 1 : lastSeq);
            const item = await tx.get(ref);
            const seq = count(item.get("seq"));
            if (!item.exists || seq <= processedSeq) {
                // Deleted by the TTL policy before it ran: skip past it.
                const next = mode === "fifo" ? processedSeq + 1 : lastSeq;
                tx.update(object.ref, { processedSeq: next });
                return { kind: "skipped", seq: next };
            }
            const data = item.data() ?? {};
            const nowMs = runtime.now();
            const notBeforeMs = toMillis(data.notBefore);
            if (notBeforeMs > nowMs + DUE_TOLERANCE_MS) {
                return { kind: "waiting", seq, attempts: count(data.attempts), notBeforeMs };
            }
            const attempts = count(data.attempts) + 1;
            if (attempts > maxAttempts) {
                const lastError = typeof data.lastError === "string" ? data.lastError : "attempt ended without an outcome";
                return { kind: "dead", letter: deadLetter(tx, key, object, item, lastError) };
            }
            tx.update(ref, { attempts, notBefore: deleteField(), startedAt: new Date(nowMs) });
            return {
                kind: "run",
                seq,
                id: String(data.id),
                msg: data.msg as M,
                attempts,
                journal: { ...(data.journal as Record<string, JournalEntry> | undefined) },
                state: object.get(STATE_FIELD) as S | undefined,
            };
        });

    /** Mark `seq` processed (and store `state` when given) inside `tx`. */
    const complete = (
        tx: Transaction,
        object: DocumentSnapshot,
        item: DocumentSnapshot,
        seq: number,
        state: S | undefined,
    ): void => {
        const update: Record<string, unknown> = { processedSeq: advanceTo(counters(object).processedSeq, seq) };
        if (state !== undefined) update[STATE_FIELD] = state;
        tx.update(object.ref, update);
        if (holds(item, seq)) tx.delete(item.ref);
    };

    const assertCurrent = (object: DocumentSnapshot, seq: number): void => {
        const { processedSeq } = counters(object);
        if (mode === "fifo" ? processedSeq !== seq - 1 : processedSeq >= seq) {
            throw new Error(`defineKeyedObject(${name}): seq ${seq} is not the next message (processedSeq ${processedSeq})`);
        }
    };

    const notify = async (letter: KeyedDeadLetter<M>): Promise<void> => {
        try {
            await config.onDeadLetter?.(letter);
        } catch {
            // The hook is for logging; it must never stall the key.
        }
    };

    const notifyExpired = async (key: string, seq: number): Promise<void> => {
        try {
            await config.onExpired?.({ key, seq });
        } catch {
            // Same as above.
        }
    };

    /** Runs one picked message. Returns the drain's outcome if it must stop. */
    async function runOne(
        held: HeldLease,
        key: string,
        handler: KeyedHandler<M, S>,
        pick: Extract<Picked<M, S>, { kind: "run" }>,
        tally: { processed: number; deadLettered: number },
    ): Promise<DrainOutcome | undefined> {
        const { seq, attempts, journal } = pick;
        const ref = itemRef(key, seq);
        let committed = false;

        const ctx: KeyedContext<S> = {
            key,
            seq,
            id: pick.id,
            attempt: attempts,
            state: pick.state,
            async tx(fn) {
                if (committed) throw new Error(`defineKeyedObject(${name}): ctx.tx already committed seq ${seq}`);
                const state = await held.tx(async (tx, object) => {
                    assertCurrent(object, seq);
                    const item = await tx.get(ref);
                    const next = await fn(tx, object.get(STATE_FIELD) as S | undefined);
                    complete(tx, object, item, seq, next);
                    return next;
                });
                committed = true;
                return state;
            },
            async once<T>(step: string, fn: () => Promise<T>): Promise<T> {
                if (!ONCE_NAME.test(step)) throw new Error(`defineKeyedObject(${name}): invalid once name "${step}"`);
                if (Object.prototype.hasOwnProperty.call(journal, step)) {
                    const stored = journal[step].json;
                    return (stored === undefined ? undefined : JSON.parse(stored)) as T;
                }
                const value = await fn();
                const entry: JournalEntry = value === undefined ? {} : { json: JSON.stringify(value) };
                await held.tx(async (tx) => {
                    const item = await tx.get(ref);
                    // Superseded (latest mode) or gone: nothing to journal on.
                    if (holds(item, seq)) tx.update(ref, { [`journal.${step}`]: entry });
                });
                journal[step] = entry;
                return value;
            },
        };

        try {
            await handler(ctx, pick.msg);
        } catch (err) {
            if (committed || err instanceof LeaseLostError) throw err;
            const lastError = describeError(err);
            const failure = await held.tx(async (tx, object) => {
                const item = await tx.get(ref);
                if (!holds(item, seq)) return { kind: "gone" as const };
                if (attempts >= maxAttempts) return { kind: "dead" as const, letter: deadLetter(tx, key, object, item, lastError) };
                const notBeforeMs = runtime.now() + backoffMs(attempts);
                tx.update(ref, { lastError, notBefore: new Date(notBeforeMs) });
                return { kind: "retry" as const, notBeforeMs };
            });
            if (failure.kind === "gone") return undefined;
            if (failure.kind === "dead") {
                tally.deadLettered++;
                await notify(failure.letter);
                return undefined;
            }
            await kick(key, `r${seq}.${attempts}`, failure.notBeforeMs - runtime.now());
            return "retrying";
        }

        if (!committed) {
            await held.tx(async (tx, object) => {
                assertCurrent(object, seq);
                complete(tx, object, await tx.get(ref), seq, undefined);
            });
        }
        tally.processed++;
        return undefined;
    }

    return {
        backoffMs,

        async purge(key) {
            if (!objectTtl) throw new Error(`defineKeyedObject(${name}): purge needs objectTtl (a restart would reuse seqs)`);
            const object = objectRef(key);
            await object.delete();
            let deleted = 0;
            for (const child of [MAILBOX, SENT, DEAD]) {
                for (;;) {
                    const page = await object.collection(child).limit(PURGE_PAGE).get();
                    if (page.empty) break;
                    const batch = db.batch();
                    page.docs.forEach((doc) => batch.delete(doc.ref));
                    await batch.commit();
                    deleted += page.size;
                }
            }
            return deleted;
        },

        async send(key, msg, options) {
            const { id } = options;
            if (!id) throw new Error(`defineKeyedObject(${name}): send needs an id`);
            const object = objectRef(key);
            const sent = await sentLedger(key).applyOnce(
                id,
                async (tx) => {
                    const snap = await tx.get(object);
                    const nowMs = runtime.now();
                    const seq = (snap.exists ? counters(snap).lastSeq : originSeq(nowMs)) + 1;
                    const item = {
                        seq,
                        id,
                        msg,
                        attempts: 0,
                        createdAt: new Date(nowMs),
                        [inboxTtl.field]: expiresAt(nowMs),
                    };
                    // FIFO: `create`, so a stray document at this seq fails loudly.
                    if (mode === "fifo") tx.create(itemRef(key, seq), item);
                    else tx.set(itemRef(key, seq), item);
                    if (snap.exists) tx.update(object, { lastSeq: seq, ...objectExpiry(nowMs) });
                    else tx.create(object, { lastSeq: seq, processedSeq: seq - 1, ...objectExpiry(nowMs) });
                    return seq;
                },
                { fields: { id } },
            );
            const wantsKick = options.kick ?? true;
            if (sent.status === "applied") {
                if (wantsKick) await kick(key, `s${sent.value}`);
                return "accepted";
            }
            if (wantsKick) {
                // The newest message's own kick id: already taken in the normal
                // case (a no-op), created if the original send died before it.
                const { lastSeq, processedSeq } = counters(await object.get());
                if (lastSeq > processedSeq) await kick(key, `s${lastSeq}`);
            }
            return "duplicate";
        },

        async drain(key, handler, options = {}) {
            const startedMs = runtime.now();
            const tally = { processed: 0, deadLettered: 0 };
            let runs = 0;
            let outcome: DrainOutcome;
            try {
                outcome = await lease.run(
                    key,
                    async (held): Promise<DrainOutcome> => {
                        for (;;) {
                            // Only after progress: a slow acquire must not spend the
                            // whole budget, or the re-kicks would never run a message.
                            const handled = runs + tally.deadLettered;
                            if (runs >= drainBudget.messages || (handled > 0 && runtime.now() - startedMs >= drainBudget.ms)) {
                                return "budget";
                            }
                            const pick = await pickNext(held, key);
                            switch (pick.kind) {
                                case "empty":
                                    return "idle";
                                case "skipped":
                                    await notifyExpired(key, pick.seq);
                                    continue;
                                case "dead":
                                    tally.deadLettered++;
                                    await notify(pick.letter);
                                    continue;
                                case "waiting":
                                    await kick(key, `r${pick.seq}.${pick.attempts}`, pick.notBeforeMs - runtime.now());
                                    return "waiting";
                                case "run": {
                                    runs++;
                                    const stop = await runOne(held, key, handler, pick, tally);
                                    if (stop) return stop;
                                }
                            }
                        }
                    },
                    { owner: options.owner },
                );
            } catch (err) {
                if (err instanceof LeaseBusyError) {
                    // The holder normally sees this kick's message. If it is gone
                    // without releasing (a failed release, a crash whose task is
                    // never retried), only a kick past the lease expiry remains.
                    await kick(key, `w${Math.floor(runtime.now() / leaseMs)}`, leaseMs);
                    return { outcome: "busy", ...tally };
                }
                if (err instanceof LeaseTargetMissingError) return { outcome: "idle", ...tally };
                if (!(err instanceof LeaseLostError)) throw err;
                outcome = "lost";
            }
            if (outcome === "idle" || outcome === "budget") {
                // A send that committed after our last read kicked a drain that
                // found us busy: whoever releases re-checks and kicks again.
                const { lastSeq, processedSeq } = counters(await objectRef(key).get());
                if (lastSeq > processedSeq) await kick(key, `n${processedSeq}.${lastSeq}`);
                else outcome = "idle";
            }
            return { outcome, ...tally };
        },
    };
}
