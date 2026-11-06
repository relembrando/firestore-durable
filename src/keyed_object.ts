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
 *   and in commit order. It reads the object in that transaction, so concurrent
 *   sends serialize and `lastSeq + 1` cannot be a live message: whatever sits
 *   there is a purged generation's leftover, and the message overwrites it. The
 *   drain kick is enqueued AFTER the commit, with a task id derived from (key,
 *   seq); a duplicate send while messages are pending re-kicks the newest one's
 *   id, so a send retried after a failed kick heals.
 * - One active handler per key: `drain` holds the key's lease (field mode on
 *   the object document, owner = the task, epoch = its retry count, so a
 *   platform retry of a crashed drain takes over at once). Contention is
 *   `busy`, not an error: the holder re-reads the mailbox after every message
 *   and, after releasing, re-checks it and kicks again if a send raced the
 *   release (the lost-wakeup window). A refused drain arms nothing: a holder
 *   that vanished without releasing is the sweep's to recover, once its lease
 *   has expired.
 * - Every drain-side write (attempt counts, `ctx.tx`, `ctx.once` journal,
 *   seq advance, dead letters) commits through the lease fence: a drain whose
 *   lease was taken over writes nothing more and reports `lost`.
 * - At-least-once per message, not exactly-once: the attempt count is written
 *   BEFORE the handler runs, so a crash counts as an attempt and the message
 *   is redelivered (`ctx.attempt` > 1). There is no deterministic replay;
 *   `ctx.once` journals a step's JSON result on the message so a redelivery
 *   returns it instead of re-running the step (a crash between the step and
 *   its journal write still re-runs it). Concurrent calls with one name share
 *   one execution and its outcome, a rejection included; a call after a failure
 *   settled runs the step again.
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
 *   The time check applies once the drain made progress of any kind (a run, a
 *   dead letter, a skip), so a slow acquire cannot spend it and an expired
 *   backlog cannot outrun it. Only handler runs spend `messages`.
 * - Liveness rests on kicks plus `sweep`. A kick can be lost: queue retries
 *   exhausted, queue purged, a process killed between a commit and its enqueue,
 *   or an id Cloud Tasks still refuses, read as success (a seq repeated by a
 *   restart with a clock behind; a retry task that re-arms its own running id,
 *   when an instance ~1-3 s behind finds the message `waiting`). A holder can
 *   also vanish, and the kicks it refused armed nothing. All of these leave the
 *   same state, a mailbox document with nothing coming for it, because the
 *   document is deleted when its message completes or dead-letters. `sweep`
 *   re-kicks those keys, so recovery takes one to two sweep periods past the
 *   threshold, not a next send. A `busy` caused by acquire contention, with no
 *   holder at all, recovers the same way when no other kick follows.
 * - `sweep` is one read per key: the head (lowest document id) of each key with
 *   a pending message, through a document-name range on the `mailbox` collection
 *   group. The range scopes it to this object, so objects sharing the group
 *   cannot starve each other, and it jumps past the rest of a key, so one long
 *   backlog costs one read. It needs no index (unfiltered, ascending; checked on
 *   a real backend, not just the emulator, which enforces none) and the field
 *   mask keeps it off message content. A head in backoff is skipped, since its
 *   retry kick is armed, and so is one with activity inside `olderThanMs`. Sweep
 *   kicks use a time window of `olderThanMs`, so runs inside one window coalesce;
 *   a refused kick costs one more run, because the next window's id is new. A
 *   stray document with no object costs one no-op drain per window until its TTL.
 *   `limit` bounds one call: follow `next` to reach every key, since keys that
 *   stay pending would otherwise hide the ones behind them.
 * - What liveness does not cover: a worker down past `inboxTtl` loses the
 *   messages (the TTL deletes them, and the sweep sees nothing), and `onExpired`
 *   reports that only on the key's next drain, which takes a later send.
 * - Messages, markers and dead letters carry the TTL field; Firestore's TTL
 *   policy (configured outside this code, per collection group) bounds them,
 *   so `inboxTtl.ms` is also the dedupe window and the longest a message may
 *   wait. A message the TTL deleted before it ran is skipped (`onExpired`). The
 *   object document has no TTL by default: deleting it restarts `seq`, and a
 *   reused seq collides with live children and with kick ids Cloud Tasks still
 *   remembers (up to 24 h). An object that must disappear with its data sets
 *   `objectTtl`: a generation then starts its seqs from the clock, which keeps
 *   its documents clear of an earlier generation's unless clocks disagree. If
 *   they do, a repeated seq overwrites a leftover document and may have its kick
 *   refused, which the sweep covers. A kick names only the key, so one from an
 *   earlier generation just drains the current one.
 * - Field names, document ids and the kick-id tags are the wire format shared
 *   with every deployed version. Changing them is a data migration. A kick id
 *   only coalesces, so the worst mix of versions is one extra no-op drain.
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
    /** Handler runs, and wall time including skips, one drain may spend before re-kicking. */
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
     * The result lives on the message document, so keep it small. Concurrent
     * calls with one name share one run and its outcome, a rejection included.
     */
    once<T>(name: string, fn: () => Promise<T>): Promise<T>;
}

export type KeyedHandler<M, S> = (ctx: KeyedContext<S>, msg: M) => Promise<void>;

/**
 * - `idle`: the mailbox was empty.
 * - `busy`: another drain holds the key; it will see this kick's message, or the
 *   sweep re-kicks the key if it is gone.
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

export interface KeyedSweepOptions {
    /**
     * Re-kick a key whose head saw no activity (created, due or started) for this
     * long. Keep it above `leaseMs`, so a kick finds a vanished holder's lease
     * expired. Also the coalescing window of sweep kicks.
     */
    olderThanMs: number;
    /** Keys examined per call, one read each; also bounds the kicks. */
    limit: number;
    /** Continue after this key: the `next` of the previous call. */
    after?: string;
}

export interface KeyedSweepResult {
    /** Keys with a pending message that were examined. */
    scanned: number;
    /** Stale keys given a new kick. */
    rekicked: number;
    /** Stale keys whose kick for this window already exists or existed. */
    coalesced: number;
    /** Keys whose head is backing off: its retry kick is armed. */
    backingOff: number;
    /** Keys whose head saw activity within `olderThanMs`. */
    recent: number;
    /** Stale keys whose kick failed to enqueue; the next run tries again. */
    failed: number;
    /** The first enqueue failure, when any. */
    error?: string;
    /**
     * `limit` was reached with keys left: pass this as `after` to reach them.
     * Restarting from the top instead revisits the same keys, and a key behind them
     * waits for as long as they stay pending.
     */
    next?: string;
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
    /** Re-kick keys whose pending message may have lost its kick. Reads no message content. */
    sweep(options: KeyedSweepOptions): Promise<KeyedSweepResult>;
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
const DOCUMENT_NAME = "__name__";
/** Document-id bounds below and above any key or message id. */
const ID_MIN = "\u0000";
const ID_MAX = "\u{10ffff}";
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
    | {
        kind: "run";
        seq: number;
        id: string;
        msg: M;
        attempts: number;
        journal: Record<string, JournalEntry>;
        state: S | undefined;
    };

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
     * one message per millisecond, or a clock running behind: a seq may then repeat
     * (see the sweep in the header).
     */
    const originSeq = (nowMs: number): number => (objectTtl ? nowMs : 0);
    const backoffMs = (attempts: number): number =>
        Math.min(backoff.maxMs, backoff.baseMs * 2 ** Math.min(Math.max(attempts - 1, 0), 52));
    /** `processedSeq` after `seq` is done: in `latest` mode a newer seq may already be done. */
    const advanceTo = (processedSeq: number, seq: number): number => (mode === "fifo" ? seq : Math.max(processedSeq, seq));
    /** The message document holds `seq` (in `latest` mode it may hold a newer one). */
    const holds = (snap: DocumentSnapshot, seq: number): boolean => snap.exists && snap.get("seq") === seq;

    /** Kick ids: `s<seq>` per send, `r<seq>.<attempts>` per retry, `n<processedSeq>.<lastSeq>` per backlog position. */
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
        /** Steps running in this attempt, so concurrent callers of one name share a run. */
        const running = new Map<string, Promise<unknown>>();

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
                const inFlight = running.get(step);
                if (inFlight) return inFlight as Promise<T>;
                const execute = async (): Promise<T> => {
                    const value = await fn();
                    const entry: JournalEntry = value === undefined ? {} : { json: JSON.stringify(value) };
                    await held.tx(async (tx) => {
                        const item = await tx.get(ref);
                        // Superseded (latest mode) or gone: nothing to journal on.
                        if (holds(item, seq)) tx.update(ref, { [`journal.${step}`]: entry });
                    });
                    journal[step] = entry;
                    return value;
                };
                const run = execute();
                running.set(step, run);
                try {
                    return await run;
                } finally {
                    running.delete(step);
                }
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

        async sweep({ olderThanMs, limit, after }) {
            positive("olderThanMs", olderThanMs);
            if (!Number.isInteger(limit) || limit < 1) {
                throw new Error(`defineKeyedObject: limit must be a positive integer, got ${limit}`);
            }
            const result: KeyedSweepResult = { scanned: 0, rekicked: 0, coalesced: 0, backingOff: 0, recent: 0, failed: 0 };
            const nowMs = runtime.now();
            const heads = db
                .collectionGroup(MAILBOX)
                .orderBy(DOCUMENT_NAME)
                .endBefore(`${name}/${ID_MAX}`)
                .select("createdAt", "notBefore", "startedAt")
                .limit(1);
            const pastKey = (key: string) => `${name}/${key}/${MAILBOX}/${ID_MAX}`;
            let cursor = after === undefined ? `${name}/${ID_MIN}` : pastKey(after);
            let lastKey = after;
            for (;;) {
                const [head] = (await heads.startAfter(cursor).get()).docs;
                if (!head) return result;
                if (result.scanned >= limit) return { ...result, next: lastKey };
                result.scanned++;
                const key = head.ref.path.split("/")[1];
                lastKey = key;
                cursor = pastKey(key);

                const notBeforeMs = toMillis(head.get("notBefore"));
                if (notBeforeMs > nowMs) {
                    result.backingOff++;
                    continue;
                }
                const activeMs = Math.max(toMillis(head.get("createdAt")), notBeforeMs, toMillis(head.get("startedAt")));
                if (nowMs - activeMs < olderThanMs) {
                    result.recent++;
                    continue;
                }
                try {
                    const outcome = await queue.enqueueOnce(
                        { key },
                        { prefix: name, key: `${key}\nsweep`, window: { seconds: olderThanMs / 1000 } },
                    );
                    if (outcome === "created") result.rekicked++;
                    else result.coalesced++;
                } catch (err) {
                    result.failed++;
                    result.error ??= describeError(err);
                }
            }
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
                    // `set`: nothing live can be at `lastSeq + 1`, so a document there
                    // is a leftover that must not wedge the key.
                    tx.set(itemRef(key, seq), {
                        seq,
                        id,
                        msg,
                        attempts: 0,
                        createdAt: new Date(nowMs),
                        [inboxTtl.field]: expiresAt(nowMs),
                    });
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
                const snap = await object.get();
                const { lastSeq, processedSeq } = counters(snap);
                if (lastSeq > processedSeq) await kick(key, `s${lastSeq}`);
            }
            return "duplicate";
        },

        async drain(key, handler, options = {}) {
            const startedMs = runtime.now();
            const tally = { processed: 0, deadLettered: 0 };
            let runs = 0;
            let skips = 0;
            let outcome: DrainOutcome;
            try {
                outcome = await lease.run(
                    key,
                    async (held): Promise<DrainOutcome> => {
                        for (;;) {
                            // Only after progress: a slow acquire must not spend the
                            // whole budget, or the re-kicks would never run a message.
                            const progress = runs + tally.deadLettered + skips;
                            if (runs >= drainBudget.messages || (progress > 0 && runtime.now() - startedMs >= drainBudget.ms)) {
                                return "budget";
                            }
                            const pick = await pickNext(held, key);
                            switch (pick.kind) {
                                case "empty":
                                    return "idle";
                                case "skipped":
                                    skips++;
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
                if (err instanceof LeaseBusyError) return { outcome: "busy", ...tally };
                if (err instanceof LeaseTargetMissingError) return { outcome: "idle", ...tally };
                if (!(err instanceof LeaseLostError)) throw err;
                outcome = "lost";
            }
            if (outcome === "idle" || outcome === "budget") {
                // A send that committed after our last read kicked a drain that
                // found us busy: whoever releases re-checks and kicks again.
                const snap = await objectRef(key).get();
                const { lastSeq, processedSeq } = counters(snap);
                if (lastSeq > processedSeq) await kick(key, `n${processedSeq}.${lastSeq}`);
                else outcome = "idle";
            }
            return { outcome, ...tally };
        },
    };
}
