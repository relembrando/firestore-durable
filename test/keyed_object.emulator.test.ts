/**
 * `src/keyed_object.ts` against the REAL Firestore emulator through the
 * REAL `firebase-admin` SDK (jest.emulator.config.js). `durable_keyed_object
 * .test.ts` pins the object's decisions against a fake that runs transactions
 * one at a time; this suite proves, under genuine concurrency (server-side
 * transactions, contention, aborts, SDK retries), what the mailbox promises:
 * gap-free ordered seqs from concurrent sends, one active handler per key,
 * create-only dedupe, journaled steps across a crash, the lease fence on
 * `ctx.tx`, dead letters, and `latest` supersession.
 */

import { FieldValue, getFirestore, Query } from "firebase-admin/firestore";
import type { QuerySnapshot } from "firebase-admin/firestore";
import { setTimeout as sleep } from "timers/promises";
import { defineKeyedObject, defineTaskQueue, LeaseLostError, taskIdFor } from "../src";
import type { KeyedDeadLetter, KeyedHandler, KeyedObjectConfig, KeyedObjectKick, KeyedObjectMode } from "../src";

const db = getFirestore();
const LEASE = 60_000;
/** Upper bound on one held handler, so a stuck drain cannot hang the suite. */
const HOLD_DEADLINE_MS = 5_000;
const SWEEP = { olderThanMs: 15 * 60 * 1000, limit: 100 };

type Msg = { n: number };
type State = { count?: number; by?: string };

/** A Cloud Tasks stand-in: records kicks, refuses a reused id like the real one. */
function fakeQueue() {
    const ids = new Set<string>();
    const kicks: { key: string; id: string; delaySeconds?: number }[] = [];
    const queue = defineTaskQueue<KeyedObjectKick>({
        enqueue: async (payload, options) => {
            if (ids.has(options.id)) throw Object.assign(new Error("6 ALREADY_EXISTS"), { code: 6 });
            ids.add(options.id);
            kicks.push({ key: payload.key, id: options.id, delaySeconds: options.scheduleDelaySeconds });
        },
    });
    return { queue, kicks };
}

/** One object per caller, as separate function instances would have. */
function newObject(
    mode: KeyedObjectMode = "fifo",
    extra: Partial<KeyedObjectConfig<Msg>> = {},
) {
    const { queue, kicks } = fakeQueue();
    const object = defineKeyedObject<Msg, State>({
        db,
        name: "objects",
        mode,
        queue,
        leaseMs: LEASE,
        maxAttempts: 3,
        backoff: { baseMs: 1, maxMs: 1 },
        inboxTtl: { field: "expiresAt", ms: 86_400_000 },
        drainBudget: { messages: 100, ms: 60_000 },
        deleteField: () => FieldValue.delete(),
        ...extra,
    });
    return { object, kicks };
}

function deferred<T = void>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((r) => (resolve = r));
    return { promise, resolve };
}

/** Resolves when `until` does or after `ms`, whichever is first, leaving no timer behind. */
async function holdUntil(until: Promise<unknown>, ms: number): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([until, new Promise<void>((r) => (timer = setTimeout(r, ms)))]);
    clearTimeout(timer);
}

async function counters(key: string) {
    const snap = await db.doc(`objects/${key}`).get();
    return { lastSeq: snap.get("lastSeq"), processedSeq: snap.get("processedSeq"), state: snap.get("state") };
}

async function mailbox(key: string) {
    return (await db.collection(`objects/${key}/mailbox`).get()).docs.map((d) => d.id);
}

/**
 * D drains race on each key. A handler that gets in holds until every other
 * drain of ITS key has returned (or the deadline), so a second handler on the
 * same key is recorded, not missed by timing: with working exclusion the
 * others are refused (`busy`) and return; with broken exclusion a second one
 * enters while the first is still inside. Each handler also waits for every
 * other KEY to have a handler inside, which only resolves if keys progress in
 * parallel. `staggerMs` spaces starts (the control's lapsing lease needs it).
 */
async function raceDrains(keys: string[], drainsPerKey: number, leaseMs: number, staggerMs = 0) {
    const active = new Map<string, number>(keys.map((k) => [k, 0]));
    let perKeyMax = 0;
    let concurrentKeysMax = 0;
    const entered = new Map<string, ReturnType<typeof deferred<void>>>(keys.map((k) => [k, deferred()]));
    const returned = new Map<string, number>(keys.map((k) => [k, 0]));
    const othersDone = new Map<string, ReturnType<typeof deferred<void>>>(keys.map((k) => [k, deferred()]));
    const handled: string[] = [];

    const handler = (key: string): KeyedHandler<Msg, State> => async (ctx) => {
        active.set(key, active.get(key)! + 1);
        perKeyMax = Math.max(perKeyMax, active.get(key)!);
        concurrentKeysMax = Math.max(concurrentKeysMax, [...active.values()].filter((n) => n > 0).length);
        handled.push(`${key}:${ctx.seq}`);
        entered.get(key)!.resolve();
        try {
            await holdUntil(Promise.all(keys.filter((k) => k !== key).map((k) => entered.get(k)!.promise)), HOLD_DEADLINE_MS);
            await holdUntil(othersDone.get(key)!.promise, HOLD_DEADLINE_MS);
        } finally {
            active.set(key, active.get(key)! - 1);
        }
    };

    const runs = keys.flatMap((key) =>
        Array.from({ length: drainsPerKey }, async (_, i) => {
            await sleep(i * staggerMs);
            let inside = false;
            const result = await newObject("fifo", { leaseMs }).object.drain(key, async (ctx, msg) => {
                inside = true;
                await handler(key)(ctx, msg);
            });
            if (!inside) {
                const n = returned.get(key)! + 1;
                returned.set(key, n);
                if (n === drainsPerKey - 1) othersDone.get(key)!.resolve();
            }
            return result.outcome;
        }),
    );
    const outcomes = await Promise.all(runs);
    return { perKeyMax, concurrentKeysMax, outcomes, handled };
}

describe("defineKeyedObject against the Firestore emulator", () => {
    it("concurrent sends on one key get gap-free seqs, and a drain handles each once in seq order", async () => {
        const N = 6;
        const { object } = newObject();
        const sent = await Promise.all(Array.from({ length: N }, (_, n) => object.send("k", { n }, { id: `m${n}` })));
        expect(sent).toEqual(Array(N).fill("accepted"));

        const items = (await db.collection("objects/k/mailbox").get()).docs.map((d) => d.data());
        const seqs = items.map((d) => d.seq as number).sort((a, b) => a - b);
        expect(seqs).toEqual(Array.from({ length: N }, (_, i) => i + 1));
        expect(new Set(items.map((d) => d.id)).size).toBe(N);

        const seen: { seq: number; n: number }[] = [];
        const result = await object.drain("k", async (ctx, msg) => {
            seen.push({ seq: ctx.seq, n: msg.n });
        });

        expect(result).toEqual({ outcome: "idle", processed: N, deadLettered: 0 });
        expect(seen.map((s) => s.seq)).toEqual(seqs);
        expect(seen.map((s) => s.n).sort()).toEqual(Array.from({ length: N }, (_, n) => n));
        // Each message carries the seq it was committed with.
        const bySeq = new Map(items.map((d) => [d.seq as number, (d.msg as Msg).n]));
        expect(seen.every((s) => bySeq.get(s.seq) === s.n)).toBe(true);
        expect(await counters("k")).toMatchObject({ lastSeq: N, processedSeq: N });
        expect(await mailbox("k")).toEqual([]);
    });

    it("a send id seen concurrently is accepted once; the rest are duplicates", async () => {
        const { object } = newObject();
        const outcomes = await Promise.all(Array.from({ length: 5 }, () => object.send("k", { n: 1 }, { id: "same" })));

        expect(outcomes.filter((o) => o === "accepted")).toHaveLength(1);
        expect(outcomes.filter((o) => o === "duplicate")).toHaveLength(4);
        expect(await mailbox("k")).toHaveLength(1);
        expect(await counters("k")).toMatchObject({ lastSeq: 1, processedSeq: 0 });
    });

    it("many concurrent drains: one active handler per key, while different keys run in parallel", async () => {
        const keys = ["a", "b"];
        for (const key of keys) await newObject().object.send(key, { n: 0 }, { id: `${key}-0` });

        const { perKeyMax, concurrentKeysMax, outcomes, handled } = await raceDrains(keys, 4, LEASE);

        expect(perKeyMax).toBe(1);
        expect(concurrentKeysMax).toBe(2);
        expect(handled.sort()).toEqual(["a:1", "b:1"]);
        expect(outcomes.filter((o) => o === "idle")).toHaveLength(2);
        expect(outcomes.filter((o) => o === "busy")).toHaveLength(6);
        for (const key of keys) expect(await counters(key)).toMatchObject({ lastSeq: 1, processedSeq: 1 });
    });

    // Verify the verifier: through the same probe, a lease that lapses as soon
    // as it is written must show two handlers on one key, or `toBe(1)` above
    // proves nothing. Staggered starts skip the simultaneous-acquire deadlock,
    // which the emulator only breaks with a multi-second lock timeout.
    it("control: the probe sees two handlers on one key when the lease cannot exclude", async () => {
        await newObject().object.send("a", { n: 0 }, { id: "a-0" });

        const { perKeyMax } = await raceDrains(["a"], 2, 1, 50);

        expect(perKeyMax).toBe(2);
    });

    it("a drain that crashed mid-message is redelivered and its journaled step is not re-run", async () => {
        const { object } = newObject();
        await object.send("k", { n: 1 }, { id: "m1" });
        let sideEffects = 0;
        const step = async () => ({ receipt: `r${++sideEffects}` });

        // Drain A runs the step, then "crashes": it never returns while B runs.
        const aJournaled = deferred();
        const aMayReturn = deferred();
        const aDrain = newObject().object.drain(
            "k",
            async (ctx) => {
                await ctx.once("charge", step);
                aJournaled.resolve();
                await aMayReturn.promise;
            },
            { owner: { id: "task-1", epoch: 0 } },
        );
        await aJournaled.promise;

        // The platform retries the same task (epoch 1), which takes the lease over.
        let redelivery: { attempt: number; receipt: unknown } | undefined;
        const b = await newObject().object.drain(
            "k",
            async (ctx) => {
                redelivery = { attempt: ctx.attempt, receipt: await ctx.once("charge", step) };
            },
            { owner: { id: "task-1", epoch: 1 } },
        );

        aMayReturn.resolve();
        expect(b).toMatchObject({ outcome: "idle", processed: 1 });
        expect(redelivery).toEqual({ attempt: 2, receipt: { receipt: "r1" } });
        expect(sideEffects).toBe(1);
        expect(await aDrain).toMatchObject({ outcome: "lost", processed: 0 });
        expect(await counters("k")).toMatchObject({ processedSeq: 1 });
    });

    it("a drain whose lease was taken over cannot commit ctx.tx, and the seq does not advance", async () => {
        const { object } = newObject();
        await object.send("k", { n: 1 }, { id: "m1" });

        const aInside = deferred();
        const bInside = deferred();
        const aTried = deferred<unknown>();
        const aDrain = object.drain("k", async (ctx) => {
            aInside.resolve();
            await bInside.promise;
            try {
                await ctx.tx(() => ({ by: "A" }));
                aTried.resolve("committed");
            } catch (err) {
                aTried.resolve(err);
                throw err;
            }
        });
        await aInside.promise;

        // B's clock runs past A's lease expiry, so B sees it lapsed and takes over.
        const bObject = newObject("fifo", { runtime: { now: () => Date.now() + LEASE + 1_000 } }).object;
        let afterA: Awaited<ReturnType<typeof counters>> | undefined;
        const b = await bObject.drain("k", async (ctx) => {
            bInside.resolve();
            expect(await aTried.promise).toBeInstanceOf(LeaseLostError);
            afterA = await counters("k");
            await ctx.tx((_tx, state) => ({ ...state, by: "B" }));
        });

        expect(afterA).toMatchObject({ processedSeq: 0 });
        expect(afterA?.state).toBeUndefined();
        expect(b).toMatchObject({ outcome: "idle", processed: 1 });
        expect(await aDrain).toMatchObject({ outcome: "lost", processed: 0 });
        expect(await counters("k")).toMatchObject({ processedSeq: 1, state: { by: "B" } });
    });

    it("a poison message is dead-lettered after maxAttempts and the next message proceeds", async () => {
        const letters: KeyedDeadLetter<Msg>[] = [];
        const { object, kicks } = newObject("fifo", { onDeadLetter: (letter) => void letters.push(letter) });
        await object.send("k", { n: 1 }, { id: "poison" });
        await object.send("k", { n: 2 }, { id: "fine" });
        const handled: number[] = [];
        const handler: KeyedHandler<Msg, State> = async (ctx, msg) => {
            if (msg.n === 1) throw new Error("cannot parse");
            await ctx.tx((_tx, state) => ({ count: (state?.count ?? 0) + 1 }));
            handled.push(msg.n);
        };

        // Each drain stands for one retry kick firing.
        expect((await object.drain("k", handler)).outcome).toBe("retrying");
        expect((await object.drain("k", handler)).outcome).toBe("retrying");
        expect(await object.drain("k", handler)).toEqual({ outcome: "idle", processed: 1, deadLettered: 1 });

        expect(handled).toEqual([2]);
        expect(letters).toEqual([expect.objectContaining({ key: "k", seq: 1, id: "poison", attempts: 3, msg: { n: 1 } })]);
        expect(letters[0].lastError).toMatch(/cannot parse/);
        const dead = await db.doc("objects/k/mailboxDead/000000000001").get();
        expect(dead.get("lastError")).toMatch(/cannot parse/);
        // Two sends, then one retry kick per failed attempt before the last.
        expect(kicks).toHaveLength(4);
        expect(await counters("k")).toMatchObject({ processedSeq: 2, state: { count: 1 } });
        expect(await mailbox("k")).toEqual([]);
    });

    it("latest mode: the handler sees only the newest payload, and a send during a handler runs next", async () => {
        const { object } = newObject("latest");
        for (const n of [1, 2, 3]) await object.send("k", { n }, { id: `v${n}` });
        expect(await mailbox("k")).toEqual(["latest"]);

        const seen: { seq: number; n: number }[] = [];
        const result = await object.drain("k", async (ctx, msg) => {
            seen.push({ seq: ctx.seq, n: msg.n });
            if (msg.n === 3) await object.send("k", { n: 4 }, { id: "v4" });
        });

        expect(seen).toEqual([{ seq: 3, n: 3 }, { seq: 4, n: 4 }]);
        expect(result).toEqual({ outcome: "idle", processed: 2, deadLettered: 0 });
        expect(await counters("k")).toMatchObject({ lastSeq: 4, processedSeq: 4 });
        expect(await mailbox("k")).toEqual([]);
    });

    describe("objectTtl", () => {
        const objectTtl = { ms: 3 * 86_400_000 };

        /** One object per instance, all kicking into one Cloud Tasks stand-in, each with its own clock. */
        function instances(mode: KeyedObjectMode = "fifo") {
            const { queue, kicks } = fakeQueue();
            const at = (nowMs: number) =>
                defineKeyedObject<Msg, State>({
                    db,
                    name: "objects",
                    mode,
                    queue,
                    leaseMs: LEASE,
                    maxAttempts: 3,
                    backoff: { baseMs: 1, maxMs: 1 },
                    inboxTtl: { field: "expiresAt", ms: 86_400_000 },
                    objectTtl,
                    drainBudget: { messages: 100, ms: 60_000 },
                    deleteField: () => FieldValue.delete(),
                    runtime: { now: () => nowMs },
                });
            return { at, kicks };
        }

        it("a restart whose clock is behind repeats seqs: their kicks are refused, and the sweep recovers them", async () => {
            const { at, kicks } = instances();
            const nowMs = Date.now();
            const ahead = at(nowMs);
            for (const n of [1, 2, 3]) await ahead.send("k", { n }, { id: `a${n}` });
            await ahead.drain("k", async () => undefined);
            await ahead.purge("k");
            expect(kicks).toHaveLength(3);

            const behind = at(nowMs - 1);
            for (const n of [1, 2, 3, 4]) expect(await behind.send("k", { n }, { id: `b${n}` })).toBe("accepted");

            // Seqs nowMs..nowMs+3 reuse the first generation's nowMs+1..nowMs+3: one new id, three refused.
            expect(await counters("k")).toMatchObject({ lastSeq: nowMs + 3, processedSeq: nowMs - 1 });
            expect(kicks).toHaveLength(4);

            // The kick that exists runs the backlog; here it is lost too, and only the sweep is left.
            kicks.length = 0;
            const later = at(nowMs + SWEEP.olderThanMs);
            expect(await later.sweep(SWEEP)).toMatchObject({ scanned: 1, rekicked: 1 });
            expect(kicks).toHaveLength(1);
            const seen: number[] = [];
            expect(await later.drain("k", async (_ctx, msg) => void seen.push(msg.n))).toEqual({
                outcome: "idle", processed: 4, deadLettered: 0,
            });
            expect(seen).toEqual([1, 2, 3, 4]);
        });

        it("concurrent first sends get one object and one task each", async () => {
            const { at, kicks } = instances();
            const nowMs = Date.now();
            const outcomes = await Promise.all(
                Array.from({ length: 6 }, (_, n) => at(nowMs + n).send("k", { n }, { id: `m${n}` })),
            );

            expect(outcomes).toEqual(Array(6).fill("accepted"));
            expect(new Set(kicks.map((k) => k.id)).size).toBe(6);
            const { lastSeq, processedSeq } = await counters("k");
            expect(lastSeq - processedSeq).toBe(6);
            const expected = Array.from({ length: 6 }, (_, i) =>
                taskIdFor({ prefix: "objects", key: `k\ns${processedSeq + i + 1}`, window: "forever" }, nowMs),
            );
            expect(kicks.map((k) => k.id).sort()).toEqual(expected.sort());
        });
    });

    describe("a stray document at the next seq", () => {
        it.each(["fifo", "latest"] as const)("%s: a send overwrites it instead of wedging the key", async (mode) => {
            const { object } = newObject(mode);
            await object.send("k", { n: 1 }, { id: "m1" });
            const next = mode === "fifo" ? "000000000002" : "latest";
            await db.doc(`objects/k/mailbox/${next}`).set({ seq: 99, id: "leftover", msg: { n: 99 }, attempts: 7 });

            expect(await object.send("k", { n: 2 }, { id: "m2" })).toBe("accepted");

            const doc = await db.doc(`objects/k/mailbox/${next}`).get();
            expect(doc.data()).toMatchObject({ seq: 2, id: "m2", msg: { n: 2 }, attempts: 0 });
            const seen: number[] = [];
            await object.drain("k", async (_ctx, msg) => void seen.push(msg.n));
            expect(seen).toEqual(mode === "fifo" ? [1, 2] : [2]);
            expect(await mailbox("k")).toEqual([]);
        });

        it("concurrent sends over a stray still take distinct seqs and lose nothing", async () => {
            const { object } = newObject();
            await object.send("k", { n: 0 }, { id: "m0" });
            await db.doc("objects/k/mailbox/000000000002").set({ seq: 99, id: "leftover", msg: { n: 99 } });

            const N = 5;
            const sent = await Promise.all(Array.from({ length: N }, (_, i) => object.send("k", { n: i + 1 }, { id: `m${i + 1}` })));

            expect(sent).toEqual(Array(N).fill("accepted"));
            expect(await counters("k")).toMatchObject({ lastSeq: N + 1 });
            const seen: number[] = [];
            await object.drain("k", async (_ctx, msg) => void seen.push(msg.n));
            expect(seen.sort()).toEqual([0, 1, 2, 3, 4, 5]);
        });
    });

    describe("sweep", () => {
        const noop: KeyedHandler<Msg, State> = async () => undefined;
        /** An object whose clock is the test's, with a shared stand-in queue, so a send can leave no kick. */
        function clocked(name = "objects", extra: Partial<KeyedObjectConfig<Msg>> = {}) {
            let clock = Date.now();
            const { queue, kicks } = fakeQueue();
            const object = defineKeyedObject<Msg, State>({
                db,
                name,
                mode: "fifo",
                queue,
                leaseMs: LEASE,
                maxAttempts: 3,
                backoff: { baseMs: 2 * SWEEP.olderThanMs, maxMs: 2 * SWEEP.olderThanMs },
                inboxTtl: { field: "expiresAt", ms: 86_400_000 },
                drainBudget: { messages: 100, ms: 60_000 },
                deleteField: () => FieldValue.delete(),
                runtime: { now: () => clock },
                ...extra,
            });
            return { object, kicks, tick: (ms: number) => void (clock += ms), now: () => clock };
        }
        const lost = (object: ReturnType<typeof clocked>["object"], key: string, n = 1) =>
            object.send(key, { n }, { id: `${key}-${n}`, kick: false });

        it("recovers a lost kick past the threshold, and only then", async () => {
            const { object, kicks, tick } = clocked();
            await lost(object, "k");

            expect(await object.sweep(SWEEP)).toMatchObject({ scanned: 1, recent: 1, rekicked: 0 });
            expect(kicks).toEqual([]);

            tick(SWEEP.olderThanMs);
            expect(await object.sweep(SWEEP)).toMatchObject({ scanned: 1, rekicked: 1 });
            expect(kicks).toEqual([expect.objectContaining({ key: "k" })]);
            expect(await object.drain("k", noop)).toMatchObject({ outcome: "idle", processed: 1 });
            expect(await object.sweep(SWEEP)).toMatchObject({ scanned: 0 });
        });

        it("recovers a vanished holder once its lease expired, with no kick armed by the refused drain", async () => {
            const { object, kicks, tick } = clocked();
            await lost(object, "k");

            // Holder dies inside the handler: the lease stays, and no platform retry follows.
            const died = deferred();
            const inside = deferred();
            const holder = object.drain("k", async () => {
                inside.resolve();
                await died.promise;
            });
            await inside.promise;
            expect((await object.drain("k", noop)).outcome).toBe("busy");
            expect(kicks).toEqual([]);

            tick(SWEEP.olderThanMs);
            expect(await object.sweep(SWEEP)).toMatchObject({ rekicked: 1 });
            const seen: number[] = [];
            expect(await object.drain("k", async (ctx) => void seen.push(ctx.attempt))).toMatchObject({
                outcome: "idle", processed: 1,
            });
            expect(seen).toEqual([2]);
            died.resolve();
            expect(await holder).toMatchObject({ outcome: "lost", processed: 0 });
        });

        it("skips a head in backoff, which keeps its due time and task", async () => {
            const { object, kicks, tick } = clocked();
            await object.send("k", { n: 1 }, { id: "m1" });
            await object.drain("k", async () => { throw new Error("boom"); });
            const before = (await db.doc("objects/k/mailbox/000000000001").get()).data();
            const armed = [...kicks];

            tick(SWEEP.olderThanMs + 1);
            expect(await object.sweep(SWEEP)).toMatchObject({ scanned: 1, backingOff: 1, rekicked: 0 });

            expect(kicks).toEqual(armed);
            expect((await db.doc("objects/k/mailbox/000000000001").get()).data()).toEqual(before);
        });

        it("sweeps two objects sharing the group, neither starved by a large backlog in the other", async () => {
            const big = clocked("objects");
            const small = clocked("othersmall");
            const prefix = clocked("objects2");
            // More keys in `objects` than either limit, and one key with a long backlog.
            for (let i = 0; i < 12; i++) await lost(big.object, `key${String(i).padStart(2, "0")}`);
            const batch = db.batch();
            for (let n = 1; n <= 300; n++) {
                batch.set(db.doc(`objects/key00/mailbox/${String(n + 1).padStart(12, "0")}`), { seq: n + 1, msg: { n } });
            }
            await batch.commit();
            await lost(small.object, "only");
            await lost(prefix.object, "elsewhere");
            for (const o of [big, small, prefix]) o.tick(SWEEP.olderThanMs);

            expect(await big.object.sweep({ ...SWEEP, limit: 5 })).toMatchObject({ scanned: 5, rekicked: 5, next: "key04" });
            expect(await small.object.sweep({ ...SWEEP, limit: 5 })).toEqual({ scanned: 1, rekicked: 1, coalesced: 0, backingOff: 0, recent: 0, failed: 0 });
            expect(small.kicks.map((k) => k.key)).toEqual(["only"]);
            // The range stops at the name: a longer collection name is another object.
            expect(await prefix.object.sweep({ ...SWEEP, limit: 5 })).toMatchObject({ scanned: 1, rekicked: 1 });
            expect(prefix.kicks.map((k) => k.key)).toEqual(["elsewhere"]);
            // The key with 301 messages took one read: all five kicks are distinct keys.
            expect(big.kicks.map((k) => k.key)).toEqual(["key00", "key01", "key02", "key03", "key04"]);
            // `after` resumes past that key on a real query.
            expect(await big.object.sweep({ ...SWEEP, limit: 5, after: "key04" })).toMatchObject({ scanned: 5, next: "key09" });
            expect(big.kicks.slice(5).map((k) => k.key)).toEqual(["key05", "key06", "key07", "key08", "key09"]);
        });

        it("finds any key the document id allows, whatever its first character", async () => {
            const { object, kicks, tick } = clocked();
            for (const key of ["~tilde", "é-accent", "\u{1F600}-emoji"]) await lost(object, key);
            tick(SWEEP.olderThanMs);

            expect(await object.sweep(SWEEP)).toMatchObject({ scanned: 3, rekicked: 3 });
            expect(kicks.map((k) => k.key).sort()).toEqual(["~tilde", "é-accent", "\u{1F600}-emoji"].sort());
        });

        it("reads only the masked fields from the backend, one document per key", async () => {
            const { object, tick } = clocked();
            for (const n of [1, 2, 3]) await lost(object, "a", n);
            await lost(object, "b");
            tick(SWEEP.olderThanMs);
            const reads = jest.spyOn(Query.prototype, "get");

            await object.sweep(SWEEP);

            const snaps = await Promise.all(reads.mock.results.map((r) => r.value as Promise<QuerySnapshot>));
            reads.mockRestore();
            const docs = snaps.flatMap((snap) => snap.docs);
            expect(docs.map((d) => d.ref.path.split("/")[1])).toEqual(["a", "b"]);
            for (const doc of docs) expect(Object.keys(doc.data()).sort()).toEqual(["createdAt"]);
        });
    });

    it("concurrent ctx.once calls with one name run the step once and journal it once", async () => {
        const { object } = newObject();
        await object.send("k", { n: 1 }, { id: "m1" });
        let executions = 0;
        const step = async () => {
            executions++;
            await sleep(20);
            return { receipt: `r${executions}` };
        };
        let results: unknown[] = [];
        let journal: unknown;

        const result = await object.drain("k", async (ctx) => {
            results = await Promise.all([ctx.once("charge", step), ctx.once("charge", step), ctx.once("charge", step)]);
            journal = (await db.doc("objects/k/mailbox/000000000001").get()).get("journal");
        });

        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
        expect(executions).toBe(1);
        expect(results).toEqual([{ receipt: "r1" }, { receipt: "r1" }, { receipt: "r1" }]);
        expect(journal).toEqual({ charge: { json: JSON.stringify({ receipt: "r1" }) } });
    });

    it("a drain over an expired backlog stops at the time budget and re-kicks, and the next drain continues", async () => {
        let clock = Date.now();
        const { object, kicks } = newObject("fifo", {
            runtime: { now: () => clock },
            drainBudget: { messages: 100, ms: 30_000 },
            onExpired: () => void (clock += 30_000),
        });
        for (const n of [1, 2, 3]) await object.send("k", { n }, { id: `m${n}` });
        for (const id of ["000000000001", "000000000002"]) await db.doc(`objects/k/mailbox/${id}`).delete();
        kicks.length = 0;
        const seen: number[] = [];
        const handler: KeyedHandler<Msg, State> = async (_ctx, msg) => void seen.push(msg.n);

        expect(await object.drain("k", handler)).toEqual({ outcome: "budget", processed: 0, deadLettered: 0 });
        expect(await counters("k")).toMatchObject({ processedSeq: 1 });
        expect(kicks).toHaveLength(1);

        expect((await object.drain("k", handler)).outcome).toBe("budget");
        expect(await object.drain("k", handler)).toEqual({ outcome: "idle", processed: 1, deadLettered: 0 });
        expect(kicks).toHaveLength(2);
        expect(seen).toEqual([3]);
    });
});
