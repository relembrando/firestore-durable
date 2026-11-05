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

import { FieldValue, getFirestore } from "firebase-admin/firestore";
import { setTimeout as sleep } from "timers/promises";
import { defineKeyedObject, defineTaskQueue, LeaseLostError } from "../src";
import type { KeyedDeadLetter, KeyedHandler, KeyedObjectConfig, KeyedObjectKick, KeyedObjectMode } from "../src";

const db = getFirestore();
const LEASE = 60_000;
/** Upper bound on one held handler, so a stuck drain cannot hang the suite. */
const HOLD_DEADLINE_MS = 5_000;

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
});
