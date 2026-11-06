/**
 * `src/keyed_object.ts` against the in-memory transactional fake (buffered
 * writes applied all-or-nothing, `create` failing on an existing doc). The
 * fake runs transactions one at a time, so races are staged explicitly: a
 * concurrent actor runs from INSIDE the handler, between the pick and the
 * completion. `durable_keyed_object.emulator.test.ts` proves the same
 * properties under real concurrency.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { fakeDb, resetStore, store } from "./helpers/fake_firestore_tx";
import { defineKeyedObject, defineTaskQueue, LeaseLostError, taskIdFor } from "../src";
import type { KeyedDeadLetter, KeyedHandler, KeyedObjectConfig, KeyedObjectKick, KeyedObjectMode } from "../src";

const NOW = 1_700_000_000_000;
const LEASE = 60_000;
const TTL = 86_400_000;
const DELETE = { __delete: true };
const object = (key = "k") => `objects/${key}`;
const item = (seq: number | "latest", key = "k") =>
    `objects/${key}/mailbox/${seq === "latest" ? "latest" : String(seq).padStart(12, "0")}`;
const dead = (seq: number, key = "k") => `objects/${key}/mailboxDead/${String(seq).padStart(12, "0")}`;

type Msg = { n: number };
type State = { count: number };

let clock = NOW;
let tasks: { key: string; id: string; delaySeconds?: number }[] = [];
let enqueueFailure: Error | undefined;
const taskIds = new Set<string>();

/** Cloud Tasks stand-in: a reused id is refused even after the task ran. */
const queue = defineTaskQueue<KeyedObjectKick>({
    enqueue: async (payload, options) => {
        if (enqueueFailure) throw enqueueFailure;
        if (taskIds.has(options.id)) throw Object.assign(new Error("6 ALREADY_EXISTS"), { code: 6 });
        taskIds.add(options.id);
        tasks.push({ key: payload.key, id: options.id, delaySeconds: options.scheduleDelaySeconds });
    },
    runtime: { now: () => clock },
});

/** The id `kick(key, tag)` must produce, pinned against `taskIdFor`. */
const kickId = (tag: string, key = "k") => taskIdFor({ prefix: "objects", key: `${key}\n${tag}`, window: "forever" }, clock);

let instances = 0;
function make(mode: KeyedObjectMode = "fifo", extra: Partial<KeyedObjectConfig<Msg>> = {}) {
    const instance = ++instances;
    let n = 0;
    return defineKeyedObject<Msg, State>({
        db: fakeDb,
        name: "objects",
        mode,
        queue,
        leaseMs: LEASE,
        maxAttempts: 3,
        backoff: { baseMs: 10_000, maxMs: 60_000 },
        inboxTtl: { field: "expiresAt", ms: TTL },
        drainBudget: { messages: 10, ms: 30_000 },
        deleteField: () => DELETE,
        runtime: { now: () => clock, newToken: () => `token-${instance}.${++n}` },
        ...extra,
    });
}

const noop: KeyedHandler<Msg, State> = async () => undefined;

const SWEEP = { olderThanMs: 15 * 60 * 1000, limit: 100 };
const EMPTY = { scanned: 0, rekicked: 0, coalesced: 0, backingOff: 0, recent: 0, failed: 0, truncated: false };
/** The id a sweep kick must produce now: a time window, not `"forever"`. */
const sweepId = (key: string) =>
    taskIdFor({ prefix: "objects", key: `${key}\nsweep`, window: { seconds: SWEEP.olderThanMs / 1000 } }, clock);

/** Records what the sweep's query chain asks for and gets back. */
function spyReads() {
    const masks: unknown[] = [];
    const results: any[] = [];
    const wrap = (query: any): any =>
        new Proxy(query, {
            get(target, prop) {
                const value = target[prop];
                if (typeof value !== "function") return value;
                return (...args: unknown[]) => {
                    if (prop === "select") masks.push(args);
                    const out = value(...args);
                    if (prop !== "get") return wrap(out);
                    return out.then((snap: any) => {
                        results.push(snap);
                        return snap;
                    });
                };
            },
        });
    const group = fakeDb.collectionGroup;
    const spy = jest.spyOn(fakeDb, "collectionGroup").mockImplementation((...args: unknown[]) => wrap(group(...args)));
    return {
        masks,
        docs: () => results.reduce((n, snap) => n + snap.size, 0),
        fields: () => results.flatMap((snap) => snap.docs.flatMap((doc: any) => Object.keys(doc.data()))),
        restore: () => spy.mockRestore(),
    };
}

beforeEach(() => {
    resetStore();
    clock = NOW;
    tasks = [];
    enqueueFailure = undefined;
    taskIds.clear();
    instances = 0;
});

describe("defineKeyedObject config", () => {
    it.each([
        [{ name: "bad/name" }, /name must match/],
        [{ mode: "lifo" as KeyedObjectMode }, /unknown mode/],
        [{ leaseMs: 0 }, /leaseMs/],
        [{ maxAttempts: 0 }, /maxAttempts/],
        [{ maxAttempts: 1.5 }, /maxAttempts/],
        [{ backoff: { baseMs: 0, maxMs: 1 } }, /backoff.baseMs/],
        [{ backoff: { baseMs: 1, maxMs: -1 } }, /backoff.maxMs/],
        [{ inboxTtl: { field: "", ms: TTL } }, /inboxTtl.field/],
        [{ inboxTtl: { field: "expiresAt", ms: 0 } }, /inboxTtl.ms/],
        [{ drainBudget: { messages: 0, ms: 1 } }, /drainBudget.messages/],
        [{ drainBudget: { messages: 1, ms: 0 } }, /drainBudget.ms/],
    ])("rejects %p", (extra, message) => {
        expect(() => make("fifo", extra as Partial<KeyedObjectConfig<Msg>>)).toThrow(message);
    });

    it("doubles the backoff from the base, capped", () => {
        expect([1, 2, 3, 4, 5, 1000].map((a) => make().backoffMs(a))).toEqual([10_000, 20_000, 40_000, 60_000, 60_000, 60_000]);
    });

    it("rejects a key that is not one document id, and a send with no id", async () => {
        await expect(make().send("a/b", { n: 1 }, { id: "x" })).rejects.toThrow(/invalid key/);
        await expect(make().send("", { n: 1 }, { id: "x" })).rejects.toThrow(/invalid key/);
        await expect(make().send("k", { n: 1 }, { id: "" })).rejects.toThrow(/needs an id/);
    });
});

describe("send", () => {
    it("fifo: appends the message, counts it, and kicks once after the commit", async () => {
        const obj = make();
        expect(await obj.send("k", { n: 1 }, { id: "m1" })).toBe("accepted");
        expect(await obj.send("k", { n: 2 }, { id: "m2" })).toBe("accepted");

        expect(store.get(object())).toEqual({ lastSeq: 2, processedSeq: 0 });
        expect(store.get(item(1))).toEqual({
            seq: 1, id: "m1", msg: { n: 1 }, attempts: 0, createdAt: new Date(NOW), expiresAt: new Date(NOW + TTL),
        });
        expect(store.get(item(2))).toMatchObject({ seq: 2, id: "m2" });
        expect(tasks.map((t) => t.id)).toEqual([kickId("s1"), kickId("s2")]);
        expect(tasks[0]).toMatchObject({ key: "k" });
        expect(tasks[0].delaySeconds).toBeUndefined();
    });

    it("pins the wire format of kick ids against the task id scheme", () => {
        expect(kickId("s1")).toMatch(/^objects-[0-9a-f]{32}$/);
    });

    it("a repeated id appends nothing, and re-kicks the same task id while still pending", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        const before = store.get(object());

        expect(await obj.send("k", { n: 99 }, { id: "m1" })).toBe("duplicate");

        expect(store.get(object())).toEqual(before);
        expect(store.get(item(1))).toMatchObject({ msg: { n: 1 } });
        expect(tasks.map((t) => t.id)).toEqual([kickId("s1")]); // refused as ALREADY_EXISTS, not a second task
    });

    it("a repeated older id re-kicks the newest pending message, without a new task", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.send("k", { n: 2 }, { id: "m2" });

        expect(await obj.send("k", { n: 1 }, { id: "m1" })).toBe("duplicate");

        expect(tasks.map((t) => t.id)).toEqual([kickId("s1"), kickId("s2")]);
    });

    it("a concurrent commit of the same id is a duplicate", async () => {
        const obj = make();
        // The id's marker is committed between this send's read and its commit.
        const original = fakeDb.runTransaction;
        const spy = jest.spyOn(fakeDb, "runTransaction").mockImplementationOnce(async (fn: any) => {
            await original(async (tx: any) => {
                await fn(tx);
            });
            return original(fn);
        });
        await expect(obj.send("k", { n: 1 }, { id: "m1" })).resolves.toBe("duplicate");
        spy.mockRestore();
        expect(store.get(object())).toEqual({ lastSeq: 1, processedSeq: 0 });
    });

    it.each(["fifo", "latest"] as const)("%s: a stray document at the next seq cannot wedge the key", async (mode) => {
        const obj = make(mode);
        await obj.send("k", { n: 1 }, { id: "m1" });
        const next = mode === "fifo" ? item(2) : item("latest");
        store.set(next, { seq: 99, id: "leftover", msg: { n: 99 }, attempts: 7, notBefore: new Date(NOW + TTL), journal: { x: {} } });

        expect(await obj.send("k", { n: 2 }, { id: "m2" })).toBe("accepted");

        expect(store.get(next)).toEqual({
            seq: 2, id: "m2", msg: { n: 2 }, attempts: 0, createdAt: new Date(NOW), expiresAt: new Date(NOW + TTL),
        });
        const seen: number[] = [];
        expect(await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n))).toMatchObject({ outcome: "idle" });
        expect(seen).toEqual(mode === "fifo" ? [1, 2] : [2]);
        expect(store.has(next)).toBe(false);
    });

    it("heals a kick that failed after the commit when the send is retried", async () => {
        const obj = make();
        enqueueFailure = new Error("tasks down");
        await expect(obj.send("k", { n: 1 }, { id: "m1" })).rejects.toThrow("tasks down");
        expect(store.get(item(1))).toBeDefined();
        expect(tasks).toEqual([]);

        enqueueFailure = undefined;
        expect(await obj.send("k", { n: 1 }, { id: "m1" })).toBe("duplicate");

        expect(tasks.map((t) => t.id)).toEqual([kickId("s1")]);
    });

    it("a duplicate of a message already processed does not kick", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.drain("k", noop);
        tasks = [];

        expect(await obj.send("k", { n: 1 }, { id: "m1" })).toBe("duplicate");

        expect(tasks).toEqual([]);
    });

    it("kick: false commits without enqueueing", async () => {
        expect(await make().send("k", { n: 1 }, { id: "m1", kick: false })).toBe("accepted");
        expect(tasks).toEqual([]);
        expect(store.get(item(1))).toBeDefined();
    });

    it("latest: replaces the pending payload whole, journal and attempts included", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        const doc = store.get(item("latest"))!;
        store.set(item("latest"), { ...doc, attempts: 2, journal: { step: { json: "1" } } });

        await obj.send("k", { n: 2 }, { id: "v2" });

        expect(store.get(item("latest"))).toEqual({
            seq: 2, id: "v2", msg: { n: 2 }, attempts: 0, createdAt: new Date(NOW), expiresAt: new Date(NOW + TTL),
        });
        expect(store.get(object())).toMatchObject({ lastSeq: 2, processedSeq: 0 });
        expect(tasks.map((t) => t.id)).toEqual([kickId("s1"), kickId("s2")]);
    });
});

describe("drain", () => {
    it("runs messages in seq order, advances past each, and releases the lease", async () => {
        const obj = make();
        for (const n of [1, 2, 3]) await obj.send("k", { n }, { id: `m${n}` });
        const seen: unknown[] = [];

        const result = await obj.drain("k", async (ctx, msg) => {
            seen.push([ctx.key, ctx.seq, ctx.id, ctx.attempt, msg.n]);
        });

        expect(seen).toEqual([["k", 1, "m1", 1, 1], ["k", 2, "m2", 1, 2], ["k", 3, "m3", 1, 3]]);
        expect(result).toEqual({ outcome: "idle", processed: 3, deadLettered: 0 });
        expect(store.get(object())).toMatchObject({ lastSeq: 3, processedSeq: 3, lease: null });
        for (const seq of [1, 2, 3]) expect(store.has(item(seq))).toBe(false);
    });

    it("is idle on a key nothing was ever sent to, without creating it", async () => {
        expect(await make().drain("never", noop)).toEqual({ outcome: "idle", processed: 0, deadLettered: 0 });
        expect(store.has(object("never"))).toBe(false);
    });

    it("is busy while another drain holds the key, and that drain sees the message", async () => {
        const holder = make();
        await holder.send("k", { n: 1 }, { id: "m1" });
        const seen: number[] = [];
        let busy: unknown;

        const result = await holder.drain("k", async (ctx, msg) => {
            seen.push(msg.n);
            if (msg.n === 1) {
                await make().send("k", { n: 2 }, { id: "m2" });
                busy = await make().drain("k", noop, { owner: { id: "other", epoch: 0 } });
            }
        });

        expect(busy).toEqual({ outcome: "busy", processed: 0, deadLettered: 0 });
        expect(seen).toEqual([1, 2]);
        expect(result).toMatchObject({ outcome: "idle", processed: 2 });
    });

    it("a retry of the same task (higher epoch) takes a live lease over; the same epoch is busy", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        const outcomes: unknown[] = [];

        await obj.drain(
            "k",
            async () => {
                outcomes.push((await make().drain("k", noop, { owner: { id: "t", epoch: 0 } })).outcome);
                outcomes.push((await make().drain("k", noop, { owner: { id: "t", epoch: 1 } })).outcome);
            },
            { owner: { id: "t", epoch: 0 } },
        );

        expect(outcomes).toEqual(["busy", "idle"]);
    });

    it("a send racing the release is re-kicked by the releaser, under a fresh id", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        // A send that lands after the holder's last read but before its release:
        // staged by committing it from the lease's own release transaction.
        const release = fakeDb.runTransaction;
        let calls = 0;
        const spy = jest.spyOn(fakeDb, "runTransaction").mockImplementation(async (fn: any) => {
            calls++;
            if (calls === 5) { // the release, after acquire + pick + complete + empty pick
                spy.mockImplementation(release);
                await make().send("k", { n: 2 }, { id: "m2" });
            }
            return release(fn);
        });
        tasks = [];
        taskIds.clear();

        const result = await obj.drain("k", noop);
        spy.mockRestore();

        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
        expect(store.get(object())).toMatchObject({ lastSeq: 2, processedSeq: 1 });
        expect(tasks.map((t) => t.id)).toEqual([kickId("s2"), kickId("n1.2")]);
    });

    it("stops at the message budget and re-kicks, once per backlog position", async () => {
        const obj = make("fifo", { drainBudget: { messages: 2, ms: 30_000 } });
        for (const n of [1, 2, 3]) await obj.send("k", { n }, { id: `m${n}` });
        tasks = [];

        expect(await obj.drain("k", noop)).toEqual({ outcome: "budget", processed: 2, deadLettered: 0 });

        expect(store.get(object())).toMatchObject({ processedSeq: 2 });
        expect(tasks.map((t) => t.id)).toEqual([kickId("n2.3")]);
    });

    it("reports idle, not budget, when the budget ran out on the last message", async () => {
        const obj = make("fifo", { drainBudget: { messages: 1, ms: 30_000 } });
        await obj.send("k", { n: 1 }, { id: "m1" });
        tasks = [];

        expect(await obj.drain("k", noop)).toEqual({ outcome: "idle", processed: 1, deadLettered: 0 });
        expect(tasks).toEqual([]);
    });

    it("stops at the time budget", async () => {
        const obj = make();
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        const seen: number[] = [];

        const result = await obj.drain("k", async (_ctx, msg) => {
            seen.push(msg.n);
            clock += 30_000;
        });

        expect(seen).toEqual([1]);
        expect(result.outcome).toBe("budget");
    });

    it("always runs at least one message, however slow the start", async () => {
        for (const n of [1, 2]) await make().send("k", { n }, { id: `m${n}` });
        // The first reading is the drain's start; every later one is past the budget.
        let reads = 0;
        const obj = make("fifo", { runtime: { now: () => NOW + (reads++ ? 10_000 : 0) }, drainBudget: { messages: 10, ms: 1 } });
        const seen: number[] = [];

        const result = await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));

        expect(seen).toEqual([1]);
        expect(result.outcome).toBe("budget");
    });

    it("skips a message the TTL policy deleted before it ran", async () => {
        const obj = make();
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        store.delete(item(1));
        const seen: number[] = [];

        const result = await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));

        expect(seen).toEqual([2]);
        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
        expect(store.get(object())).toMatchObject({ processedSeq: 2 });
    });

    describe("an expired backlog", () => {
        async function backlog(obj: ReturnType<typeof make>, expired: number, live = 0) {
            for (let n = 1; n <= expired + live; n++) await obj.send("k", { n }, { id: `m${n}` });
            for (let n = 1; n <= expired; n++) store.delete(item(n));
            tasks = [];
        }

        it("stops at the time budget and re-kicks, like a budget stop after handled messages", async () => {
            const gone: number[] = [];
            const obj = make("fifo", {
                drainBudget: { messages: 10, ms: 30_000 },
                onExpired: ({ seq }) => {
                    gone.push(seq);
                    clock += 30_000;
                },
            });
            await backlog(obj, 3, 1);
            const handler = jest.fn(noop);

            expect(await obj.drain("k", handler)).toEqual({ outcome: "budget", processed: 0, deadLettered: 0 });

            expect(gone).toEqual([1]);
            expect(handler).not.toHaveBeenCalled();
            expect(store.get(object())).toMatchObject({ processedSeq: 1, lease: null });
            expect(tasks).toEqual([{ key: "k", id: kickId("n1.4") }]);
        });

        it("makes progress on every re-kick, under a fresh id each, until the backlog is gone", async () => {
            const obj = make("fifo", { onExpired: () => void (clock += 30_000) });
            await backlog(obj, 3, 1);
            const seen: number[] = [];
            const handler: KeyedHandler<Msg, State> = async (_ctx, msg) => void seen.push(msg.n);

            const outcomes = [];
            for (let i = 0; i < 4; i++) outcomes.push((await obj.drain("k", handler)).outcome);

            expect(outcomes).toEqual(["budget", "budget", "budget", "idle"]);
            expect(seen).toEqual([4]);
            expect(tasks.map((t) => t.id)).toEqual([kickId("n1.4"), kickId("n2.4"), kickId("n3.4")]);
            expect(store.get(object())).toMatchObject({ processedSeq: 4 });
        });

        it("reports idle, with no re-kick, when the time ran out on the last skip", async () => {
            const obj = make("fifo", { onExpired: () => void (clock += 30_000) });
            await backlog(obj, 1);

            expect(await obj.drain("k", noop)).toEqual({ outcome: "idle", processed: 0, deadLettered: 0 });

            expect(tasks).toEqual([]);
        });

        it("does not spend the message budget: skips never take a handler run away", async () => {
            const obj = make("fifo", { drainBudget: { messages: 1, ms: 30_000 } });
            await backlog(obj, 3, 2);
            const seen: number[] = [];

            const result = await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));

            expect(seen).toEqual([4]);
            expect(result).toEqual({ outcome: "budget", processed: 1, deadLettered: 0 });
            expect(store.get(object())).toMatchObject({ processedSeq: 4 });
            expect(tasks.map((t) => t.id)).toEqual([kickId("n4.5")]);
        });

        it("a slow start still skips one per drain, instead of never moving", async () => {
            for (const n of [1, 2]) await make().send("k", { n }, { id: `m${n}` });
            for (const n of [1, 2]) store.delete(item(n));
            let reads = 0;
            const obj = make("fifo", { runtime: { now: () => NOW + (reads++ ? 10_000 : 0) }, drainBudget: { messages: 10, ms: 1 } });

            expect((await obj.drain("k", noop)).outcome).toBe("budget");

            expect(store.get(object())).toMatchObject({ processedSeq: 1 });
        });
    });
});

describe("ctx.tx", () => {
    it("commits the handler's writes, the new state and the seq advance together", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });

        await obj.drain("k", async (ctx) => {
            await ctx.tx((tx, state) => {
                tx.set(fakeDb.doc("domain/d"), { from: ctx.seq });
                return { count: (state?.count ?? 0) + 1 };
            });
        });

        expect(store.get("domain/d")).toEqual({ from: 1 });
        expect(store.get(object())).toMatchObject({ processedSeq: 1, state: { count: 1 } });
        expect(store.has(item(1))).toBe(false);
    });

    it("exposes the state at pick time, and reads it fresh inside the transaction", async () => {
        const obj = make();
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        const seen: unknown[] = [];

        await obj.drain("k", async (ctx) => {
            seen.push(ctx.state);
            await ctx.tx((_tx, state) => ({ count: (state?.count ?? 0) + 1 }));
        });

        expect(seen).toEqual([undefined, { count: 1 }]);
        expect(store.get(object())).toMatchObject({ state: { count: 2 } });
    });

    it("leaves the state untouched when fn returns undefined", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        store.set(object(), { ...store.get(object())!, state: { count: 7 } });

        await obj.drain("k", async (ctx) => {
            await ctx.tx(() => undefined);
        });

        expect(store.get(object())).toMatchObject({ processedSeq: 1, state: { count: 7 } });
    });

    it("may run once per message", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        let second: unknown;

        await obj.drain("k", async (ctx) => {
            await ctx.tx(() => undefined);
            second = await ctx.tx(() => undefined).catch((err: unknown) => err);
        });

        expect(String(second)).toMatch(/already committed/);
        expect(store.get(object())).toMatchObject({ processedSeq: 1 });
    });

    it("is refused once the lease was taken over: no write, no advance, and the drain reports lost", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        let refused: unknown;

        const result = await obj.drain("k", async (ctx) => {
            // A second holder takes the lapsed lease and stays inside.
            clock += LEASE + 1;
            const other = make();
            await other.drain(
                "k",
                async () => {
                    refused = await ctx.tx((tx) => {
                        tx.set(fakeDb.doc("domain/late"), { x: 1 });
                        return { count: 1 };
                    }).catch((err: unknown) => err);
                    throw refused;
                },
            ).catch(() => undefined);
            throw refused;
        });

        expect(refused).toBeInstanceOf(LeaseLostError);
        expect(store.has("domain/late")).toBe(false);
        expect(result).toEqual({ outcome: "lost", processed: 0, deadLettered: 0 });
    });

    it("a throw after the commit keeps the message processed and surfaces the error", async () => {
        const obj = make();
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        const seen: number[] = [];
        const handler: KeyedHandler<Msg, State> = async (ctx, msg) => {
            seen.push(msg.n);
            await ctx.tx(() => undefined);
            if (msg.n === 1) throw new Error("after the commit");
        };

        await expect(obj.drain("k", handler)).rejects.toThrow("after the commit");

        expect(store.get(object())).toMatchObject({ processedSeq: 1, lease: null });
        // The platform retry of the same task continues with the next message.
        expect(await obj.drain("k", handler)).toMatchObject({ outcome: "idle", processed: 1 });
        expect(seen).toEqual([1, 2]);
    });
});

describe("ctx.once", () => {
    it("journals the result on the message; a redelivery returns it without re-running", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        const step = jest.fn(async () => ({ receipt: "r1", at: [1, 2] }));
        const results: unknown[] = [];
        const handler: KeyedHandler<Msg, State> = async (ctx) => {
            results.push(await ctx.once("charge", step));
            if (ctx.attempt === 1) throw new Error("later step failed");
        };

        expect((await obj.drain("k", handler)).outcome).toBe("retrying");
        expect(store.get(item(1))).toMatchObject({ journal: { charge: { json: JSON.stringify({ receipt: "r1", at: [1, 2] }) } } });
        clock += 10_000;
        expect((await obj.drain("k", handler)).outcome).toBe("idle");

        expect(step).toHaveBeenCalledTimes(1);
        expect(results).toEqual([{ receipt: "r1", at: [1, 2] }, { receipt: "r1", at: [1, 2] }]);
    });

    it("journals undefined as undefined, and returns a step once within one attempt", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        const step = jest.fn(async () => undefined);
        const results: unknown[] = [];

        await obj.drain("k", async (ctx) => {
            results.push(await ctx.once("void", step), await ctx.once("void", step));
        });

        expect(step).toHaveBeenCalledTimes(1);
        expect(results).toEqual([undefined, undefined]);
    });

    it("rejects a name that would not be one field", async () => {
        const obj = make("fifo", { maxAttempts: 1 });
        await obj.send("k", { n: 1 }, { id: "m1" });
        let error: unknown;

        await obj.drain("k", async (ctx) => {
            error = await ctx.once("a.b", async () => 1).catch((err: unknown) => err);
        });

        expect(String(error)).toMatch(/invalid once name/);
    });

    it("latest: a step that finishes after a newer send does not touch the newer message", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        const seen: number[] = [];

        await obj.drain("k", async (ctx, msg) => {
            seen.push(msg.n);
            await ctx.once("step", async () => {
                if (msg.n === 1) await make("latest").send("k", { n: 2 }, { id: "v2" });
                if (msg.n === 1) expect(store.get(item("latest"))).not.toHaveProperty("journal");
                return msg.n;
            });
            if (msg.n === 1) expect(store.get(item("latest"))).not.toHaveProperty("journal");
        });

        expect(seen).toEqual([1, 2]);
    });

    describe("concurrent calls with one name", () => {
        function gate<T>() {
            let open!: (value: T) => void;
            let fail!: (err: unknown) => void;
            const promise = new Promise<T>((resolve, reject) => {
                open = resolve;
                fail = reject;
            });
            return { promise, open, fail };
        }

        it("share one execution and one result, journaled once", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            const release = gate<{ receipt: string }>();
            const step = jest.fn(() => release.promise);
            const journalWrites = jest.spyOn(fakeDb, "runTransaction");
            let results: unknown[] = [];

            await obj.drain("k", async (ctx) => {
                const calls = [ctx.once("charge", step), ctx.once("charge", step), ctx.once("charge", step)];
                const before = journalWrites.mock.calls.length;
                release.open({ receipt: "r1" });
                results = await Promise.all(calls);
                expect(journalWrites.mock.calls.length - before).toBe(1);
            });
            journalWrites.mockRestore();

            expect(step).toHaveBeenCalledTimes(1);
            expect(results).toEqual([{ receipt: "r1" }, { receipt: "r1" }, { receipt: "r1" }]);
        });

        it("share a rejection: every caller sees the same error, and nothing is journaled", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            const release = gate<number>();
            const step = jest.fn(() => release.promise);
            const boom = new Error("provider down");
            let settled: PromiseSettledResult<number>[] = [];

            const result = await obj.drain("k", async (ctx) => {
                const calls = Promise.allSettled([ctx.once("charge", step), ctx.once("charge", step)]);
                release.fail(boom);
                settled = await calls;
                throw boom;
            });

            expect(step).toHaveBeenCalledTimes(1);
            expect(settled).toEqual([{ status: "rejected", reason: boom }, { status: "rejected", reason: boom }]);
            expect(result.outcome).toBe("retrying");
            expect(store.get(item(1))).not.toHaveProperty("journal");
        });

        it("run again when called after the shared failure settled, as a sequential retry does", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            const step = jest.fn<Promise<string>, []>()
                .mockRejectedValueOnce(new Error("flaky"))
                .mockResolvedValueOnce("second try");
            let later: unknown;

            await obj.drain("k", async (ctx) => {
                await ctx.once("charge", step).catch(() => undefined);
                later = await ctx.once("charge", step);
            });

            expect(step).toHaveBeenCalledTimes(2);
            expect(later).toBe("second try");
        });

        it("a step that throws synchronously rejects its callers and leaves nothing in flight", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            let calls = 0;
            const step = () => {
                if (++calls === 1) throw new Error("sync");
                return Promise.resolve("ok");
            };
            let settled: PromiseSettledResult<string>[] = [];
            let later: unknown;

            await obj.drain("k", async (ctx) => {
                settled = await Promise.allSettled([ctx.once("charge", step)]);
                later = await ctx.once("charge", step);
            });

            expect(settled[0]).toMatchObject({ status: "rejected" });
            expect(later).toBe("ok");
            expect(calls).toBe(2);
        });

        it("different names still run independently", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            const a = jest.fn(async () => "a");
            const b = jest.fn(async () => "b");
            let results: unknown[] = [];

            await obj.drain("k", async (ctx) => {
                results = await Promise.all([ctx.once("a", a), ctx.once("b", b)]);
                expect(store.get(item(1))).toMatchObject({ journal: { a: { json: '"a"' }, b: { json: '"b"' } } });
            });

            expect(results).toEqual(["a", "b"]);
            expect(a).toHaveBeenCalledTimes(1);
            expect(b).toHaveBeenCalledTimes(1);
        });

        it("a redelivery returns the journaled result to all of them without re-running", async () => {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            const step = jest.fn(async () => "r1");
            const results: unknown[] = [];
            const handler: KeyedHandler<Msg, State> = async (ctx) => {
                results.push(await Promise.all([ctx.once("charge", step), ctx.once("charge", step)]));
                if (ctx.attempt === 1) throw new Error("later step failed");
            };

            await obj.drain("k", handler);
            clock += 10_000;
            await obj.drain("k", handler);

            expect(step).toHaveBeenCalledTimes(1);
            expect(results).toEqual([["r1", "r1"], ["r1", "r1"]]);
        });
    });
});

describe("failure", () => {
    const failing: KeyedHandler<Msg, State> = async () => {
        throw new Error("boom");
    };

    it("a throw records the attempt, backs off, and arms a delayed retry kick; the head blocks the key", async () => {
        const obj = make();
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        tasks = [];

        expect(await obj.drain("k", failing)).toEqual({ outcome: "retrying", processed: 0, deadLettered: 0 });

        expect(store.get(item(1))).toMatchObject({ attempts: 1, lastError: "Error: boom", notBefore: new Date(NOW + 10_000) });
        expect(store.get(object())).toMatchObject({ processedSeq: 0, lease: null });
        expect(tasks).toEqual([{ key: "k", id: kickId("r1.1"), delaySeconds: 11 }]);
    });

    it("a drain before the due time waits and re-arms the same kick (healing a lost one)", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        enqueueFailure = new Error("tasks down");
        await expect(obj.drain("k", failing)).rejects.toThrow("tasks down");
        enqueueFailure = undefined;
        tasks = [];
        taskIds.clear();
        const handler = jest.fn(noop);

        clock += 4_000;
        expect(await obj.drain("k", handler)).toEqual({ outcome: "waiting", processed: 0, deadLettered: 0 });
        expect(await obj.drain("k", handler)).toEqual({ outcome: "waiting", processed: 0, deadLettered: 0 });

        expect(handler).not.toHaveBeenCalled();
        expect(tasks).toEqual([{ key: "k", id: kickId("r1.1"), delaySeconds: 7 }]);
        expect(store.get(item(1))).toMatchObject({ attempts: 1 });
    });

    it("runs again once due, within the clock-skew tolerance, and clears the due time", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.drain("k", failing);
        const attempts: number[] = [];
        let during: unknown;

        clock += 10_000 - 1_000;
        const result = await obj.drain("k", async (ctx) => {
            attempts.push(ctx.attempt);
            during = store.get(item(1));
        });

        expect(attempts).toEqual([2]);
        expect(during).not.toHaveProperty("notBefore");
        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
    });

    it("dead-letters after maxAttempts, notifies, and continues with the next message", async () => {
        const letters: KeyedDeadLetter<Msg>[] = [];
        const obj = make("fifo", { onDeadLetter: (letter) => void letters.push(letter) });
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        const handled: number[] = [];
        const handler: KeyedHandler<Msg, State> = async (_ctx, msg) => {
            if (msg.n === 1) throw new Error("poison");
            handled.push(msg.n);
        };

        for (const expected of ["retrying", "retrying"]) {
            expect((await obj.drain("k", handler)).outcome).toBe(expected);
            clock += 60_000;
        }
        const last = await obj.drain("k", handler);

        expect(last).toEqual({ outcome: "idle", processed: 1, deadLettered: 1 });
        expect(handled).toEqual([2]);
        expect(letters).toEqual([{ key: "k", seq: 1, id: "m1", msg: { n: 1 }, attempts: 3, lastError: "Error: poison" }]);
        expect(store.get(dead(1))).toMatchObject({
            seq: 1, id: "m1", msg: { n: 1 }, attempts: 3, lastError: "Error: poison", expiresAt: new Date(clock + TTL),
        });
        expect(store.has(item(1))).toBe(false);
        expect(store.get(object())).toMatchObject({ processedSeq: 2 });
    });

    it("a message whose attempts all crashed is dead-lettered at the next pick", async () => {
        const letters: KeyedDeadLetter<Msg>[] = [];
        const obj = make("fifo", { onDeadLetter: (letter) => void letters.push(letter) });
        await obj.send("k", { n: 1 }, { id: "m1" });
        // Three attempts were counted before a handler ran; none recorded an outcome.
        store.set(item(1), { ...store.get(item(1))!, attempts: 3 });
        const handler = jest.fn(noop);

        expect(await obj.drain("k", handler)).toEqual({ outcome: "idle", processed: 0, deadLettered: 1 });

        expect(handler).not.toHaveBeenCalled();
        expect(letters[0]).toMatchObject({ attempts: 3, lastError: "attempt ended without an outcome" });
        expect(store.get(dead(1))).toBeDefined();
    });

    it("counts an attempt before the handler runs, so a crash is not free", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        let during: unknown;

        await obj.drain("k", async () => {
            during = store.get(item(1));
        });

        expect(during).toMatchObject({ attempts: 1 });
    });

    it("a hook that throws does not stall the key", async () => {
        const obj = make("fifo", {
            maxAttempts: 1,
            onDeadLetter: () => {
                throw new Error("logger down");
            },
        });
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `m${n}` });
        const handled: number[] = [];

        const result = await obj.drain("k", async (_ctx, msg) => {
            if (msg.n === 1) throw new Error("poison");
            handled.push(msg.n);
        });

        expect(handled).toEqual([2]);
        expect(result).toEqual({ outcome: "idle", processed: 1, deadLettered: 1 });
    });

    it("truncates a long error", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });

        await obj.drain("k", async () => {
            throw new Error("x".repeat(5_000));
        });

        expect((store.get(item(1))!.lastError as string).length).toBe(1_000);
    });
});

describe("latest mode", () => {
    it("the handler sees only the newest payload", async () => {
        const obj = make("latest");
        for (const n of [1, 2, 3]) await obj.send("k", { n }, { id: `v${n}` });
        const seen: unknown[] = [];

        const result = await obj.drain("k", async (ctx, msg) => void seen.push([ctx.seq, msg.n]));

        expect(seen).toEqual([[3, 3]]);
        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
        expect(store.get(object())).toMatchObject({ lastSeq: 3, processedSeq: 3 });
        expect(store.has(item("latest"))).toBe(false);
    });

    it("finishing an older message keeps a newer one pending, and it runs next", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        const seen: number[] = [];
        let afterFirst: unknown;

        await obj.drain("k", async (ctx, msg) => {
            seen.push(msg.n);
            if (msg.n === 1) {
                await make("latest").send("k", { n: 2 }, { id: "v2" });
                await ctx.tx(() => ({ count: 1 }));
                afterFirst = { object: store.get(object()), item: store.get(item("latest")) };
            }
        });

        expect(afterFirst).toMatchObject({ object: { lastSeq: 2, processedSeq: 1 }, item: { seq: 2 } });
        expect(seen).toEqual([1, 2]);
        expect(store.has(item("latest"))).toBe(false);
    });

    it("a failure of a superseded message records nothing on the newer one, which runs next", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        const seen: number[] = [];

        const result = await obj.drain("k", async (_ctx, msg) => {
            seen.push(msg.n);
            if (msg.n === 1) {
                await make("latest").send("k", { n: 2 }, { id: "v2" });
                throw new Error("stale render failed");
            }
        });

        expect(seen).toEqual([1, 2]);
        expect(result).toMatchObject({ outcome: "idle", processed: 1 });
    });

    it("a newer send replaces a message backing off, and runs at once", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        await obj.drain("k", async () => {
            throw new Error("boom");
        });
        await obj.send("k", { n: 2 }, { id: "v2" });
        const seen: unknown[] = [];

        await obj.drain("k", async (ctx, msg) => void seen.push([msg.n, ctx.attempt]));

        expect(seen).toEqual([[2, 1]]);
    });

    it("dead-letters under the message's own seq and clears the pending document", async () => {
        const letters: KeyedDeadLetter<Msg>[] = [];
        const obj = make("latest", { maxAttempts: 1, onDeadLetter: (letter) => void letters.push(letter) });
        for (const n of [1, 2]) await obj.send("k", { n }, { id: `v${n}` });

        const result = await obj.drain("k", async () => {
            throw new Error("boom");
        });

        expect(result).toEqual({ outcome: "idle", processed: 0, deadLettered: 1 });
        expect(letters[0]).toMatchObject({ seq: 2, id: "v2" });
        expect(store.has(dead(2))).toBe(true);
        expect(store.has(item("latest"))).toBe(false);
        expect(store.get(object())).toMatchObject({ processedSeq: 2 });
    });

    it("skips forward when the pending document expired", async () => {
        const obj = make("latest");
        await obj.send("k", { n: 1 }, { id: "v1" });
        store.delete(item("latest"));
        const handler = jest.fn(noop);

        expect(await obj.drain("k", handler)).toMatchObject({ outcome: "idle", processed: 0 });

        expect(handler).not.toHaveBeenCalled();
        expect(store.get(object())).toMatchObject({ processedSeq: 1 });
    });
});

describe("objectTtl (a restartable object)", () => {
    const objectTtl = { ms: 3 * TTL };
    const origin = NOW;

    it("rejects a horizon that a dead letter could outlive", () => {
        expect(() => make("fifo", { objectTtl: { ms: 2 * TTL } })).toThrow(/objectTtl/);
    });

    it("starts a generation's seqs from the clock and refreshes the object's expiry", async () => {
        const obj = make("fifo", { objectTtl });
        await obj.send("k", { n: 1 }, { id: "m1" });
        expect(store.get(object())).toMatchObject({ lastSeq: origin + 1, processedSeq: origin });
        expect(store.get(object())?.expiresAt).toEqual(new Date(NOW + 3 * TTL));
        clock += 1000;
        await obj.send("k", { n: 2 }, { id: "m2" });
        expect(store.get(object())).toMatchObject({ lastSeq: origin + 2, processedSeq: origin });
        expect(store.get(object())?.expiresAt).toEqual(new Date(NOW + 1000 + 3 * TTL));
        const seen: number[] = [];
        await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));
        expect(seen).toEqual([1, 2]);
    });

    it("leaves an object without it byte-identical: seq 1, no expiry", async () => {
        await make().send("k", { n: 1 }, { id: "m1" });
        expect(store.get(object())).toEqual({ lastSeq: 1, processedSeq: 0 });
    });

    it("purge needs objectTtl: a restart would reuse seqs", async () => {
        await expect(make().purge("k")).rejects.toThrow(/objectTtl/);
    });

    it("purge removes the object, messages, markers and dead letters; a later send still gets a kick", async () => {
        const obj = make("fifo", { objectTtl, maxAttempts: 1 });
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.send("k", { n: 2 }, { id: "m2" });
        await obj.drain("k", async (_ctx, msg) => {
            if (msg.n === 1) throw new Error("poison");
        }, {});
        await obj.send("k", { n: 3 }, { id: "m3" });
        expect(await obj.purge("k")).toBeGreaterThanOrEqual(3);
        expect([...store.keys()].filter((path) => path.startsWith(object()))).toEqual([]);
        expect(await obj.purge("k")).toBe(0);

        // A reused `s<seq>` id would be refused, stranding the message.
        tasks = [];
        clock += 5;
        expect(await obj.send("k", { n: 4 }, { id: "m1" })).toBe("accepted");
        expect(tasks).toHaveLength(1);
    });

});

describe("a refused drain", () => {
    it("arms nothing, and a holder that vanished is recovered by a sweep once its lease expired", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        // A drain whose release failed: its lease stays live and no task will retry.
        store.set(object(), {
            ...store.get(object())!,
            lease: { holder: "gone", epoch: 0, token: "t", expiresAtMs: NOW + LEASE },
        });
        tasks = [];
        const handler = jest.fn(noop);

        expect((await obj.drain("k", handler)).outcome).toBe("busy");
        expect(tasks).toEqual([]);

        // A sweep past the threshold kicks, and that kick finds the lease expired.
        clock += SWEEP.olderThanMs;
        expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1 });
        expect(await obj.drain("k", handler)).toMatchObject({ outcome: "idle", processed: 1 });
        expect(handler).toHaveBeenCalledTimes(1);
    });

    it("a sweep kick that still finds a live lease is retried with a new id in the next window", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        store.set(object(), {
            ...store.get(object())!,
            lease: { holder: "slow", epoch: 0, token: "t", expiresAtMs: NOW + 2 * SWEEP.olderThanMs },
        });
        tasks = [];
        clock += SWEEP.olderThanMs;
        await obj.sweep(SWEEP);
        expect((await obj.drain("k", noop)).outcome).toBe("busy");

        clock += SWEEP.olderThanMs;
        expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1, coalesced: 0 });
        expect(new Set(tasks.map((t) => t.id)).size).toBe(2);
        expect(await obj.drain("k", noop)).toMatchObject({ outcome: "idle", processed: 1 });
    });
});

describe("sweep", () => {
    /** The kick a lost `send` left unarmed: the message is there, no task is. */
    async function pending(obj: ReturnType<typeof make>, key: string, n = 1) {
        await obj.send(key, { n }, { id: `${key}-${n}`, kick: false });
    }

    it("recovers a lost kick: nothing before the threshold, one task after it, and the message runs", async () => {
        const obj = make();
        await pending(obj, "k");

        clock += SWEEP.olderThanMs - 1;
        expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, recent: 1 });
        expect(tasks).toEqual([]);

        clock += 1;
        expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, rekicked: 1 });
        expect(tasks).toEqual([{ key: "k", id: sweepId("k"), delaySeconds: undefined }]);
        expect(tasks[0].id).toMatch(/^objects-[0-9a-f]{32}-\d+$/);

        expect(await obj.drain("k", noop)).toMatchObject({ outcome: "idle", processed: 1 });
        expect(await obj.sweep(SWEEP)).toEqual(EMPTY);
    });

    it("recovers an accepted send whose kick id Cloud Tasks refused, for a seq a clock running behind repeated", async () => {
        const objectTtl = { ms: 3 * TTL };
        const first = make("fifo", { objectTtl });
        await first.send("k", { n: 1 }, { id: "a" });
        await first.drain("k", noop);
        await first.purge("k");
        tasks = [];

        const behind = make("fifo", { objectTtl });
        expect(await behind.send("k", { n: 2 }, { id: "b" })).toBe("accepted");
        expect(tasks).toEqual([]); // Control: same clock, same seq, its `s` id was refused.

        clock += SWEEP.olderThanMs;
        expect(await behind.sweep(SWEEP)).toMatchObject({ rekicked: 1 });
        expect(await behind.drain("k", noop)).toMatchObject({ outcome: "idle", processed: 1 });
    });

    describe("a retry task that re-arms its own running id", () => {
        /** A failed attempt, whose retry kick fires while this instance's clock is `skewMs` behind. */
        async function firedEarly(skewMs: number) {
            const obj = make();
            await obj.send("k", { n: 1 }, { id: "m1" });
            await obj.drain("k", async () => { throw new Error("boom"); });
            const armed = tasks[tasks.length - 1];
            expect(armed.id).toBe(kickId("r1.1"));
            clock = NOW + 10_000 + 1_000 - skewMs; // backoff 10 s, then the delay's ceil + 1 s
            const taskCount = tasks.length;
            return { obj, taskCount };
        }

        it("control: under the tolerance the attempt runs, so there is nothing to strand", async () => {
            const { obj } = await firedEarly(1_500);
            expect((await obj.drain("k", noop)).outcome).toBe("idle");
        });

        it("past the tolerance the id is refused and read as success; the sweep recovers the message", async () => {
            const { obj, taskCount } = await firedEarly(3_000);

            expect((await obj.drain("k", noop)).outcome).toBe("waiting");
            expect(tasks).toHaveLength(taskCount); // Stranded: no new task.

            clock += SWEEP.olderThanMs + 3_000;
            expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1 });
            expect(await obj.drain("k", noop)).toMatchObject({ outcome: "idle", processed: 1 });
        });
    });

    describe("a head in backoff", () => {
        async function backingOff() {
            const obj = make("fifo", { backoff: { baseMs: 2 * SWEEP.olderThanMs, maxMs: 2 * SWEEP.olderThanMs } });
            await obj.send("k", { n: 1 }, { id: "m1" });
            await obj.drain("k", async () => { throw new Error("boom"); });
            return obj;
        }

        it("is skipped however old it is, and its retry timing and task stay untouched", async () => {
            const obj = await backingOff();
            const before = store.get(item(1));
            const armed = [...tasks];

            clock += SWEEP.olderThanMs + 1;
            expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, backingOff: 1 });

            expect(tasks).toEqual(armed);
            expect(store.get(item(1))).toEqual(before);
        });

        it("a sweep kick racing into a backoff only re-arms the same id: waiting, no attempt spent, same due time", async () => {
            const obj = await backingOff();
            const before = store.get(item(1));
            const armed = [...tasks];
            const handler = jest.fn(noop);

            clock += 1_000;
            expect((await obj.drain("k", handler)).outcome).toBe("waiting");

            expect(handler).not.toHaveBeenCalled();
            expect(store.get(item(1))).toEqual(before);
            expect(tasks).toEqual(armed);
        });

        it("is left alone for `olderThanMs` after it comes due, while its armed retry kick runs", async () => {
            const obj = await backingOff();
            clock += 2 * SWEEP.olderThanMs;
            expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, recent: 1 });
            clock += SWEEP.olderThanMs;
            expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, rekicked: 1 });
        });
    });

    it("an old head that just started an attempt is left alone, even with its lease gone", async () => {
        const obj = make();
        await pending(obj, "k");
        clock += 10 * SWEEP.olderThanMs;
        store.set(item(1), { ...store.get(item(1))!, startedAt: new Date(clock - 1) });
        expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 1, recent: 1 });
    });

    it("a healthy old head costs one no-op drain, and kicks coalesce within a window", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        const sendKick = tasks[0];
        tasks = [];
        clock += SWEEP.olderThanMs;
        expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1 });
        expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 0, coalesced: 1 });
        expect(tasks).toHaveLength(1);

        const handler = jest.fn(noop);
        expect(await obj.drain("k", handler)).toMatchObject({ outcome: "idle", processed: 1 });
        expect(await obj.drain(sendKick.key, handler)).toEqual({ outcome: "idle", processed: 0, deadLettered: 0 });
        expect(handler).toHaveBeenCalledTimes(1);
    });

    describe("at a window boundary", () => {
        it("a kick refused inside a window leaves the key to the next window's new id, which is not refused", async () => {
            const windowMs = SWEEP.olderThanMs;
            const boundary = (Math.floor(NOW / windowMs) + 2) * windowMs;
            const obj = make();
            await pending(obj, "k");

            clock = boundary + 1;
            expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1 }); // That task runs and finds the key busy.
            clock = boundary + windowMs - 1;
            expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 0, coalesced: 1 }); // Refused.
            clock = boundary + windowMs;
            expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 1, coalesced: 0 });
            expect(new Set(tasks.map((t) => t.id)).size).toBe(2);
        });
    });

    it("a failed enqueue is counted, does not stop the other keys, and the next run retries", async () => {
        const obj = make();
        await pending(obj, "a");
        await pending(obj, "b");
        clock += SWEEP.olderThanMs;
        enqueueFailure = new Error("tasks down");
        expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 2, failed: 2, error: "Error: tasks down" });

        enqueueFailure = undefined;
        expect(await obj.sweep(SWEEP)).toMatchObject({ rekicked: 2, failed: 0 });
    });

    it("a kick for a key with no pending message is harmless", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.drain("k", noop);
        const before = store.get(object());
        expect(await obj.drain("k", noop)).toEqual({ outcome: "idle", processed: 0, deadLettered: 0 });
        expect(store.get(object())).toEqual(before);
        // No object at all: a stray document costs one no-op, and creates nothing.
        store.set(item(1, "orphan"), { seq: 1, createdAt: new Date(NOW) });
        clock += SWEEP.olderThanMs;
        expect(await obj.sweep(SWEEP)).toMatchObject({ scanned: 1, rekicked: 1 });
        expect(await obj.drain("orphan", noop)).toEqual({ outcome: "idle", processed: 0, deadLettered: 0 });
        expect(store.has(object("orphan"))).toBe(false);
    });

    describe("bounds", () => {
        it("reads one head per key however long the backlog, in key order", async () => {
            const obj = make();
            for (let n = 1; n <= 30; n++) await pending(obj, "a", n);
            await pending(obj, "b");
            clock += SWEEP.olderThanMs;
            const reads = spyReads();

            expect(await obj.sweep(SWEEP)).toEqual({ ...EMPTY, scanned: 2, rekicked: 2 });

            expect(reads.docs()).toBe(2);
            reads.restore();
            expect(tasks.map((t) => t.key)).toEqual(["a", "b"]);
        });

        it("stops at `limit` keys and says so; kicked keys clear, so the next run reaches the rest", async () => {
            const obj = make();
            for (const key of ["a", "b", "c"]) await pending(obj, key);
            clock += SWEEP.olderThanMs;

            expect(await obj.sweep({ ...SWEEP, limit: 2 })).toEqual({ ...EMPTY, scanned: 2, rekicked: 2, truncated: true });
            expect(tasks.map((t) => t.key)).toEqual(["a", "b"]);
            await obj.drain("a", noop);
            await obj.drain("b", noop);

            clock += SWEEP.olderThanMs;
            expect(await obj.sweep({ ...SWEEP, limit: 2 })).toEqual({ ...EMPTY, scanned: 1, rekicked: 1 });
            expect(tasks.map((t) => t.key)).toEqual(["a", "b", "c"]);
        });

        it("is not truncated when `limit` is exactly the number of keys", async () => {
            const obj = make();
            await pending(obj, "a");
            clock += SWEEP.olderThanMs;
            expect(await obj.sweep({ ...SWEEP, limit: 1 })).toEqual({ ...EMPTY, scanned: 1, rekicked: 1 });
        });

        it("rejects a bad threshold or limit", async () => {
            await expect(make().sweep({ olderThanMs: 0, limit: 1 })).rejects.toThrow(/olderThanMs/);
            await expect(make().sweep({ olderThanMs: 1, limit: 0 })).rejects.toThrow(/limit/);
            await expect(make().sweep({ olderThanMs: 1, limit: 1.5 })).rejects.toThrow(/limit/);
        });
    });

    it("sees only its own object: another object's backlog neither counts nor starves it", async () => {
        const mine = make();
        const other = make("fifo", { name: "others" });
        for (const key of ["a", "b", "c", "d", "e"]) await other.send(key, { n: 1 }, { id: key, kick: false });
        await pending(mine, "z");
        clock += SWEEP.olderThanMs;

        expect(await mine.sweep({ ...SWEEP, limit: 1 })).toEqual({ ...EMPTY, scanned: 1, rekicked: 1 });
        expect(tasks.map((t) => t.key)).toEqual(["z"]);
        expect(await other.sweep({ ...SWEEP, limit: 5 })).toEqual({ ...EMPTY, scanned: 5, rekicked: 5 });
    });

    it("masks the read: no message content reaches the sweep", async () => {
        const obj = make();
        await pending(obj, "k");
        clock += SWEEP.olderThanMs;
        const reads = spyReads();
        await obj.sweep(SWEEP);
        reads.restore();
        expect(reads.masks).toEqual([["createdAt", "notBefore", "startedAt"]]);
        expect(reads.fields()).not.toContain("msg");
    });
});

describe("onExpired", () => {
    it("reports a pending message the TTL took, then continues with the next", async () => {
        const gone: { key: string; seq: number }[] = [];
        const obj = make("fifo", { onExpired: (g) => void gone.push(g) });
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.send("k", { n: 2 }, { id: "m2" });
        store.delete(item(1));
        const seen: number[] = [];
        await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));
        expect(gone).toEqual([{ key: "k", seq: 1 }]);
        expect(seen).toEqual([2]);
    });

    it("a throwing hook cannot stall the key", async () => {
        const obj = make("fifo", { onExpired: () => { throw new Error("hook"); } });
        await obj.send("k", { n: 1 }, { id: "m1" });
        await obj.send("k", { n: 2 }, { id: "m2" });
        store.delete(item(1));
        const seen: number[] = [];
        await obj.drain("k", async (_ctx, msg) => void seen.push(msg.n));
        expect(seen).toEqual([2]);
    });
});
