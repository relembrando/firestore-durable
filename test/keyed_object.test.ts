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

    it("a concurrent commit of the same id is a duplicate, and a stray message document stays loud", async () => {
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

        store.set(item(2), { stray: true });
        await expect(obj.send("k", { n: 2 }, { id: "m2" })).rejects.toThrow(/ALREADY_EXISTS/);
        expect(store.get(object())).toEqual({ lastSeq: 1, processedSeq: 0 });
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

    it("a refused drain arms one kick past the lease, so a holder that never released cannot strand the key", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        // A drain whose release failed: its lease stays live and no task will retry.
        store.set(object(), {
            ...store.get(object())!,
            lease: { holder: "gone", epoch: 0, token: "t", expiresAtMs: NOW + LEASE },
        });
        tasks = [];
        const window = Math.floor(NOW / LEASE);
        const handler = jest.fn(noop);

        expect((await obj.drain("k", handler)).outcome).toBe("busy");
        expect((await obj.drain("k", handler)).outcome).toBe("busy");

        expect(tasks).toEqual([{ key: "k", id: kickId(`w${window}`), delaySeconds: 61 }]);
        expect(handler).not.toHaveBeenCalled();

        clock += 61_000;
        expect(await obj.drain("k", handler)).toMatchObject({ outcome: "idle", processed: 1 });
    });

    it("a kick that finds the lease still live re-arms in the next lease window", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        store.set(object(), {
            ...store.get(object())!,
            lease: { holder: "other", epoch: 0, token: "t", expiresAtMs: NOW + 10 * LEASE },
        });
        tasks = [];

        await obj.drain("k", noop);
        clock += 61_000;
        await obj.drain("k", noop);

        expect(tasks.map((t) => t.id)).toEqual([
            kickId(`w${Math.floor(NOW / LEASE)}`),
            kickId(`w${Math.floor((NOW + 61_000) / LEASE)}`),
        ]);
    });

    it("a refused drain whose wake-up cannot be armed fails, so the platform retries it", async () => {
        const obj = make();
        await obj.send("k", { n: 1 }, { id: "m1" });
        store.set(object(), {
            ...store.get(object())!,
            lease: { holder: "other", epoch: 0, token: "t", expiresAtMs: NOW + LEASE },
        });
        enqueueFailure = new Error("tasks down");

        await expect(obj.drain("k", noop)).rejects.toThrow("tasks down");
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
