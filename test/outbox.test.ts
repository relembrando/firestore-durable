/**
 * `src/outbox.ts` against the in-memory transactional fake (buffered
 * writes applied all-or-nothing, `create` failing on an existing doc). The
 * fake runs transactions one at a time, so races are staged explicitly: a
 * concurrent dispatcher acts from INSIDE `deliver`, between the claim and the
 * finalize, which is exactly the window the token fence exists for.
 * `durable_outbox.emulator.test.ts` proves the same properties under real
 * concurrency.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { fakeDb, resetStore, store } from "./helpers/fake_firestore_tx";
import { defineOutbox } from "../src";
import type { DocumentReference } from "firebase-admin/firestore";
import type { OutboxConfig, OutboxHandlers, OutboxOutcome } from "../src";

const NOW = 1_700_000_000_000;
const LEASE = 2 * 60 * 1000;
const TTL = 7 * 86400_000;
const DELETE = { __delete: true };
const path = (id: string) => `outbox/${id}`;
const ms = (value: unknown) => (value as Date).getTime();

/** Each dispatcher instance mints its own tokens, as separate instances would. */
let instances = 0;
function makeOutbox(now: () => number = () => NOW, extra: Partial<OutboxConfig> = {}) {
    const instance = ++instances;
    let n = 0;
    return defineOutbox({
        db: fakeDb,
        at: (id) => fakeDb.doc(path(id)),
        items: () => fakeDb.collection("outbox"),
        leaseMs: LEASE,
        backoff: { baseMs: 30_000, maxMs: 60 * 60_000 },
        ttl: { field: "expiresAt", ms: TTL },
        deleteField: () => DELETE,
        runtime: { now, newToken: () => `token-${instance}.${++n}` },
        ...extra,
    });
}

async function seed(id = "a", fields: Record<string, unknown> = { payload: 1 }, due = true) {
    await fakeDb.runTransaction(async (tx: any) => makeOutbox().put(tx, id, fields, { due }));
}

/** Handlers that claim and then return `outcome`, recording deliveries. */
function handlers(outcome: OutboxOutcome = { kind: "settle", state: "done" }) {
    const delivered: { value: unknown; attempts: number; token: string }[] = [];
    const h: OutboxHandlers<string> = {
        claim: async () => ({ kind: "claim", value: "v", patch: { claimedBy: "test" } }),
        deliver: async (value, ctx) => {
            delivered.push({ value, ...ctx });
            return outcome;
        },
    };
    return { h, delivered };
}

beforeEach(() => {
    resetStore();
    instances = 0;
});

describe("defineOutbox config", () => {
    it.each([
        [{ leaseMs: 0 }, /leaseMs/],
        [{ backoff: { baseMs: 0, maxMs: 1 } }, /backoff.baseMs/],
        [{ ttl: { field: "", ms: TTL } }, /ttl.field/],
        [{ ttl: { field: "expiresAt", ms: -1 } }, /ttl.ms/],
        [{ maxAge: { ms: NaN, from: () => 0, outcome: { kind: "drop" } as OutboxOutcome } }, /maxAge.ms/],
    ])("rejects %p", (extra, message) => {
        expect(() => makeOutbox(undefined, extra as Partial<OutboxConfig>)).toThrow(message);
    });

    it("pins the backoff curve: 30 s doubling, capped at one hour", () => {
        const outbox = makeOutbox();
        expect([1, 2, 3, 4, 5, 6, 7, 8, 1000].map((a) => outbox.backoffMs(a))).toEqual(
            [60_000, 120_000, 240_000, 480_000, 960_000, 1_920_000, 3_600_000, 3_600_000, 3_600_000]);
    });
});

describe("put", () => {
    it("creates a due item with the lifecycle fields", async () => {
        await seed("a", { payload: 1 });
        const doc = store.get(path("a"))!;
        expect(doc).toEqual({ payload: 1, state: "pending", attempts: 0,
            expiresAt: new Date(NOW + TTL), createdAt: new Date(NOW), nextAttemptAt: new Date(NOW) });
    });

    it("records an undue item without nextAttemptAt, in the caller's state", async () => {
        await fakeDb.runTransaction(async (tx: any) => makeOutbox().put(tx, "x", { p: 1 }, { state: "expired", due: false }));
        expect(store.get(path("x"))).toEqual({ p: 1, state: "expired", attempts: 0,
            expiresAt: new Date(NOW + TTL), createdAt: new Date(NOW) });
        expect(await makeOutbox().dispatch("x", handlers().h)).toBe("not_due");
    });

    it("is create-only: a replayed put fails instead of resetting a delivered item", async () => {
        await seed("a");
        await makeOutbox().dispatch("a", handlers().h);
        const delivered = store.get(path("a"));
        await expect(seed("a")).rejects.toMatchObject({ code: 6 });
        expect(store.get(path("a"))).toEqual(delivered);
    });
});

describe("dispatch", () => {
    it("reports missing and not-yet-due items without calling a hook", async () => {
        const claim = jest.fn();
        const h = { claim, deliver: jest.fn() } as any;
        expect(await makeOutbox().dispatch("nope", h)).toBe("missing");
        await seed("a");
        expect(await makeOutbox(() => NOW - 1).dispatch("a", h)).toBe("not_due");
        expect(claim).not.toHaveBeenCalled();
    });

    it("claims under a fresh token, delivers outside the transaction, then settles", async () => {
        await seed("a");
        const { h, delivered } = handlers({ kind: "settle", state: "done", patch: { receipt: "r1", payload: DELETE } });
        let duringDelivery: any;
        const deliver = h.deliver;
        h.deliver = async (value, ctx) => {
            duringDelivery = { ...store.get(path("a")) };
            return deliver(value, ctx);
        };

        expect(await makeOutbox().dispatch("a", h)).toBe("delivered");

        expect(delivered).toEqual([{ value: "v", attempts: 1, token: expect.stringMatching(/^token-/) }]);
        expect(duringDelivery).toMatchObject({ state: "sending", leaseToken: delivered[0].token, attempts: 1, claimedBy: "test" });
        expect(ms(duringDelivery.nextAttemptAt)).toBe(NOW + LEASE);
        const done = store.get(path("a"))!;
        expect(done).toMatchObject({ state: "done", attempts: 1, receipt: "r1", claimedBy: "test" });
        expect(done).not.toHaveProperty("leaseToken");
        expect(done).not.toHaveProperty("nextAttemptAt");
        expect(done).not.toHaveProperty("payload");
        // Settled is terminal: a later dispatch finds nothing due.
        expect(await makeOutbox(() => NOW + 10 * TTL).dispatch("a", h)).toBe("not_due");
    });

    it("retries with capped backoff on the attempt count, back in state pending", async () => {
        await seed("a");
        const { h } = handlers({ kind: "retry" });
        await makeOutbox().dispatch("a", h);
        expect(store.get(path("a"))).toMatchObject({ state: "pending", attempts: 1 });
        expect(ms(store.get(path("a"))!.nextAttemptAt)).toBe(NOW + 60_000);

        await makeOutbox(() => NOW + 60_000).dispatch("a", h);
        expect(store.get(path("a"))!.attempts).toBe(2);
        expect(ms(store.get(path("a"))!.nextAttemptAt)).toBe(NOW + 60_000 + 120_000);
        expect(store.get(path("a"))).not.toHaveProperty("leaseToken");
    });

    it("parks at claim time without counting an attempt", async () => {
        await seed("a");
        const deliver = jest.fn();
        const result = await makeOutbox().dispatch("a", {
            claim: async () => ({ kind: "park", state: "waiting", afterMs: 600_000, patch: { why: "identity" } }),
            deliver,
        });
        expect(result).toBe("decided");
        expect(deliver).not.toHaveBeenCalled();
        expect(store.get(path("a"))).toMatchObject({ state: "waiting", attempts: 0, why: "identity" });
        expect(ms(store.get(path("a"))!.nextAttemptAt)).toBe(NOW + 600_000);
    });

    it("drops at claim time and after delivery", async () => {
        await seed("a");
        await seed("b");
        expect(await makeOutbox().dispatch("a", { claim: async () => ({ kind: "drop" }), deliver: jest.fn() })).toBe("decided");
        expect(await makeOutbox().dispatch("b", handlers({ kind: "drop" }).h)).toBe("delivered");
        expect(store.has(path("a"))).toBe(false);
        expect(store.has(path("b"))).toBe(false);
    });

    it("lets claim-hook reads run in the claim transaction", async () => {
        await seed("a");
        store.set("users/u1", { eligible: true });
        const seen: unknown[] = [];
        await makeOutbox().dispatch("a", {
            claim: async ({ tx, data }) => {
                seen.push((await tx.get(fakeDb.doc("users/u1") as DocumentReference)).data(), data.payload);
                return { kind: "claim", value: 1 };
            },
            deliver: async () => ({ kind: "settle", state: "done" }),
        });
        expect(seen).toEqual([{ eligible: true }, 1]);
    });
});

describe("lease and fence", () => {
    it("a claimed item is not due until the lease lapses, then is reclaimed under a new token", async () => {
        await seed("a");
        const late = handlers();
        let inner: string | undefined;
        let firstToken = "";
        await makeOutbox().dispatch("a", {
            claim: async () => ({ kind: "claim", value: "first" }),
            deliver: async (_value, ctx) => {
                firstToken = ctx.token;
                inner = await makeOutbox(() => NOW + LEASE - 1).dispatch("a", late.h);
                throw new Error("crash mid-delivery");
            },
        }).catch(() => undefined);
        expect(inner).toBe("not_due");
        expect(late.delivered).toHaveLength(0);
        // The thrown delivery left the claim in place, to lapse.
        expect(store.get(path("a"))).toMatchObject({ state: "sending", attempts: 1, leaseToken: firstToken });

        const again = makeOutbox(() => NOW + LEASE);
        expect(await again.dispatch("a", late.h)).toBe("delivered");
        expect(late.delivered).toEqual([{ value: "v", attempts: 2, token: expect.not.stringMatching(firstToken) }]);
    });

    it("a stale finalize writes nothing once a later claim replaced the token", async () => {
        await seed("a");
        let replaced: Record<string, unknown> | undefined;
        const result = await makeOutbox().dispatch("a", {
            claim: async () => ({ kind: "claim", value: "slow" }),
            deliver: async () => {
                // The lease lapses mid-delivery and another dispatcher claims and settles.
                const other = makeOutbox(() => NOW + LEASE + 1);
                await other.dispatch("a", handlers({ kind: "settle", state: "by-other", patch: { winner: "other" } }).h);
                replaced = { ...store.get(path("a")) };
                return { kind: "settle", state: "by-slow", patch: { winner: "slow" } };
            },
        });
        expect(result).toBe("lost");
        expect(store.get(path("a"))).toEqual(replaced);
        expect(replaced).toMatchObject({ state: "by-other", winner: "other", attempts: 2 });
    });

    it("a claim-time decision by another dispatcher also fences the slow holder", async () => {
        await seed("a");
        const result = await makeOutbox().dispatch("a", {
            claim: async () => ({ kind: "claim", value: "slow" }),
            deliver: async () => {
                await makeOutbox(() => NOW + LEASE + 1).dispatch("a", {
                    claim: async () => ({ kind: "park", state: "waiting", afterMs: 1000 }),
                    deliver: jest.fn(),
                });
                return { kind: "settle", state: "by-slow" };
            },
        });
        expect(result).toBe("lost");
        expect(store.get(path("a"))).toMatchObject({ state: "waiting" });
        expect(store.get(path("a"))).not.toHaveProperty("leaseToken");
    });

    it("a finalize after the item was deleted does not resurrect it", async () => {
        await seed("a");
        const result = await makeOutbox().dispatch("a", {
            claim: async () => ({ kind: "claim", value: 1 }),
            deliver: async () => {
                store.delete(path("a"));
                return { kind: "settle", state: "done" };
            },
        });
        expect(result).toBe("lost");
        expect(store.has(path("a"))).toBe(false);
    });
});

describe("max age", () => {
    const maxAge = { ms: 1000, from: (d: any) => d.occurredAtMs, outcome: { kind: "settle", state: "expired" } as OutboxOutcome };

    it("tells the claim hook, and never delivers an expired item even if the hook claims it", async () => {
        await seed("a", { occurredAtMs: NOW - 1001 });
        const { h, delivered } = handlers();
        const claim = jest.spyOn(h, "claim");
        expect(await makeOutbox(undefined, { maxAge }).dispatch("a", h)).toBe("decided");
        expect(claim.mock.calls[0][0].expired).toBe(true);
        expect(delivered).toHaveLength(0);
        expect(store.get(path("a"))).toMatchObject({ state: "expired", attempts: 0 });
        expect(store.get(path("a"))).not.toHaveProperty("nextAttemptAt");
    });

    it("lets the hook order another decision before expiry", async () => {
        await seed("a", { occurredAtMs: NOW - 1001 });
        await makeOutbox(undefined, { maxAge }).dispatch("a", { claim: async () => ({ kind: "drop" }), deliver: jest.fn() });
        expect(store.has(path("a"))).toBe(false);
    });

    it("delivers an item exactly at the age limit", async () => {
        await seed("a", { occurredAtMs: NOW - 1000 });
        const { h, delivered } = handlers();
        await makeOutbox(undefined, { maxAge }).dispatch("a", h);
        expect(delivered).toHaveLength(1);
    });
});

describe("sweep", () => {
    function query(ids: string[]) {
        const calls: unknown[][] = [];
        const q: any = {
            where: (...a: unknown[]) => (calls.push(["where", ...a]), q),
            orderBy: (...a: unknown[]) => (calls.push(["orderBy", ...a]), q),
            limit: (...a: unknown[]) => (calls.push(["limit", ...a]), q),
            get: async () => ({ docs: ids.map((id) => ({ id })) }),
        };
        return { q, calls };
    }

    it("reads due items oldest first and runs every one with bounded concurrency", async () => {
        const { q, calls } = query(["a", "b", "c", "d", "e"]);
        const outbox = makeOutbox(undefined, { items: () => q });
        let active = 0;
        let max = 0;
        const seen: string[] = [];
        const count = await outbox.sweep({ limit: 100, concurrency: 2 }, async (id) => {
            active++;
            max = Math.max(max, active);
            await new Promise((r) => setTimeout(r, 5));
            seen.push(id);
            active--;
        });
        expect(count).toBe(5);
        expect(seen.sort()).toEqual(["a", "b", "c", "d", "e"]);
        expect(max).toBe(2);
        expect(calls).toEqual([["where", "nextAttemptAt", "<=", new Date(NOW)], ["orderBy", "nextAttemptAt"], ["limit", 100]]);
    });

    it("rejects like Promise.all when one item throws", async () => {
        const { q } = query(["a", "b"]);
        await expect(makeOutbox(undefined, { items: () => q }).sweep({ limit: 10, concurrency: 1 }, async (id) => {
            if (id === "a") throw new Error("boom");
        })).rejects.toThrow("boom");
    });
});
