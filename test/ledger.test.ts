/**
 * `src/ledger.ts` against the in-memory transactional fake: buffered
 * writes applied all-or-nothing on commit, and `create` failing with code 6 on
 * an existing document. The fake runs transactions one at a time, so a
 * concurrent delivery is staged explicitly by committing the marker from
 * inside `fn` (after the ledger's read, before its commit), which is exactly
 * the window `tx.create` exists to close.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { fakeDb, resetStore, store } from "./helpers/fake_firestore_tx";
import { defineLedger } from "../src";

const TTL = 7 * 24 * 60 * 60 * 1000;
const NOW = 1_700_000_000_000;

// The shared fake's document refs only read; `runAndRecord` writes the marker
// outside a transaction, so give the refs a direct `set`.
function ref(path: string): any {
    return {
        ...fakeDb.doc(path),
        set: async (data: Record<string, unknown>) => {
            store.set(path, { ...data });
        },
    };
}

function makeLedger() {
    return defineLedger({
        db: fakeDb,
        at: (id) => ref(`events/${id}`),
        ttl: { field: "expiresAt", ms: TTL },
        runtime: { now: () => NOW },
    });
}

beforeEach(() => resetStore());

describe("defineLedger", () => {
    it("rejects a config without a usable TTL", () => {
        expect(() => defineLedger({ db: fakeDb, at: ref, ttl: { field: "", ms: TTL } })).toThrow(/ttl.field/);
        expect(() => defineLedger({ db: fakeDb, at: ref, ttl: { field: "expiresAt", ms: 0 } })).toThrow(/ttl.ms/);
    });

    describe("applyOnce", () => {
        it("applies fn's writes and creates the marker with the TTL field in the same commit", async () => {
            const out = await makeLedger().applyOnce(
                "evt_1",
                async (tx) => {
                    tx.set(fakeDb.doc("users/u1"), { plan: "family" });
                    return "done";
                },
                { fields: { type: "customer.subscription.updated" } },
            );
            expect(out).toEqual({ status: "applied", value: "done" });
            expect(store.get("users/u1")).toEqual({ plan: "family" });
            expect(store.get("events/evt_1")).toEqual({
                type: "customer.subscription.updated",
                expiresAt: new Date(NOW + TTL),
            });
        });

        it("reports a redelivery as duplicate without running fn", async () => {
            const ledger = makeLedger();
            await ledger.applyOnce("evt_1", async () => undefined);
            const fn = jest.fn(async () => "again");
            await expect(ledger.applyOnce("evt_1", fn)).resolves.toEqual({ status: "duplicate" });
            expect(fn).not.toHaveBeenCalled();
        });

        it("treats a marker in any shape (written by the code it replaced) as a duplicate", async () => {
            store.set("events/evt_1", { type: "legacy", processedAt: "__server_ts__" });
            const fn = jest.fn(async () => undefined);
            await expect(makeLedger().applyOnce("evt_1", fn)).resolves.toEqual({ status: "duplicate" });
            expect(fn).not.toHaveBeenCalled();
            expect(store.get("events/evt_1")).toEqual({ type: "legacy", processedAt: "__server_ts__" });
        });

        it("leaves neither marker nor effect when fn throws", async () => {
            const boom = new Error("boom");
            await expect(
                makeLedger().applyOnce("evt_1", async (tx) => {
                    tx.set(fakeDb.doc("users/u1"), { plan: "family" });
                    throw boom;
                }),
            ).rejects.toBe(boom);
            expect(store.has("events/evt_1")).toBe(false);
            expect(store.has("users/u1")).toBe(false);
        });

        it("drops the effect when a concurrent delivery commits the marker first (create, not set)", async () => {
            // Control for the `tx.create` invariant: with `tx.set` this commits
            // and the effect lands twice; with `create` the whole commit fails.
            const out = await makeLedger().applyOnce("evt_1", async (tx) => {
                store.set("events/evt_1", { type: "winner" });
                tx.set(fakeDb.doc("counters/c"), { n: 2 });
                return "loser";
            });
            expect(out).toEqual({ status: "duplicate" });
            expect(store.get("events/evt_1")).toEqual({ type: "winner" });
            expect(store.has("counters/c")).toBe(false);
        });

        it("rethrows an ALREADY_EXISTS that came from fn's own create, not the marker", async () => {
            store.set("orgs/o1", { name: "taken" });
            await expect(
                makeLedger().applyOnce("evt_1", async (tx) => {
                    tx.create(fakeDb.doc("orgs/o1"), { name: "mine" });
                }),
            ).rejects.toMatchObject({ code: 6 });
            expect(store.has("events/evt_1")).toBe(false);
        });
    });

    describe("runAndRecord", () => {
        it("runs fn, then records the marker with the caller's fields and the TTL field", async () => {
            const order: string[] = [];
            const out = await makeLedger().runAndRecord(
                "evt_1",
                async () => {
                    order.push(store.has("events/evt_1") ? "marker-before-fn" : "fn");
                    return 7;
                },
                { fields: { type: "charge.refunded", processedAt: "__server_ts__" } },
            );
            expect(out).toEqual({ status: "applied", value: 7 });
            expect(order).toEqual(["fn"]);
            expect(store.get("events/evt_1")).toEqual({
                type: "charge.refunded",
                processedAt: "__server_ts__",
                expiresAt: new Date(NOW + TTL),
            });
        });

        it("reports a redelivery as duplicate without running fn", async () => {
            store.set("events/evt_1", { type: "legacy" });
            const fn = jest.fn(async () => undefined);
            await expect(makeLedger().runAndRecord("evt_1", fn)).resolves.toEqual({ status: "duplicate" });
            expect(fn).not.toHaveBeenCalled();
        });

        it("records nothing when fn throws, so the retry runs the work again", async () => {
            const ledger = makeLedger();
            const boom = new Error("boom");
            await expect(ledger.runAndRecord("evt_1", async () => { throw boom; })).rejects.toBe(boom);
            expect(store.has("events/evt_1")).toBe(false);
            const retry = jest.fn(async () => "ok");
            await expect(ledger.runAndRecord("evt_1", retry)).resolves.toEqual({ status: "applied", value: "ok" });
            expect(retry).toHaveBeenCalledTimes(1);
        });

        it("does not fail when a concurrent delivery recorded the marker while fn ran", async () => {
            const out = await makeLedger().runAndRecord("evt_1", async () => {
                store.set("events/evt_1", { type: "concurrent" });
                return "ok";
            });
            expect(out).toEqual({ status: "applied", value: "ok" });
            expect(store.get("events/evt_1")?.expiresAt).toEqual(new Date(NOW + TTL));
        });
    });
});
