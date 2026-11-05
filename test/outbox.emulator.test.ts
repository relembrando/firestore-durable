/**
 * `src/outbox.ts` against the REAL Firestore emulator through the REAL
 * `firebase-admin` SDK (jest.emulator.config.js). `outbox.test.ts`
 * pins the outbox's decisions against a fake that runs transactions one at a
 * time; this suite proves, under genuine concurrency (server-side
 * transactions, contention, aborts, SDK retries), the two properties callers
 * rely on: one delivery per lease window, and a fenced finalize.
 */

import { FieldValue, getFirestore } from "firebase-admin/firestore";
import { setTimeout as sleep } from "timers/promises";
import { defineOutbox } from "../src";
import type { OutboxDispatchResult, OutboxHandlers, OutboxOutcome } from "../src";

const db = getFirestore();
const LEASE = 2 * 60 * 1000;
const DISPATCHERS = 8;
/** Upper bound on one held delivery, so a stuck dispatcher cannot hang the suite. */
const HOLD_DEADLINE_MS = 5_000;

/** One outbox per dispatcher, as separate function instances would have. */
function newOutbox(now: () => number = () => Date.now(), leaseMs = LEASE) {
    return defineOutbox({
        db,
        at: (id) => db.doc(`outbox/${id}`),
        items: () => db.collection("outbox"),
        leaseMs,
        backoff: { baseMs: 30_000, maxMs: 60 * 60_000 },
        ttl: { field: "expiresAt", ms: 7 * 86400_000 },
        deleteField: () => FieldValue.delete(),
        runtime: { now },
    });
}

async function putDue(id: string): Promise<void> {
    await db.runTransaction(async (tx) => newOutbox().put(tx, id, { payload: "x" }));
}

/** Resolves when `until` does or after `ms`, whichever is first, leaving no timer behind. */
async function holdUntil(until: Promise<void>, ms: number): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([until, new Promise<void>((r) => (timer = setTimeout(r, ms)))]);
    clearTimeout(timer);
}

function deferred<T = void>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((r) => (resolve = r));
    return { promise, resolve };
}

/**
 * Handlers whose delivery stays in flight until `release` resolves (or the
 * deadline passes), so every competing dispatch runs while it is held.
 */
function heldHandlers(deliveries: string[], who: string, release: Promise<void>, outcome: OutboxOutcome): OutboxHandlers<string> {
    return {
        claim: async () => ({ kind: "claim", value: who }),
        deliver: async (value) => {
            deliveries.push(value);
            await holdUntil(release, HOLD_DEADLINE_MS);
            return outcome;
        },
    };
}

/**
 * N dispatchers race for one due item. A dispatcher that wins holds its
 * delivery until the other N-1 have returned (or `holdMs`), so a second
 * delivery inside the same lease window is recorded, not missed by timing:
 * with two winners only N-2 others return and both hold to the deadline.
 */
async function race(id: string, n: number, now: () => number,
    { leaseMs = LEASE, staggerMs = 0, holdMs = HOLD_DEADLINE_MS } = {}) {
    const deliveries: string[] = [];
    let othersReturned = 0;
    const allOthersDone = deferred();
    const results = await Promise.all(Array.from({ length: n }, async (_, i) => {
        await sleep(i * staggerMs);
        let delivered = false;
        const result = await newOutbox(now, leaseMs).dispatch(id, {
            claim: async () => ({ kind: "claim", value: `d${i}` }),
            deliver: async (value) => {
                delivered = true;
                deliveries.push(value);
                await holdUntil(allOthersDone.promise, holdMs);
                return { kind: "settle", state: "done", patch: { by: value } };
            },
        });
        if (!delivered && ++othersReturned === n - 1) allOthersDone.resolve();
        return result;
    }));
    return { deliveries, results };
}

function tally(results: OutboxDispatchResult[]): Record<string, number> {
    return results.reduce<Record<string, number>>((acc, r) => ({ ...acc, [r]: (acc[r] ?? 0) + 1 }), {});
}

describe("defineOutbox against the Firestore emulator", () => {
    it("N concurrent dispatchers on one due item deliver it exactly once", async () => {
        await putDue("a");

        const { deliveries, results } = await race("a", DISPATCHERS, () => Date.now());

        expect(deliveries).toHaveLength(1);
        expect(tally(results)).toEqual({ delivered: 1, not_due: DISPATCHERS - 1 });
        const doc = (await db.doc("outbox/a").get()).data();
        expect(doc).toMatchObject({ state: "done", attempts: 1, by: deliveries[0] });
        expect(doc).not.toHaveProperty("leaseToken");
        expect(doc).not.toHaveProperty("nextAttemptAt");
    });

    it("one delivery per lease window, and the stale holder's finalize writes nothing", async () => {
        await putDue("b");
        const start = Date.now();
        const deliveries: string[] = [];

        // Holder A claims and stays in flight.
        const releaseA = deferred();
        const aRun = newOutbox(() => start).dispatch("b",
            heldHandlers(deliveries, "A", releaseA.promise, { kind: "settle", state: "by-A", patch: { winner: "A" } }));
        while (deliveries.length === 0) await sleep(5);

        // Inside A's lease window: N dispatchers, none delivers.
        const inside = await Promise.all(Array.from({ length: DISPATCHERS }, () =>
            newOutbox(() => start + LEASE - 1).dispatch("b",
                heldHandlers(deliveries, "inside", Promise.resolve(), { kind: "settle", state: "by-inside" }))));
        expect(tally(inside)).toEqual({ not_due: DISPATCHERS });
        expect(deliveries).toEqual(["A"]);

        // A's lease has lapsed: N dispatchers, exactly one (B) delivers and settles.
        const { deliveries: next, results } = await race("b", DISPATCHERS, () => start + LEASE);
        expect(next).toHaveLength(1);
        expect(tally(results)).toEqual({ delivered: 1, not_due: DISPATCHERS - 1 });
        const afterB = (await db.doc("outbox/b").get()).data();
        expect(afterB).toMatchObject({ state: "done", attempts: 2, by: next[0] });

        // A finishes late: its token was replaced, so its finalize is refused.
        releaseA.resolve();
        expect(await aRun).toBe("lost");
        expect((await db.doc("outbox/b").get()).data()).toEqual(afterB);
    });

    // Verify the verifier: the same race through a lease that lapses as soon
    // as it is written must show more than one delivery, or `toHaveLength(1)`
    // above proves nothing. Each delivery is held 150 ms and starts are 25 ms
    // apart, so every later dispatcher reads a lapsed claim of a delivery
    // still in flight (and the emulator's simultaneous-lock wait is skipped).
    it("control: the race sees duplicate deliveries when the lease cannot exclude", async () => {
        await putDue("c");

        const { deliveries } = await race("c", 3, () => Date.now(), { leaseMs: 1, staggerMs: 25, holdMs: 150 });

        expect(deliveries.length).toBeGreaterThan(1);
    });
});
