/**
 * `src/lease.ts` against the REAL Firestore emulator through the REAL
 * `firebase-admin` SDK (jest.emulator.config.js). `lease.test.ts` pins
 * the lease's decisions against a fake that runs transactions one at a time;
 * this suite proves the properties those decisions are for under genuine
 * concurrency: server-side transactions, contention, aborts and SDK retries.
 */

import { FieldValue, getFirestore } from "firebase-admin/firestore";
import type { DocumentReference } from "firebase-admin/firestore";
import { setTimeout as sleep } from "timers/promises";
import { defineLease, LeaseLostError } from "../src";
import type { HeldLease, LeaseRuntime } from "../src";

const db = getFirestore();
const TTL = 60_000;
const CONTENDERS = 8;
/** The control needs only one waiter to show overlap; fewer keeps it cheap. */
const CONTROL_CONTENDERS = 3;
const WAIT = { attempts: 500, backoffMs: 10 };
/** Upper bound on one hold, so a contender that died cannot hang the suite. */
const HOLD_DEADLINE_MS = 5_000;

/**
 * Detects overlapping holders without relying on timing luck. A holder keeps
 * the lease until some contender still waiting has been REFUSED since it
 * entered (a refusal is the lease's `sleep` before a retry), or until it sees
 * another holder inside. So every hold spans at least one competing attempt:
 * with working exclusion that attempt is refused and the holder leaves; with
 * broken exclusion it succeeds while the holder is still inside, so `max`
 * reaches 2 deterministically instead of only when timings happen to collide.
 */
function exclusionProbe(n: number) {
    let active = 0;
    let max = 0;
    const entered = new Set<number>();
    let refusedSinceEntry = false;
    return {
        runtimeFor(i: number): Partial<LeaseRuntime> {
            return {
                sleep: async (ms) => {
                    if (!entered.has(i)) refusedSinceEntry = true;
                    await sleep(ms);
                },
            };
        },
        async hold(i: number): Promise<void> {
            active++;
            max = Math.max(max, active);
            entered.add(i);
            refusedSinceEntry = false;
            const deadline = Date.now() + HOLD_DEADLINE_MS;
            const lastOne = () => entered.size === n;
            try {
                while (active === 1 && !refusedSinceEntry && !lastOne() && Date.now() < deadline) await sleep(5);
            } finally {
                active--;
            }
        },
        get max() {
            return max;
        },
    };
}

/** One lease instance per contender, as separate function instances would be. */
function newLease(extra: { field?: string; ttlMs?: number; runtime?: Partial<LeaseRuntime> } = {}) {
    return defineLease({
        db,
        at: (key: string) => db.doc(extra.field ? `stories/${key}` : `leases/${key}`),
        field: extra.field,
        ttlMs: extra.ttlMs ?? TTL,
        runtime: extra.runtime,
    });
}

/**
 * N contenders race for `key`; each holds through the probe, then runs `work`.
 * They start together unless `staggerMs` spaces them out.
 */
async function contend(
    n: number,
    key: string,
    leaseOptions: { field?: string; ttlMs?: number },
    work: (held: HeldLease) => Promise<void> = async () => undefined,
    staggerMs = 0,
) {
    const probe = exclusionProbe(n);
    // allSettled, not all: a failed contender must not leave the others running
    // into the next test (after its beforeEach cleared the database).
    const settled = await Promise.allSettled(
        Array.from({ length: n }, async (_, i) => {
            await sleep(i * staggerMs);
            return newLease({ ...leaseOptions, runtime: probe.runtimeFor(i) }).run(
                key,
                async (held) => {
                    await probe.hold(i);
                    await work(held);
                    return i;
                },
                { wait: WAIT },
            );
        }),
    );
    const failed = settled.find((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed) throw failed.reason;
    const done = settled.map((r) => (r as PromiseFulfilledResult<number>).value);
    return { maxConcurrent: probe.max, done };
}

function deferred<T = void>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((r) => (resolve = r));
    return { promise, resolve };
}

async function readNumber(ref: DocumentReference, field: string): Promise<number> {
    return ((await ref.get()).get(field) as number | undefined) ?? 0;
}

describe("defineLease against the Firestore emulator", () => {
    it("lets exactly one of N concurrent contenders hold the lease at a time, and all complete", async () => {
        const tally = db.doc("tallies/k");

        const { maxConcurrent, done } = await contend(CONTENDERS, "k", {}, async (held) => {
            await held.tx(async (tx) => {
                tx.set(tally, { runs: FieldValue.increment(1) }, { merge: true });
            });
        });

        expect(maxConcurrent).toBe(1);
        expect(done.sort((a, b) => a - b)).toEqual(Array.from({ length: CONTENDERS }, (_, i) => i));
        expect(await readNumber(tally, "runs")).toBe(CONTENDERS);
        expect((await db.doc("leases/k").get()).data()).toEqual({ holder: null });
    });

    it("fences a holder whose lease expired: no write lands once another holder acquires", async () => {
        const counter = db.doc("tallies/fence");
        // A's clock is real; B's runs past A's expiry, so B sees A's lease as lapsed.
        const leaseA = newLease();
        const leaseB = newLease({ runtime: { now: () => Date.now() + TTL + 1_000 } });

        const aCommitted = deferred();
        const aMayReturn = deferred();
        let aCommits = 0;
        let aFailure: unknown;
        const aRun = leaseA.run("k", async (heldA) => {
            // A keeps writing through its lease until the fence stops it.
            try {
                for (;;) {
                    await heldA.tx(async (tx) => {
                        tx.set(counter, { a: FieldValue.increment(1) }, { merge: true });
                    });
                    if (++aCommits === 3) aCommitted.resolve();
                }
            } catch (err) {
                aFailure = err;
            }
            await aMayReturn.promise;
        });

        await aCommitted.promise;
        const bTokenSeen: string[] = [];
        await leaseB.run("k", async (heldB) => {
            bTokenSeen.push(heldB.token);
            const atAcquire = await heldB.tx(async (_tx, snap) => {
                expect(snap.get("token")).toBe(heldB.token);
                return readNumber(counter, "a");
            });
            await sleep(150);
            expect(await readNumber(counter, "a")).toBe(atAcquire);

            // A finishing (and running its release) must not free B's lease.
            aMayReturn.resolve();
            await aRun;
            expect((await db.doc("leases/k").get()).get("token")).toBe(heldB.token);

            const late = jest.fn();
            await expect(
                leaseA.run("k", late),
            ).rejects.toMatchObject({ name: "LeaseBusyError" });
            expect(late).not.toHaveBeenCalled();
        });

        expect(aFailure).toBeInstanceOf(LeaseLostError);
        expect(await readNumber(counter, "a")).toBe(aCommits);
        expect(bTokenSeen).toHaveLength(1);
        expect((await db.doc("leases/k").get()).data()).toEqual({ holder: null });
    });

    it("excludes in field mode on an existing document while holders read-modify-write it", async () => {
        const story = db.doc("stories/s1");
        await story.set({ title: "Infância em Minas", n: 0 });

        const { maxConcurrent } = await contend(CONTENDERS, "s1", { field: "processingLease" }, async (held) => {
            await held.tx(async (tx, snap) => {
                tx.update(story, { n: (snap.get("n") as number) + 1 });
            });
        });

        expect(maxConcurrent).toBe(1);
        expect((await story.get()).data()).toEqual({ title: "Infância em Minas", n: CONTENDERS, processingLease: null });
    });

    // Verify the verifier: through the same probe, a lease that lapses as soon
    // as it is written must show overlap, or `toBe(1)` above proves nothing.
    // Staggered starts skip the simultaneous-acquire deadlock, which the
    // emulator only breaks with a multi-second lock timeout; the probe does not
    // need it to see the overlap.
    it("control: the probe sees concurrent holders when the lease cannot exclude", async () => {
        const { maxConcurrent, done } = await contend(CONTROL_CONTENDERS, "k", { ttlMs: 1 }, undefined, 20);

        expect(maxConcurrent).toBeGreaterThan(1);
        expect(done).toHaveLength(CONTROL_CONTENDERS);
    });
});
