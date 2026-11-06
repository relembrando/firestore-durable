/**
 * `src/lease.ts` against the in-memory transactional fake. The fake runs
 * transactions one at a time, so these pin the lease's DECISIONS (who may
 * acquire, what the fence lets commit, what release may clear) across explicit
 * interleavings; Firestore itself provides the serializability.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { fakeDb, resetStore, store } from "./helpers/fake_firestore_tx";
import {
    CONTENDED_HOLDER,
    defineLease,
    LeaseBusyError,
    LeaseLostError,
    LeaseTargetMissingError,
} from "../src";

const TTL = 60_000;
let nowMs: number;
let tokens: number;

function makeLease(extra: { field?: string; deleteOnRelease?: boolean; sleep?: (ms: number) => Promise<void> } = {}) {
    return defineLease({
        db: fakeDb,
        at: (key: string) => fakeDb.doc(extra.field ? `stories/${key}` : `leases/${key}`),
        field: extra.field,
        deleteOnRelease: extra.deleteOnRelease,
        ttlMs: TTL,
        runtime: {
            now: () => nowMs,
            newToken: () => `t${tokens++}`,
            sleep: extra.sleep ?? (async () => undefined),
        },
    });
}

function holdAs(path: string, state: Record<string, unknown>) {
    store.set(path, { holder: "other", epoch: 0, token: "other-token", expiresAtMs: nowMs + TTL, ...state });
}

beforeEach(() => {
    resetStore();
    nowMs = 1_000_000;
    tokens = 0;
});

describe("defineLease", () => {
    it("holds the lease while fn runs and frees it afterwards", async () => {
        const lease = makeLease();
        let during: any;
        const out = await lease.run("k", async (held) => {
            during = store.get("leases/k");
            expect(held.token).toBe(during.token);
            return 42;
        });
        expect(out).toBe(42);
        expect(during).toMatchObject({ epoch: 0, expiresAtMs: nowMs + TTL });
        expect(store.get("leases/k")).toEqual({ holder: null });
    });

    it("refuses while another owner holds a live lease, without running fn", async () => {
        holdAs("leases/k", {});
        const fn = jest.fn();
        await expect(makeLease().run("k", fn)).rejects.toMatchObject({
            name: "LeaseBusyError",
            holder: "other",
        });
        expect(fn).not.toHaveBeenCalled();
        expect(store.get("leases/k")?.token).toBe("other-token");
    });

    it("waits with backoff and acquires once the holder releases", async () => {
        holdAs("leases/k", {});
        const sleep = jest.fn(async () => {
            if (sleep.mock.calls.length === 2) store.set("leases/k", { holder: null });
        });
        const fn = jest.fn(async () => "ok");
        await expect(
            makeLease({ sleep }).run("k", fn, { wait: { attempts: 5, backoffMs: 250 } }),
        ).resolves.toBe("ok");
        expect(sleep).toHaveBeenCalledTimes(2);
        expect(sleep).toHaveBeenCalledWith(250);
    });

    it("gives up with LeaseBusyError after the configured attempts", async () => {
        holdAs("leases/k", {});
        const sleep = jest.fn(async () => undefined);
        await expect(
            makeLease({ sleep }).run("k", jest.fn(), { wait: { attempts: 3, backoffMs: 10 } }),
        ).rejects.toBeInstanceOf(LeaseBusyError);
        expect(sleep).toHaveBeenCalledTimes(2);
    });

    it("takes over an expired lease", async () => {
        holdAs("leases/k", { expiresAtMs: nowMs });
        await expect(makeLease().run("k", async () => "ok")).resolves.toBe("ok");
    });

    it("stays exclusive when the contender's clock runs behind the holder's", async () => {
        await makeLease().run("k", async () => {
            nowMs -= 5_000;
            await expect(makeLease().run("k", jest.fn())).rejects.toBeInstanceOf(LeaseBusyError);
        });
    });

    it("ignores a document that is not in the lease shape", async () => {
        // e.g. a hand-rolled shape { holder, heldUntil }.
        store.set("leases/k", { holder: "legacy-op", heldUntil: { toMillis: () => nowMs + TTL } });
        await expect(makeLease().run("k", async () => "ok")).resolves.toBe("ok");
    });

    describe("legacy format (adopting a hand-rolled lease during a rolling deploy)", () => {
        const legacy = {
            read: (raw: unknown) => {
                const d = raw as { owner?: unknown; until?: unknown } | undefined;
                return typeof d?.owner === "string" && typeof d?.until === "number"
                    ? { holder: d.owner, expiresAtMs: d.until }
                    : undefined;
            },
            write: (state: { holder: string; expiresAtMs: number } | null) => ({
                owner: state?.holder ?? null,
                until: state?.expiresAtMs ?? 0,
            }),
        };
        const lease = () =>
            defineLease({
                db: fakeDb,
                at: (k: string) => fakeDb.doc(`leases/${k}`),
                ttlMs: TTL,
                legacy,
                runtime: { now: () => nowMs, newToken: () => `t${tokens++}`, sleep: async () => undefined },
            });

        it("treats a live legacy holder as busy", async () => {
            store.set("leases/k", { owner: "old-op", until: nowMs + TTL });
            await expect(lease().run("k", jest.fn())).rejects.toMatchObject({ holder: "old-op" });
        });

        it("acquires over an expired legacy holder", async () => {
            store.set("leases/k", { owner: "old-op", until: nowMs - 1 });
            await expect(lease().run("k", async () => "ok")).resolves.toBe("ok");
        });

        it("writes the legacy fields while held and clears them on release, so old code sees both", async () => {
            let during: any;
            await lease().run("k", async (held) => {
                during = store.get("leases/k");
                expect(during).toMatchObject({ token: held.token, owner: held.owner.id, until: nowMs + TTL });
            });
            expect(store.get("leases/k")).toEqual({ holder: null, owner: null, until: 0 });
        });

        // A legacy writer using `update` or merge keeps the modern fields of the
        // expired lease it acquired over, so the document decodes as a modern lease.
        describe("a legacy holder over an expired modern lease", () => {
            const stale = { holder: "crashed", epoch: 0, token: "stale-token", expiresAtMs: 0 };

            it("is busy while the legacy holder is live, and writes nothing", async () => {
                const doc = { ...stale, expiresAtMs: nowMs - 1, owner: "old-op", until: nowMs + TTL };
                store.set("leases/k", doc);
                const fn = jest.fn();

                await expect(lease().run("k", fn)).rejects.toMatchObject({ name: "LeaseBusyError", holder: "old-op" });

                expect(fn).not.toHaveBeenCalled();
                expect(store.get("leases/k")).toEqual(doc);
            });

            it("is busy in field mode too", async () => {
                store.set("stories/k", { title: "t", lease: { ...stale, expiresAtMs: nowMs - 1, owner: "old-op", until: nowMs + TTL } });
                const inField = defineLease({
                    db: fakeDb,
                    at: (k: string) => fakeDb.doc(`stories/${k}`),
                    field: "lease",
                    ttlMs: TTL,
                    legacy,
                    runtime: { now: () => nowMs, newToken: () => `t${tokens++}`, sleep: async () => undefined },
                });

                await expect(inField.run("k", jest.fn())).rejects.toMatchObject({ holder: "old-op" });
            });

            it("acquires once the legacy holder expired too", async () => {
                store.set("leases/k", { ...stale, expiresAtMs: nowMs - 1, owner: "old-op", until: nowMs - 1 });

                await expect(lease().run("k", async () => "ok")).resolves.toBe("ok");
            });

            it("acquires over the crashed holder's own mirrored legacy fields, which expire with it", async () => {
                await lease().run("k", async () => {
                    // No release: this holder crashes. Its mirror carries the same expiry.
                    store.set("crash/k", store.get("leases/k")!);
                });
                store.set("leases/k", store.get("crash/k")!);
                nowMs += TTL;

                await expect(lease().run("k", async () => "ok")).resolves.toBe("ok");
            });

            it("still lets a retry take over its own live lease, whose mirror reads as live", async () => {
                store.set("leases/k", { holder: "task-1", epoch: 1, token: "t", expiresAtMs: nowMs + TTL, owner: "task-1", until: nowMs + TTL });

                await expect(lease().run("k", async () => "ok", { owner: { id: "task-1", epoch: 2 } })).resolves.toBe("ok");
            });
        });
    });

    describe("transaction contention", () => {
        // A db whose first `failures` transactions throw like the Admin SDK
        // does when it gives up under contention.
        function contendedDb(failures: number, error: unknown = Object.assign(new Error("10 ABORTED"), { code: 10 })) {
            let left = failures;
            return {
                ...fakeDb,
                runTransaction: (fn: any) => (left-- > 0 ? Promise.reject(error) : fakeDb.runTransaction(fn)),
            };
        }
        const leaseOn = (db: any) =>
            defineLease({
                db,
                at: (k: string) => fakeDb.doc(`leases/${k}`),
                ttlMs: TTL,
                runtime: { now: () => nowMs, newToken: () => `t${tokens++}`, sleep: async () => undefined },
            });

        it("retries an acquire that lost to contention within the wait budget", async () => {
            await expect(
                leaseOn(contendedDb(2)).run("k", async () => "ok", { wait: { attempts: 3, backoffMs: 1 } }),
            ).resolves.toBe("ok");
        });

        it("reports exhausted contention as LeaseBusyError, not a raw Firestore error", async () => {
            const fn = jest.fn();
            await expect(
                leaseOn(contendedDb(3)).run("k", fn, { wait: { attempts: 3, backoffMs: 1 } }),
            ).rejects.toMatchObject({ name: "LeaseBusyError", holder: CONTENDED_HOLDER });
            expect(fn).not.toHaveBeenCalled();
        });

        it("rethrows any other acquire error at once", async () => {
            const boom = Object.assign(new Error("permission"), { code: 7 });
            await expect(
                leaseOn(contendedDb(1, boom)).run("k", jest.fn(), { wait: { attempts: 5, backoffMs: 1 } }),
            ).rejects.toBe(boom);
        });
    });

    describe("owner epochs (Cloud Tasks retries)", () => {
        beforeEach(() => holdAs("leases/k", { holder: "task-1", epoch: 1 }));

        it("lets a later attempt of the holder's own task take a live lease over", async () => {
            await expect(
                makeLease().run("k", async () => "ok", { owner: { id: "task-1", epoch: 2 } }),
            ).resolves.toBe("ok");
        });

        it.each([1, 0])("treats epoch %i of the same task as busy (duplicate or stale delivery)", async (epoch) => {
            await expect(
                makeLease().run("k", jest.fn(), { owner: { id: "task-1", epoch } }),
            ).rejects.toBeInstanceOf(LeaseBusyError);
        });

        it("never lets a different task take over, whatever its epoch", async () => {
            await expect(
                makeLease().run("k", jest.fn(), { owner: { id: "task-2", epoch: 99 } }),
            ).rejects.toBeInstanceOf(LeaseBusyError);
        });
    });

    describe("fenced transactions", () => {
        it("commits while the lease is held", async () => {
            await makeLease().run("k", async (held) => {
                await held.tx(async (tx) => tx.set(fakeDb.doc("results/k"), { done: true }));
            });
            expect(store.get("results/k")).toEqual({ done: true });
        });

        it("refuses to commit after another acquisition took the lease, and writes nothing", async () => {
            await makeLease().run("k", async (held) => {
                holdAs("leases/k", { holder: "usurper", token: "usurper-token" });
                await expect(
                    held.tx(async (tx) => tx.set(fakeDb.doc("results/k"), { done: true })),
                ).rejects.toBeInstanceOf(LeaseLostError);
            });
            expect(store.has("results/k")).toBe(false);
        });

        it("still commits through an expired lease nobody took over", async () => {
            await makeLease().run("k", async (held) => {
                nowMs += TTL * 10;
                await held.tx(async (tx) => tx.set(fakeDb.doc("results/k"), { done: true }));
            });
            expect(store.get("results/k")).toEqual({ done: true });
        });
    });

    it("propagates fn's error and still releases", async () => {
        await expect(
            makeLease().run("k", async () => {
                throw new Error("boom");
            }),
        ).rejects.toThrow("boom");
        expect(store.get("leases/k")).toEqual({ holder: null });
    });

    it("never clears a lease another acquisition holds by the time it releases", async () => {
        await makeLease().run("k", async () => {
            holdAs("leases/k", { holder: "usurper", token: "usurper-token" });
        });
        expect(store.get("leases/k")?.token).toBe("usurper-token");
    });

    describe("deleteOnRelease", () => {
        const lease = () => makeLease({ deleteOnRelease: true });

        it("holds a document while fn runs and leaves none after success or failure", async () => {
            await lease().run("k", async () => {
                expect(store.get("leases/k")).toMatchObject({ expiresAtMs: nowMs + TTL });
            });
            expect(store.has("leases/k")).toBe(false);

            await expect(lease().run("k", async () => {
                throw new Error("boom");
            })).rejects.toThrow("boom");
            expect(store.has("leases/k")).toBe(false);
        });

        it("still excludes a second acquirer while held", async () => {
            await lease().run("k", async () => {
                await expect(lease().run("k", async () => "second")).rejects.toThrow(LeaseBusyError);
            });
        });

        it("a busy acquire writes nothing", async () => {
            holdAs("leases/k", {});
            const before = store.get("leases/k");
            await expect(lease().run("k", async () => "second")).rejects.toThrow(LeaseBusyError);
            expect(store.get("leases/k")).toEqual(before);
        });

        it("never deletes a lease another acquisition holds by the time it releases", async () => {
            await lease().run("k", async () => {
                holdAs("leases/k", { holder: "usurper", token: "usurper-token" });
            });
            expect(store.get("leases/k")?.token).toBe("usurper-token");
        });

        it("replaces and then removes a free document left by a keeping release", async () => {
            store.set("leases/k", { holder: null });
            await expect(lease().run("k", async () => "ok")).resolves.toBe("ok");
            expect(store.has("leases/k")).toBe(false);
        });

        it("a failed release leaves an expiring document (the crash residual)", async () => {
            let calls = 0;
            const db = {
                ...fakeDb,
                doc: fakeDb.doc,
                runTransaction: (fn: any) => {
                    calls++;
                    return calls === 1
                        ? fakeDb.runTransaction(fn)
                        : Promise.reject(new Error("release failed"));
                },
            };
            const crashing = defineLease({
                db: db as any,
                at: (key: string) => fakeDb.doc(`leases/${key}`),
                deleteOnRelease: true,
                ttlMs: TTL,
                runtime: { now: () => nowMs, newToken: () => `t${tokens++}` },
            });
            await expect(crashing.run("k", async () => "ok")).resolves.toBe("ok");
            expect(store.get("leases/k")).toMatchObject({ expiresAtMs: nowMs + TTL });

            nowMs += TTL + 1;
            await expect(lease().run("k", async () => "next")).resolves.toBe("next");
            expect(store.has("leases/k")).toBe(false);
        });

        it("refuses field mode, whose document is not the lease's to delete", () => {
            expect(() => makeLease({ deleteOnRelease: true, field: "lease" })).toThrow(/document mode/);
        });
    });

    describe("field mode (lease on an existing domain document)", () => {
        it("stores the lease in the field, keeps other fields, and hands the domain doc to tx()", async () => {
            store.set("stories/s1", { title: "Avó", processingStatus: "uploaded" });
            const lease = makeLease({ field: "transcriptionLease" });
            await lease.run("s1", async (held) => {
                expect(store.get("stories/s1")?.transcriptionLease).toMatchObject({ token: held.token });
                await held.tx(async (tx, snap) => {
                    expect(snap.data()?.title).toBe("Avó");
                    tx.update(fakeDb.doc("stories/s1"), { processingStatus: "transcribed" });
                });
            });
            expect(store.get("stories/s1")).toEqual({
                title: "Avó",
                processingStatus: "transcribed",
                transcriptionLease: null,
            });
        });

        it("throws LeaseTargetMissingError instead of creating the domain document", async () => {
            const fn = jest.fn();
            await expect(makeLease({ field: "transcriptionLease" }).run("missing", fn)).rejects.toBeInstanceOf(
                LeaseTargetMissingError,
            );
            expect(fn).not.toHaveBeenCalled();
            expect(store.has("stories/missing")).toBe(false);
        });
    });

    it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY])("rejects ttlMs %p at definition time", (ttlMs) => {
        expect(() => defineLease({ db: fakeDb, at: (k: string) => fakeDb.doc(`leases/${k}`), ttlMs })).toThrow(
            /ttlMs/,
        );
    });
});
