/**
 * `src/task_queue.ts`: deterministic task ids, windows, and "already
 * exists" as a duplicate. Ids are pinned as literal strings on purpose: the
 * id format is shared with whatever code is deployed next to this one, so a
 * change must show up here as a failing string, not as a still-green regex.
 */

import { createHash } from "crypto";
import { defineTaskQueue, isTaskAlreadyExistsError, taskIdFor } from "../src";

const NOW = 1_800_000_000_000;

function makeQueue(enqueue = jest.fn(async () => undefined), now = () => NOW) {
    let n = 0;
    const queue = defineTaskQueue<{ v: number }>({
        enqueue,
        runtime: { now, newUniqueSuffix: () => `u${n++}` },
    });
    return { queue, enqueue };
}

describe("taskIdFor", () => {
    it("pins the id shape for each window", () => {
        // sha256("k") = 8254c329a92850f6d539dd376f4816ee...
        expect(taskIdFor({ prefix: "p", key: "k", window: { seconds: 5 } }, NOW)).toBe(
            "p-8254c329a92850f6d539dd376f4816ee-360000000",
        );
        expect(taskIdFor({ prefix: "p", key: "k", window: "forever" }, NOW)).toBe(
            "p-8254c329a92850f6d539dd376f4816ee",
        );
        expect(taskIdFor({ prefix: "p", key: "k", window: "unique" }, NOW, () => "x")).toBe(
            "p-8254c329a92850f6d539dd376f4816ee-x",
        );
    });

    it("hashes the key (32 hex chars) rather than embedding or sanitizing it", () => {
        const long = "a:b/".repeat(500);
        const id = taskIdFor({ prefix: "p", key: long, window: "forever" }, NOW);
        expect(id).toBe(`p-${createHash("sha256").update(long).digest("hex").slice(0, 32)}`);
        expect(taskIdFor({ prefix: "p", key: "a:b", window: "forever" }, NOW)).not.toBe(
            taskIdFor({ prefix: "p", key: "a_b", window: "forever" }, NOW),
        );
    });

    it("buckets by window: same bucket coalesces, the next bucket gets a fresh id", () => {
        const at = (ms: number) => taskIdFor({ prefix: "p", key: "k", window: { seconds: 5 } }, ms);
        const start = 360_000_000 * 5_000; // first ms of a 5 s bucket
        expect(at(start)).toBe(at(start + 4_999));
        expect(at(start - 1)).not.toBe(at(start));
        expect(at(start + 5_000)).not.toBe(at(start + 4_999));
    });

    it("bucket arithmetic matches the `nowMs / (seconds * 1000)` form callers shipped with", () => {
        // prompt_card_queue.ts used `floor(now / 43_200_000)`; recompute used
        // `floor(now / 1000 / 5)`. Both must keep producing the same bucket.
        for (const seconds of [5, 12 * 60 * 60]) {
            const w = seconds * 1000;
            const k = Math.floor(NOW / w);
            const samples = [NOW, NOW + 1, 0, 1];
            for (let i = -50; i <= 50; i++) samples.push((k + i) * w - 1, (k + i) * w, (k + i) * w + 1);
            for (const ms of samples) {
                const id = taskIdFor({ prefix: "p", key: "k", window: { seconds } }, ms);
                expect(id.slice(id.lastIndexOf("-") + 1)).toBe(String(Math.floor(ms / w)));
            }
        }
    });

    it.each([0, -5, Number.NaN, Number.POSITIVE_INFINITY])("rejects window.seconds = %p", (seconds) => {
        expect(() => taskIdFor({ prefix: "p", key: "k", window: { seconds } }, NOW)).toThrow(/window.seconds/);
    });
});

describe("defineTaskQueue.enqueueOnce", () => {
    it("enqueues with the derived id and reports created", async () => {
        const { queue, enqueue } = makeQueue();
        await expect(
            queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: { seconds: 5 } }),
        ).resolves.toBe("created");
        expect(enqueue).toHaveBeenCalledTimes(1);
        // No delay requested: the option key is absent, not undefined.
        expect(enqueue.mock.calls[0]).toEqual([{ v: 1 }, { id: "p-8254c329a92850f6d539dd376f4816ee-360000000" }]);
        expect(Object.keys((enqueue.mock.calls[0] as unknown[])[1] as object)).toEqual(["id"]);
    });

    it("passes delaySeconds through as scheduleDelaySeconds", async () => {
        const { queue, enqueue } = makeQueue();
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "forever", delaySeconds: 0 });
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "forever", delaySeconds: 30 });
        expect((enqueue.mock.calls as unknown[][]).map((c) => c[1])).toEqual([
            { id: "p-8254c329a92850f6d539dd376f4816ee", scheduleDelaySeconds: 0 },
            { id: "p-8254c329a92850f6d539dd376f4816ee", scheduleDelaySeconds: 30 },
        ]);
    });

    it("gives each unique enqueue its own id", async () => {
        const { queue, enqueue } = makeQueue();
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "unique" });
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "unique" });
        expect((enqueue.mock.calls as unknown[][]).map((c) => (c[1] as { id: string }).id)).toEqual([
            "p-8254c329a92850f6d539dd376f4816ee-u0",
            "p-8254c329a92850f6d539dd376f4816ee-u1",
        ]);
    });

    it("reads the clock per call (a later bucket gets a new id)", async () => {
        let now = NOW;
        const { queue, enqueue } = makeQueue(undefined, () => now);
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: { seconds: 5 } });
        now += 5_000;
        await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: { seconds: 5 } });
        const ids = (enqueue.mock.calls as unknown[][]).map((c) => (c[1] as { id: string }).id);
        expect(ids[0]).not.toBe(ids[1]);
    });

    it("defaults to Date.now, read at call time", async () => {
        const enqueue = jest.fn(async () => undefined);
        const queue = defineTaskQueue<{ v: number }>({ enqueue });
        const spy = jest.spyOn(Date, "now").mockReturnValue(NOW);
        try {
            await queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: { seconds: 5 } });
        } finally {
            spy.mockRestore();
        }
        expect((enqueue.mock.calls[0] as unknown[])[1]).toEqual({ id: "p-8254c329a92850f6d539dd376f4816ee-360000000" });
    });

    it.each([
        ["the Admin SDK code", { code: "functions/task-already-exists" }],
        ["an Error saying so", new Error("Task already exists in queue")],
        ["a gRPC-style status message", new Error("6 ALREADY_EXISTS: The task cannot be created")],
        ["a hyphenated message", { message: "already-exists" }],
    ])("reports duplicate for %s", async (_label, err) => {
        const { queue } = makeQueue(jest.fn(async () => Promise.reject(err)));
        await expect(queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "forever" })).resolves.toBe(
            "duplicate",
        );
    });

    it.each([
        ["an unrelated Error", new Error("UNAVAILABLE")],
        ["another error code", { code: "functions/invalid-argument", message: "bad id" }],
        ["null", null],
        ["undefined", undefined],
    ])("propagates %s unchanged", async (_label, err) => {
        const { queue } = makeQueue(jest.fn(async () => Promise.reject(err)));
        const outcome = queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "forever" });
        await expect(outcome).rejects.toBe(err);
    });

    it("propagates a synchronous throw from the injected enqueue", async () => {
        const boom = new Error("no app");
        const { queue } = makeQueue(
            jest.fn(() => {
                throw boom;
            }) as unknown as jest.Mock<Promise<undefined>, []>,
        );
        await expect(queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: "forever" })).rejects.toBe(boom);
    });

    it("does not enqueue when the window is invalid", async () => {
        const { queue, enqueue } = makeQueue();
        await expect(
            queue.enqueueOnce({ v: 1 }, { prefix: "p", key: "k", window: { seconds: 0 } }),
        ).rejects.toThrow(/window.seconds/);
        expect(enqueue).not.toHaveBeenCalled();
    });
});

describe("isTaskAlreadyExistsError", () => {
    it("is false for errors that merely mention existence", () => {
        expect(isTaskAlreadyExistsError(new Error("queue does not exist"))).toBe(false);
        expect(isTaskAlreadyExistsError({ code: "functions/not-found" })).toBe(false);
        expect(isTaskAlreadyExistsError({ code: 5, message: "NOT_FOUND" })).toBe(false);
    });

    it("is true for the raw Cloud Tasks client's gRPC ALREADY_EXISTS (code 6)", () => {
        expect(isTaskAlreadyExistsError({ code: 6, message: "" })).toBe(true);
    });
});
