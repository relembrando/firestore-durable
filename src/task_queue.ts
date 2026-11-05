import { createHash, randomUUID } from "crypto";

/**
 * Idempotent Cloud Tasks enqueue: a deterministic task id derived from a
 * caller key, with "already exists" read as success.
 *
 * Task id shape (load-bearing, see the rolling-deploy invariant below):
 *
 *     <prefix>-<sha256(key) as hex, first 32 chars>[-<suffix>]
 *
 * where the suffix depends on the window:
 * - `{ seconds: n }`: `floor(nowMs / 1000 / n)`, a time bucket. Enqueues of the
 *   same key inside one bucket coalesce to one task (debounce); the next
 *   bucket gets a fresh id.
 * - `"forever"`: no suffix. One task per key for as long as Cloud Tasks
 *   remembers the id.
 * - `"unique"`: a random UUID. Never coalesces (an explicit re-send); the
 *   digest prefix only keeps ids grouped and well distributed.
 *
 * Invariants:
 * - The id is a pure function of (prefix, key, window, now). Changing the
 *   format, the hash or the bucket arithmetic changes ids, and during a
 *   rolling deploy old and new instances enqueue concurrently: a format
 *   change silently stops them coalescing with each other. Callers migrating
 *   a hand-rolled id must pin the exact strings in a test.
 * - The key is hashed, never sanitized: raw ids are caller-controlled and
 *   unbounded, so sanitizing could collide (`a:b` vs `a_b`) or exceed the
 *   500-char task-id limit. Hashed ids also spread load across the queue.
 * - Cloud Tasks keeps an executed or deleted id unusable for up to about a
 *   day. A key that must enqueue again after its task ran needs a time window
 *   wider than one burst but narrower than the re-enqueue cadence; `"forever"`
 *   would read the later enqueue as a duplicate and drop it.
 * - `"duplicate"` means a task with this id exists or existed, not that it is
 *   still pending or that it succeeded. Callers needing delivery guarantees
 *   beyond "a task was queued once" must reconcile on their own state.
 * - Any other enqueue error propagates unchanged; the caller decides whether
 *   it is a throw (let the trigger retry) or a logged `false`.
 */

/** How long an enqueue for the same key coalesces with earlier ones. */
export type EnqueueWindow = { seconds: number } | "forever" | "unique";

export interface EnqueueOnceOptions {
    /** Leading id component, e.g. the target kind. Not hashed. */
    prefix: string;
    /** The identity being deduplicated. Hashed into the id. */
    key: string;
    window: EnqueueWindow;
    /** Passed as `scheduleDelaySeconds`; omitted from the call when absent. */
    delaySeconds?: number;
}

export type EnqueueOutcome = "created" | "duplicate";

/** The subset of a task queue's enqueue options this primitive sets. */
export interface TaskEnqueueOptions {
    id: string;
    scheduleDelaySeconds?: number;
}

/** Injectable effects, so tests (and other runtimes) control time and ids. */
export interface TaskQueueRuntime {
    now: () => number;
    newUniqueSuffix: () => string;
}

export interface TaskQueueConfig<P> {
    /**
     * The actual enqueue, e.g.
     * `(p, o) => getFunctions().taskQueue(name).enqueue(p, o)`. Resolved per
     * call, so the queue name (and the SDK) can stay lazy.
     */
    enqueue: (payload: P, options: TaskEnqueueOptions) => Promise<unknown>;
    runtime?: Partial<TaskQueueRuntime>;
}

export interface TaskQueue<P> {
    enqueueOnce(payload: P, options: EnqueueOnceOptions): Promise<EnqueueOutcome>;
}

const defaultRuntime: TaskQueueRuntime = {
    // Wrapped, not `Date.now` by reference, so a spy installed later still applies.
    now: () => Date.now(),
    newUniqueSuffix: () => randomUUID(),
};

/**
 * True for the error the Firebase Admin task queue raises when the id is
 * taken (`functions/task-already-exists`), gRPC ALREADY_EXISTS (code 6, the
 * raw `@google-cloud/tasks` client), or any error whose message says so
 * (other client libraries, emulators).
 */
export function isTaskAlreadyExistsError(err: unknown): boolean {
    const e = err as { code?: unknown; message?: unknown } | null | undefined;
    return e?.code === "functions/task-already-exists" || e?.code === 6 || /already.?exists/i.test(String(e?.message));
}

/** The task id {@link TaskQueue.enqueueOnce} uses for these options at `nowMs`. */
export function taskIdFor(
    options: Pick<EnqueueOnceOptions, "prefix" | "key" | "window">,
    nowMs: number,
    newUniqueSuffix: () => string = defaultRuntime.newUniqueSuffix,
): string {
    const digest = createHash("sha256").update(options.key).digest("hex").slice(0, 32);
    const base = `${options.prefix}-${digest}`;
    const { window } = options;
    if (window === "forever") return base;
    if (window === "unique") return `${base}-${newUniqueSuffix()}`;
    if (!Number.isFinite(window.seconds) || window.seconds <= 0) {
        throw new Error(`enqueueOnce: window.seconds must be a positive number, got ${window.seconds}`);
    }
    // `/ 1000 / seconds`, not `/ (seconds * 1000)`: this is the arithmetic the
    // first callers shipped with, and the two agree on every integer `nowMs`.
    return `${base}-${Math.floor(nowMs / 1000 / window.seconds)}`;
}

export function defineTaskQueue<P>(config: TaskQueueConfig<P>): TaskQueue<P> {
    const runtime: TaskQueueRuntime = { ...defaultRuntime, ...config.runtime };
    return {
        async enqueueOnce(payload, options) {
            const id = taskIdFor(options, runtime.now(), runtime.newUniqueSuffix);
            const enqueueOptions: TaskEnqueueOptions =
                options.delaySeconds === undefined ? { id } : { id, scheduleDelaySeconds: options.delaySeconds };
            try {
                await config.enqueue(payload, enqueueOptions);
                return "created";
            } catch (err: unknown) {
                if (isTaskAlreadyExistsError(err)) return "duplicate";
                throw err;
            }
        },
    };
}
