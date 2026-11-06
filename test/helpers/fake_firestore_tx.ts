/**
 * In-memory Firestore fake with REAL transaction semantics where they matter:
 * writes are buffered and applied only on commit (a thrown handler leaves
 * nothing behind), `create` fails on an existing doc, and a read after a write
 * throws exactly like the Admin SDK does.
 *
 * Use from a `jest.mock` factory (SWC hoists the factory above imports, so the
 * module must be `require`d INSIDE it, AGENTS.md §9):
 *
 *   jest.mock("firebase-admin/firestore", () =>
 *       require("./helpers/fake_firestore_tx").firestoreModule());
 *
 * and import `store` normally in the test body: both resolve to the same module
 * instance within one test file.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

export type Data = Record<string, any>;

export const store = new Map<string, Data>();
let autoId = 0;
export function resetStore(): void {
    store.clear();
    autoId = 0;
}
export function nextAutoId(): number {
    return autoId++;
}

// Deep-copies plain maps/arrays so a snapshot never aliases the store, while
// keeping Timestamp-like objects (anything with `toMillis`) and Dates (which
// Firestore stores as Timestamps) by reference.
function clone<T>(value: T): T {
    if (Array.isArray(value)) return value.map(clone) as T;
    if (
        value &&
        typeof value === "object" &&
        !(value instanceof Date) &&
        typeof (value as any).toMillis !== "function"
    ) {
        return Object.fromEntries(
            Object.entries(value as Data).map(([k, v]) => [k, clone(v)]),
        ) as T;
    }
    return value;
}

function snap(path: string) {
    const data = store.get(path);
    return {
        exists: data !== undefined,
        id: path.split("/").pop(),
        ref: docRef(path),
        data: () => (data === undefined ? undefined : clone(data)),
        get: (fieldPath: string) =>
            clone(fieldPath.split(".").reduce<any>((o, k) => o?.[k], data)),
    };
}

function isSentinel(value: unknown, key: string): boolean {
    return !!value && typeof value === "object" && key in (value as Data);
}

// Dotted keys (update) write the nested leaf; plain nested objects under
// `merge` deep-merge like Firestore `set(..., {merge:true})`.
function setFieldPath(obj: Data, keyPath: string, value: unknown): void {
    const parts = keyPath.split(".");
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
        const child = cur[parts[i]];
        cur[parts[i]] = child && typeof child === "object" ? { ...child } : {};
        cur = cur[parts[i]];
    }
    const leaf = parts[parts.length - 1];
    if (isSentinel(value, "__increment")) {
        cur[leaf] = Number(cur[leaf] ?? 0) + Number((value as Data).__increment);
        return;
    }
    if (isSentinel(value, "__delete")) {
        delete cur[leaf];
        return;
    }
    cur[leaf] = value;
}

function deepMerge(target: Data, patch: Data): Data {
    const out: Data = { ...target };
    for (const [key, value] of Object.entries(patch)) {
        if (
            value &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            !isSentinel(value, "__serverTimestamp") &&
            !isSentinel(value, "__increment") &&
            !isSentinel(value, "__delete") &&
            !(value instanceof Date) &&
            typeof (value as any).toMillis !== "function"
        ) {
            out[key] = deepMerge(
                out[key] && typeof out[key] === "object" ? out[key] : {},
                value as Data,
            );
        } else {
            setFieldPath(out, key, value);
        }
    }
    return out;
}

type Op =
    | { kind: "set"; path: string; data: Data; merge: boolean }
    | { kind: "create"; path: string; data: Data }
    | { kind: "update"; path: string; data: Data }
    | { kind: "delete"; path: string };

function applyOp(op: Op): void {
    switch (op.kind) {
        case "create":
            if (store.has(op.path)) {
                const err: any = new Error(`ALREADY_EXISTS: ${op.path}`);
                err.code = 6;
                throw err;
            }
            store.set(op.path, deepMerge({}, op.data));
            return;
        case "set":
            store.set(
                op.path,
                op.merge
                    ? deepMerge(store.get(op.path) ?? {}, op.data)
                    : deepMerge({}, op.data),
            );
            return;
        case "update": {
            const current = store.get(op.path);
            if (!current) {
                const err: any = new Error(`NOT_FOUND: ${op.path}`);
                err.code = 5;
                throw err;
            }
            const next = { ...current };
            for (const [key, value] of Object.entries(op.data)) {
                setFieldPath(next, key, value);
            }
            store.set(op.path, next);
            return;
        }
        case "delete":
            store.delete(op.path);
    }
}

function docRef(path: string): any {
    return {
        path,
        id: path.split("/").pop(),
        parent: { path: path.split("/").slice(0, -1).join("/") },
        collection: (sub: string) => collRef(`${path}/${sub}`),
        get: async () => snap(path),
        delete: async () => void store.delete(path),
    };
}

function matches(data: Data, filters: [string, string, unknown][]): boolean {
    return filters.every(([field, op, val]) => {
        const fv = field.split(".").reduce<any>((o, k) => o?.[k], data);
        if (op === "==") return fv === val;
        if (op === "in") return Array.isArray(val) && val.includes(fv);
        throw new Error(`Unsupported operator in fake: ${op}`);
    });
}

/** Document-name order: segment by segment, like Firestore. */
function compareNames(a: string, b: string): number {
    const [x, y] = [a.split("/"), b.split("/")];
    for (let i = 0; i < Math.min(x.length, y.length); i++) {
        if (x[i] !== y[i]) return x[i] < y[i] ? -1 : 1;
    }
    return x.length - y.length;
}

/** `orderBy("__name__")` with its cursors; other orderings are ignored. */
type NameRange = { ordered?: boolean; after?: string; before?: string; mask?: string[] };

function masked(path: string, mask: string[] | undefined) {
    const doc = snap(path);
    if (!mask) return doc;
    const data = doc.data() ?? {};
    const kept = Object.fromEntries(Object.entries(data).filter(([field]) => mask.includes(field)));
    return { ...doc, data: () => kept, get: (field: string) => kept[field] };
}

function runQuery(
    collPath: string,
    group: boolean,
    filters: [string, string, unknown][],
    limit: number | null,
    range: NameRange = {},
) {
    const docs: any[] = [];
    const paths = [...store.keys()];
    if (range.ordered) paths.sort(compareNames);
    for (const path of paths) {
        const data = store.get(path)!;
        const segments = path.split("/");
        const inScope = group
            ? segments.length >= 2 && segments[segments.length - 2] === collPath
            : path.startsWith(`${collPath}/`) &&
              segments.length === collPath.split("/").length + 1;
        if (!inScope || !matches(data, filters)) continue;
        if (range.after !== undefined && compareNames(path, range.after) <= 0) continue;
        if (range.before !== undefined && compareNames(path, range.before) >= 0) continue;
        docs.push(masked(path, range.mask));
        if (limit !== null && docs.length >= limit) break;
    }
    return { docs, size: docs.length, empty: docs.length === 0 };
}

function queryRef(
    path: string,
    group: boolean,
    filters: [string, string, unknown][] = [],
    limit: number | null = null,
    range: NameRange = {},
): any {
    return {
        __query: true,
        doc: (id?: string) => docRef(`${path}/${id ?? `auto_${autoId++}`}`),
        where: (field: string, op: string, val: unknown) =>
            queryRef(path, group, [...filters, [field, op, val]], limit, range),
        orderBy: (field?: unknown) =>
            queryRef(path, group, filters, limit, field === "__name__" ? { ...range, ordered: true } : range),
        startAfter: (name: string) => queryRef(path, group, filters, limit, { ...range, after: name }),
        endBefore: (name: string) => queryRef(path, group, filters, limit, { ...range, before: name }),
        select: (...mask: string[]) => queryRef(path, group, filters, limit, { ...range, mask }),
        limit: (value: number) => queryRef(path, group, filters, value, range),
        get: async () => runQuery(path, group, filters, limit, range),
    };
}

function collRef(path: string): any {
    return queryRef(path, false);
}

/**
 * One transaction attempt that runs `fn` against the live store and then
 * DISCARDS its writes, as if the commit lost to a concurrent writer. For
 * testing what a handler leaves behind in closure state when Firestore retries.
 */
export async function abortedTransactionAttempt(
    fn: (tx: any) => Promise<any>,
): Promise<void> {
    await runTransaction(fn, { commit: false });
}

async function runTransaction(
    fn: (tx: any) => Promise<any>,
    { commit = true }: { commit?: boolean } = {},
): Promise<any> {
    const ops: Op[] = [];
    const tx = {
        get: async (ref: any) => {
            if (ops.length > 0) {
                throw new Error(
                    "Firestore transactions require all reads to be executed before all writes.",
                );
            }
            return ref.__query ? ref.get() : snap(ref.path);
        },
        set: (ref: any, data: Data, opts?: { merge?: boolean }) => {
            ops.push({ kind: "set", path: ref.path, data, merge: !!opts?.merge });
        },
        create: (ref: any, data: Data) => {
            ops.push({ kind: "create", path: ref.path, data });
        },
        update: (ref: any, data: Data) => {
            ops.push({ kind: "update", path: ref.path, data });
        },
        delete: (ref: any) => {
            ops.push({ kind: "delete", path: ref.path });
        },
    };
    const result = await fn(tx);
    if (!commit) return result;
    // Commit: all-or-nothing, like Firestore.
    const backup = new Map(store);
    try {
        for (const op of ops) applyOp(op);
    } catch (err) {
        store.clear();
        for (const [k, v] of backup) store.set(k, v);
        throw err;
    }
    return result;
}


function batch(): any {
    const ops: Op[] = [];
    return {
        set: (ref: any, data: Data, opts?: { merge?: boolean }) =>
            ops.push({ kind: "set", path: ref.path, data, merge: !!opts?.merge }),
        update: (ref: any, data: Data) =>
            ops.push({ kind: "update", path: ref.path, data }),
        delete: (ref: any) => ops.push({ kind: "delete", path: ref.path }),
        commit: async () => {
            for (const op of ops) applyOp(op);
        },
    };
}

export const fakeDb: any = {
    collection: (name: string) => collRef(name),
    collectionGroup: (name: string) => queryRef(name, true),
    doc: (path: string) => docRef(path),
    runTransaction: (fn: (tx: any) => Promise<any>) => runTransaction(fn),
    getAll: async (...refs: any[]) => refs.map((ref) => snap(ref.path)),
    batch,
};

class MockTimestamp {
    constructor(private readonly ms: number) {}
    toMillis(): number {
        return this.ms;
    }
    toDate(): Date {
        return new Date(this.ms);
    }
    static fromMillis(ms: number): MockTimestamp {
        return new MockTimestamp(ms);
    }
    static fromDate(date: Date): MockTimestamp {
        return new MockTimestamp(date.getTime());
    }
    static now(): MockTimestamp {
        return new MockTimestamp(Date.now());
    }
}

/** The `firebase-admin/firestore` module shape the code under test imports. */
export function firestoreModule(): Record<string, unknown> {
    return {
        getFirestore: () => fakeDb,
        Timestamp: MockTimestamp,
        FieldValue: {
            serverTimestamp: () => ({ __serverTimestamp: true }),
            increment: (value: number) => ({ __increment: value }),
            delete: () => ({ __delete: true }),
        },
    };
}
