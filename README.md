# firestore-durable

Durable-execution primitives for **Cloud Functions, Firestore and Cloud Tasks**:
fenced leases, idempotent task enqueue, an event ledger, a transactional outbox
and per-key mailboxes.

Influenced by Temporal and Restate, but **durable by convergence, not by replay**:
handlers run at least once, every commit is fenced, and re-running is the repair.
There is no extra server to run, no step history to replay and no determinism
requirement on your code.

> **Early release.** The API can still change and nothing is on npm yet. The code
> has a full test suite, including a real Firestore emulator, and most of it has run in a
> staging environment, but it has not run in production yet. Use it with that in
> mind, and read [Guarantees and non-guarantees](#guarantees-and-non-guarantees).

## Why

Serverless backends on Firebase end up hand-writing the same patterns: a lease
here, a "task already exists" catch there, a webhook dedupe collection, an outbox
with a cron behind it. Each copy has slightly different rules for staleness and
failure, and the bugs live in those differences. This package is one tested
version of each, generalized from an application that had written them 2 to 9
times each.

| You need | Use |
|---|---|
| Only one holder per key, across functions, including across network calls | [`defineLease`](#definelease) |
| A burst of triggers to collapse into one Cloud Task | [`defineTaskQueue`](#definetaskqueue) |
| To handle a webhook or inbound message once | [`defineLedger`](#defineledger) |
| To call an external API because a transaction committed | [`defineOutbox`](#defineoutbox) |
| Per-key ordering, with different keys running in parallel | [`defineKeyedObject`](#definekeyedobject) |

### Compared with a workflow engine

| | firestore-durable | Temporal, Restate, DBOS, Inngest, Trigger.dev, Hatchet |
|---|---|---|
| Runs on | Your Firestore and Cloud Tasks | Their server or a SQL/Redis stack; none ships a Firestore backend (checked 2026-11) |
| Model | Primitives you compose | Workflows, with a journal or replay |
| Crash recovery | At-least-once handler, fenced commit, re-run converges | The engine replays or checkpoints your steps |
| Code constraints | Effects must be idempotent, or use `ctx.once` | Determinism or step boundaries |
| Long multi-step workflows | Not the goal | Their strength |

If you need long-running workflows with many steps, timers and signals, use one of
those. This is for teams on Firebase who need the reliability pieces without
adopting a new runtime.

`@fantasticfour/world-firestore-tasks` is the closest project on the same stack. It
is a replay engine behind Vercel Workflow DevKit rather than a set of primitives.

## Install

Not on npm yet. It has no runtime dependencies; `firebase-admin` is a peer
dependency used for types only, and you pass in the Firestore instance. Tested on
Node 24 and `firebase-admin` 14 only.

## Primitives

Runnable-shaped versions of these, wired into Cloud Functions, are in
[examples/](examples/). The full contract of each primitive is in its source header.

### `defineLease`

At most one holder per key. A crashed holder's lease lapses, and writes through
`lease.tx` commit only while the lease is still this acquisition's.

```ts
const lease = defineLease({
    db,
    at: (userId: string) => db.collection("leases").doc(userId),
    ttlMs: 120_000, // must outlive the operation
});

await lease.run(userId, async (held) => {
    const result = await callThirdParty();                 // never inside a transaction
    await held.tx(async (tx) => tx.set(resultRef, result)); // throws LeaseLostError if taken over
}, { wait: { attempts: 6, backoffMs: 500 } });              // else throws LeaseBusyError
```

- `owner: { id: taskName, epoch: retryCount }` lets a Cloud Tasks retry take over
  its own crashed attempt, while a duplicate delivery of the same attempt stays busy.
- `field: "myLease"` keeps the lease on an existing document instead of owning one.
- `legacy` lets it share a key with a hand-rolled lease during a rolling deploy.
- `deleteOnRelease: true` removes the lease document on release, so no document
  outlives a run for a key that may be deleted (a crashed holder still leaves one).
- Expiry compares wall clocks across instances. Size `ttlMs` as the longest
  operation plus the worst clock skew, or inject `runtime.now`.

### `defineTaskQueue`

Deterministic task ids, with "already exists" reported as success.

```ts
const queue = defineTaskQueue<Payload>({
    enqueue: (payload, { id, scheduleDelaySeconds }) =>
        getFunctions().taskQueue("worker").enqueue(payload, { id, scheduleDelaySeconds }),
});

await queue.enqueueOnce(payload, {
    prefix: "recount",
    key: listId,
    window: { seconds: 30 }, // or "forever", or "unique"
    delaySeconds: 30,
}); // "created" | "duplicate"
```

Cloud Tasks only remembers a task id for a limited time, so `"forever"` is not a
permanent guarantee on its own. Use a ledger if a task must never repeat.

### `defineLedger`

Once-only markers for external events, in two modes, because there are two honest
shapes of "exactly once":

| Mode | When | Guarantee |
|---|---|---|
| `applyOnce(id, fn(tx))` | The whole effect is Firestore writes in one transaction | Exactly once: marker and effect commit together |
| `runAndRecord(id, fn)` | The effect calls other systems, and is idempotent | At least once: the marker only skips later redeliveries |

`ttl.ms` must outlast the provider's redelivery window.

### `defineOutbox`

The producer records the side effect in the same transaction as the domain change.
A dispatcher delivers it later, with retries, and no transaction ever waits on the
network.

```ts
await db.runTransaction(async (tx) => {
    tx.create(orderRef, order);
    outbox.put(tx, `confirm-${orderId}`, { to: email });
});

await outbox.dispatch(id, {
    claim: async ({ data, expired }) =>           // inside the claim transaction
        expired ? { kind: "settle", state: "expired" } : { kind: "claim", value: data.to },
    deliver: async (to) =>                        // outside any transaction
        (await send(to, id)) ? { kind: "settle", state: "sent" } : { kind: "retry" },
});

await outbox.sweep({ limit: 100, concurrency: 5 }, dispatchById); // on a schedule
```

Outcomes are `retry`, `settle`, `park` and `drop`. The lease is `nextAttemptAt`
itself, so a crashed dispatcher needs no cleanup. Delivery is **at-least-once**:
the external call can succeed and the finalize still fail, so receivers must dedupe
on a stable id.

### `defineKeyedObject`

A per-key mailbox, in the spirit of Restate's Virtual Objects: at most one active
handler per key, with state committed together with the mailbox's progress.

```ts
const conversations = defineKeyedObject<Message, State>({
    db,
    name: "conversations",
    mode: "fifo",                  // or "latest": only the newest message matters
    queue: kicks,                  // a defineTaskQueue<KeyedObjectKick>
    leaseMs: 120_000,              // must exceed the worker's timeout
    maxAttempts: 12,
    backoff: { baseMs: 15_000, maxMs: 3_600_000 },
    inboxTtl: { field: "expiresAt", ms: 8 * DAY_MS },
    drainBudget: { messages: 1, ms: 30_000 },
    onDeadLetter: (letter) => logger.error("gave up", { key: letter.key, seq: letter.seq }),
    deleteField: () => FieldValue.delete(),
});

await conversations.send(phone, message, { id: providerMessageId }); // "accepted" | "duplicate"

await conversations.drain(key, async (ctx, msg) => {
    await ctx.once("reply", () => sendReply(msg));          // a redelivery reuses the result
    await ctx.tx(async (tx, state) => nextState(state));    // fenced; advances the mailbox
}, { owner: { id: taskName, epoch: retryCount } });
```

- **Ordering:** `seq` follows `send` commit order, not the provider's timestamp.
- **Failure:** a failing head blocks its key, with backoff, until it succeeds or
  moves to the dead-letter collection. Other keys are not affected.
- **Lost kicks:** call `sweep({ olderThanMs, limit, after })` on a schedule. It re-kicks
  a key whose oldest pending message saw no activity for `olderThanMs`, which you keep
  above `leaseMs`. Follow `next` into `after` until it is absent, or keys that stay
  pending hide the ones behind them. Without a sweep, a key that lost every kick waits
  for its next send.
- **Budget one drain per task** if a handler can take close to the worker's timeout.
- **Erasure:** set `objectTtl` to enable `purge(key)` and let idle objects expire.
- **Personal data:** the key is stored as a document id and messages are stored as
  sent. Hash the key and keep payloads minimal if they hold personal data.

It does not need a worker per key or a global queue concurrency of 1. Queue
concurrency is yours to set.

## Guarantees and non-guarantees

- Handlers and deliveries are **at least once**, and a crash re-runs them. Effects
  must be idempotent, go through `ctx.tx`, or use `ctx.once`.
- `ctx.once` narrows the duplicate window but does not close it: a crash between
  the step and its journal write re-runs the step.
- Liveness rests on tasks plus `sweep`. A stranded key waits up to about two sweep
  periods past `olderThanMs`. With the worker down past `inboxTtl`, messages are lost,
  and `onExpired` only fires on that key's next drain.
- Field names and document ids are a wire format shared with every deployed version.
  Changing them is a data migration.

## TTL policies

The library writes the TTL fields, and you configure the policies. In
`firestore.indexes.json`, for a keyed object with `inboxTtl.field: "expiresAt"`:

```json
{
    "fieldOverrides": [
        { "collectionGroup": "mailbox", "fieldPath": "expiresAt", "ttl": true, "indexes": [] },
        { "collectionGroup": "mailboxSent", "fieldPath": "expiresAt", "ttl": true, "indexes": [] },
        { "collectionGroup": "mailboxDead", "fieldPath": "expiresAt", "ttl": true, "indexes": [] }
    ]
}
```

Add the same for your ledger, outbox and, with `objectTtl`, object collections.
Without a policy nothing breaks, but those documents accumulate. Also deny client
access to these collections in your security rules.

## Development

```bash
npm ci
npm run check   # typecheck, build, unit tests, emulator tests (needs Java)
```

`*.emulator.test.ts` runs against a real Firestore emulator through the real Admin
SDK, and each suite includes a control proving its probe can fail.

## License

[Apache-2.0](LICENSE)
