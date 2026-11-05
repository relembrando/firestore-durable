# Examples

Each file is one primitive wired into Cloud Functions Gen2. They compile in CI
(`npm run typecheck`) but are not deployed or run. The HTTP and business helpers
are stubs in `shared.ts`.

| File | Shows |
|---|---|
| [01-lease.ts](01-lease.ts) | Serializing a third-party API per user, with a fenced write |
| [02-enqueue-once.ts](02-enqueue-once.ts) | Debounced and one-off Cloud Tasks |
| [03-ledger.ts](03-ledger.ts) | Webhook dedupe in both modes |
| [04-outbox.ts](04-outbox.ts) | Transactional outbox with dispatch and a sweep |
| [05-keyed-object.ts](05-keyed-object.ts) | Per-conversation FIFO, journaled send, dead letters, purge |
