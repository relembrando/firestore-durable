// Record the side effect in the same transaction as the domain change, deliver
// it later with retries. Delivery is at-least-once, so pass a stable id to the
// receiver.
import { FieldValue, getFirestore, Timestamp } from "firebase-admin/firestore";
import { onCall } from "firebase-functions/v2/https";
import { onDocumentCreated } from "firebase-functions/v2/firestore";
import { onSchedule } from "firebase-functions/v2/scheduler";
import { defineOutbox } from "../src";
import { sendEmail } from "./shared";

const db = getFirestore();
const DAY_MS = 24 * 60 * 60 * 1000;

const emails = defineOutbox({
    db,
    at: (id) => db.collection("emailOutbox").doc(id),
    items: () => db.collection("emailOutbox"),
    leaseMs: 2 * 60 * 1000, // must outlive deliver()
    backoff: { baseMs: 30 * 1000, maxMs: 60 * 60 * 1000 },
    ttl: { field: "expiresAt", ms: 7 * DAY_MS },
    maxAge: {
        ms: 3 * DAY_MS,
        from: (data) => (data.createdAt as Timestamp).toMillis(),
        outcome: { kind: "settle", state: "expired" },
    },
    deleteField: () => FieldValue.delete(),
});

export const placeOrder = onCall(async (request) => {
    const orderId = db.collection("orders").doc().id;
    await db.runTransaction(async (tx) => {
        tx.create(db.doc(`orders/${orderId}`), { total: request.data.total, to: request.data.email });
        emails.put(tx, `order-confirmation-${orderId}`, { to: request.data.email });
    });
    return { orderId };
});

async function dispatch(id: string): Promise<void> {
    await emails.dispatch<{ to: string }>(id, {
        claim: async ({ data, expired }) =>
            expired
                ? { kind: "settle", state: "expired" }
                : { kind: "claim", value: { to: data.to as string } },
        deliver: async ({ to }) => {
            const result = await sendEmail(to, id);
            if (result === "accepted") return { kind: "settle", state: "sent" };
            if (result === "rejected") return { kind: "settle", state: "invalid" };
            return { kind: "retry" };
        },
    });
}

// Fast path: deliver right after the commit.
export const onEmailQueued = onDocumentCreated("emailOutbox/{id}", (event) => dispatch(event.params.id));

// Backstop: retries, lost triggers and crashed dispatchers.
export const sweepEmails = onSchedule("every 5 minutes", async () => {
    await emails.sweep({ limit: 100, concurrency: 5 }, dispatch);
});
