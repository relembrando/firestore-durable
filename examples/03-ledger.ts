// Handling an external event once.
import { FieldValue, getFirestore } from "firebase-admin/firestore";
import { onRequest } from "firebase-functions/v2/https";
import { defineLedger } from "../src";
import { applySubscriptionChange } from "./shared";

const db = getFirestore();
const DAY_MS = 24 * 60 * 60 * 1000;

const events = defineLedger({
    db,
    at: (id) => db.collection("webhookEvents").doc(id),
    // Must outlast the provider's redelivery window. The TTL policy on this field
    // is configured outside the library, in firestore.indexes.json.
    ttl: { field: "expiresAt", ms: 7 * DAY_MS },
});

export const webhook = onRequest(async (req, res) => {
    const event = req.body as { id: string; type: string; userId: string };

    if (event.type === "credit.added") {
        // The whole effect fits one transaction: exactly once on its own.
        const result = await events.applyOnce(event.id, async (tx) => {
            tx.set(db.doc(`users/${event.userId}`), { credits: FieldValue.increment(1) }, { merge: true });
        });
        res.status(200).send(result.status);
        return;
    }

    // The effect calls other systems: it must be idempotent. The marker only
    // skips later redeliveries, and a throw leaves no marker, so a 5xx retries it.
    const result = await events.runAndRecord(event.id, () => applySubscriptionChange(event), {
        fields: { type: event.type },
    });
    res.status(200).send(result.status);
});
