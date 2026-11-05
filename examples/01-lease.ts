// One provider operation per user at a time, across every function that touches
// it. The lease is held across the network call; external calls never run inside
// a transaction.
import { getFirestore } from "firebase-admin/firestore";
import { onCall, HttpsError } from "firebase-functions/v2/https";
import { defineLease, LeaseBusyError, LeaseLostError } from "../src";
import { chargeProvider } from "./shared";

const db = getFirestore();
const TIMEOUT_SECONDS = 60;

const billingLease = defineLease({
    db,
    at: (userId: string) => db.collection("billingLeases").doc(userId),
    // Must outlive the function, which the platform kills at its timeout.
    ttlMs: (TIMEOUT_SECONDS + 60) * 1000,
});

export const charge = onCall({ timeoutSeconds: TIMEOUT_SECONDS }, async (request) => {
    const userId = request.auth?.uid;
    if (!userId) throw new HttpsError("unauthenticated", "sign in");
    try {
        return await billingLease.run(
            userId,
            async (lease) => {
                const payment = await chargeProvider(userId, 1000);
                // Commits only while this acquisition still holds the lease.
                await lease.tx(async (tx) => {
                    tx.set(db.doc(`users/${userId}/payments/${payment.id}`), { amount: 1000 });
                });
                return payment;
            },
            { wait: { attempts: 6, backoffMs: 500 } },
        );
    } catch (err) {
        if (err instanceof LeaseBusyError) throw new HttpsError("aborted", "try again");
        if (err instanceof LeaseLostError) throw new HttpsError("aborted", "lease lost, try again");
        throw err;
    }
});
