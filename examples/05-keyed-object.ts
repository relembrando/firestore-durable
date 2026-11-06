// A mailbox per conversation: messages are handled one at a time and in order
// within a conversation, while different conversations run in parallel.
import { FieldValue, getFirestore } from "firebase-admin/firestore";
import { getFunctions } from "firebase-admin/functions";
import { logger } from "firebase-functions";
import { onRequest, onCall } from "firebase-functions/v2/https";
import { onSchedule } from "firebase-functions/v2/scheduler";
import { onTaskDispatched } from "firebase-functions/v2/tasks";
import { defineKeyedObject, defineTaskQueue } from "../src";
import type { KeyedObjectKick } from "../src";
import { sendReply } from "./shared";

const db = getFirestore();
const DAY_MS = 24 * 60 * 60 * 1000;

interface InboundMessage {
    from: string;
    text: string;
}
interface ConversationState {
    messageCount: number;
}

const kicks = defineTaskQueue<KeyedObjectKick>({
    enqueue: (payload, { id, scheduleDelaySeconds }) =>
        getFunctions().taskQueue("drainConversation").enqueue(payload, { id, scheduleDelaySeconds }),
});

const conversations = defineKeyedObject<InboundMessage, ConversationState>({
    db,
    name: "conversations",
    mode: "fifo",
    queue: kicks,
    leaseMs: 2 * 60 * 1000, // must exceed the worker's 60 s timeout
    maxAttempts: 12,
    backoff: { baseMs: 15 * 1000, maxMs: 60 * 60 * 1000 },
    // TTL policies on `expiresAt` for mailbox, mailboxSent, mailboxDead and, with
    // objectTtl, the object collection are configured in firestore.indexes.json.
    inboxTtl: { field: "expiresAt", ms: 8 * DAY_MS },
    objectTtl: { ms: 20 * DAY_MS },
    drainBudget: { messages: 1, ms: 30 * 1000 },
    onDeadLetter: ({ key, seq, attempts, lastError }) => {
        // Never log the message: it is user content.
        logger.error("conversation message gave up", { key, seq, attempts, lastError });
    },
    deleteField: () => FieldValue.delete(),
});

export const inboundWebhook = onRequest(async (req, res) => {
    const { id, from, text } = req.body as { id: string; from: string; text: string };
    // A provider redelivery with the same id is a duplicate.
    await conversations.send(from, { from, text }, { id });
    res.status(200).send("ok");
});

export const drainConversation = onTaskDispatched<KeyedObjectKick>(async (request) => {
    await conversations.drain(
        request.data.key,
        async (ctx, msg) => {
            // A redelivery after a crash returns the stored result instead of sending again.
            await ctx.once("reply", () => sendReply(msg.from, `Got: ${msg.text}`));
            // Fenced by the lease, and advances the mailbox in the same commit.
            await ctx.tx(async (_tx, state) => ({ messageCount: (state?.messageCount ?? 0) + 1 }));
        },
        { owner: { id: request.id, epoch: request.retryCount } },
    );
});

// Backstop for lost kicks. Keep olderThanMs above leaseMs, and the period at or below it.
export const sweepConversations = onSchedule("every 5 minutes", async () => {
    const { failed, error } = await conversations.sweep({ olderThanMs: 5 * 60 * 1000, limit: 500 });
    if (failed) logger.error("conversation sweep had failures", { failed, error });
});

// Erasure: deletes the object, its messages, markers and dead letters.
export const forgetConversation = onCall(async (request) => {
    await conversations.purge(request.data.phone as string);
});
