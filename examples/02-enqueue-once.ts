// Deterministic task ids, so a burst of triggers collapses to one task.
import { getFunctions } from "firebase-admin/functions";
import { onDocumentWritten } from "firebase-functions/v2/firestore";
import { onTaskDispatched } from "firebase-functions/v2/tasks";
import { getFirestore } from "firebase-admin/firestore";
import { defineTaskQueue } from "../src";

interface RecountTask {
    listId: string;
}

const recounts = defineTaskQueue<RecountTask>({
    // Injected, so the library has no runtime Firebase import.
    enqueue: (payload, { id, scheduleDelaySeconds }) =>
        getFunctions().taskQueue("recountList").enqueue(payload, { id, scheduleDelaySeconds }),
});

export const onItemWritten = onDocumentWritten("lists/{listId}/items/{itemId}", async (event) => {
    await recounts.enqueueOnce(
        { listId: event.params.listId },
        // One task per list per 30 s window. "forever" = once ever; "unique" = always new.
        { prefix: "recount", key: event.params.listId, window: { seconds: 30 }, delaySeconds: 30 },
    );
});

export const recountList = onTaskDispatched<RecountTask>(async (request) => {
    const list = getFirestore().doc(`lists/${request.data.listId}`);
    const count = (await list.collection("items").count().get()).data().count;
    await list.set({ itemCount: count }, { merge: true });
});
