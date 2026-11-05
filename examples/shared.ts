// Stubs that stand in for your application code.
export async function chargeProvider(_userId: string, _amount: number): Promise<{ id: string }> {
    return { id: "provider-1" };
}
export async function sendEmail(_to: string, _idempotencyKey: string): Promise<"accepted" | "rejected" | "unavailable"> {
    return "accepted";
}
export async function sendReply(_to: string, _text: string): Promise<{ messageId: string }> {
    return { messageId: "m1" };
}
export async function applySubscriptionChange(_event: { id: string; userId: string }): Promise<void> {}
