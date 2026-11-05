/**
 * `src/` may import only its own files, Node built-ins and
 * `firebase-admin/firestore` TYPES. The Firestore instance and every effect are
 * injected, so the package has no runtime dependency.
 */

import { readdirSync, readFileSync } from "fs";
import { join } from "path";

const DIR = join(__dirname, "..", "src");

function importsOf(source: string): { specifier: string; typeOnly: boolean }[] {
    const re = /^\s*(import|export)\s+(type\s+)?[^;]*?\bfrom\s+["']([^"']+)["']/gm;
    return [...source.matchAll(re)].map((m) => ({ specifier: m[3], typeOnly: Boolean(m[2]) }));
}

function violation(imp: { specifier: string; typeOnly: boolean }): string | null {
    if (imp.specifier.startsWith("./")) return null;
    if (["crypto", "timers/promises"].includes(imp.specifier) || imp.specifier.startsWith("node:")) return null;
    if (imp.specifier === "firebase-admin/firestore" && imp.typeOnly) return null;
    return imp.specifier;
}

describe("package boundary", () => {
    const files = readdirSync(DIR).filter((f) => f.endsWith(".ts"));

    it("scans the library files (control: the scan is not vacuous)", () => {
        expect(files).toEqual(expect.arrayContaining(["index.ts", "lease.ts", "ledger.ts", "outbox.ts", "task_queue.ts"]));
        expect(importsOf(readFileSync(join(DIR, "lease.ts"), "utf8")).length).toBeGreaterThan(0);
    });

    it("flags the imports it exists to forbid (control: the rule can fail)", () => {
        const bad = [
            'import { db } from "../app_core";',
            'import { getFirestore } from "firebase-admin/firestore";',
            'import { logger } from "firebase-functions";',
        ].join("\n");
        expect(importsOf(bad).map(violation)).toEqual([
            "../app_core",
            "firebase-admin/firestore",
            "firebase-functions",
        ]);
    });

    it.each(files)("%s imports nothing outside the boundary", (file) => {
        const bad = importsOf(readFileSync(join(DIR, file), "utf8"))
            .map(violation)
            .filter(Boolean);
        expect(bad).toEqual([]);
    });
});
