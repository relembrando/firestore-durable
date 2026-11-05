// Setup for jest.emulator.config.js. Runs before every *.emulator.test.ts file
// is loaded, so nothing under test can reach ADC or a live project.
const EMULATOR_PROJECT_ID = 'demo-firestore-durable';

const host = process.env.FIRESTORE_EMULATOR_HOST || '';
if (!/^(127\.0\.0\.1|localhost|\[::1\]):\d+$/.test(host)) {
    throw new Error(
        `Emulator tests require a loopback FIRESTORE_EMULATOR_HOST (got "${host}"). ` +
            'Run them through `npm run test:emulator`.',
    );
}

// A `demo-` project id never maps to real resources, whatever credentials exist.
process.env.GCLOUD_PROJECT = EMULATOR_PROJECT_ID;
process.env.GOOGLE_CLOUD_PROJECT = EMULATOR_PROJECT_ID;
process.env.FIREBASE_CONFIG = JSON.stringify({ projectId: EMULATOR_PROJECT_ID });

const { getApps, initializeApp } = require('firebase-admin/app');
if (getApps().length === 0) initializeApp({ projectId: EMULATOR_PROJECT_ID });

// Every test starts from an empty database.
beforeEach(async () => {
    const url = `http://${host}/emulator/v1/projects/${EMULATOR_PROJECT_ID}/databases/(default)/documents`;
    const res = await fetch(url, { method: 'DELETE' });
    if (!res.ok) throw new Error(`Clearing the Firestore emulator failed: ${res.status} ${await res.text()}`);
});
