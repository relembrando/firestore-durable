// `*.emulator.test.ts` run against a real Firestore emulator through the real
// firebase-admin SDK: server-side transactions, contention, aborts and retries,
// which the in-memory fake used by the unit suite cannot show.
const base = require('./jest.config');
module.exports = {
    ...base,
    testMatch: ['**/*.emulator.test.ts'],
    setupFilesAfterEnv: ['<rootDir>/test/helpers/emulator-setup.cjs'],
    clearMocks: false,
    testTimeout: 30000,
};
