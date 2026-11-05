/** @type {import('jest').Config} */
module.exports = {
    testEnvironment: 'node',
    roots: ['<rootDir>/test'],
    testMatch: ['**/*.test.ts', '!**/*.emulator.test.ts'],
    moduleFileExtensions: ['ts', 'js', 'json'],
    transform: {
        '^.+\\.ts$': ['@swc/jest', {
            jsc: { parser: { syntax: 'typescript' }, target: 'es2022' },
            module: { type: 'commonjs' },
        }],
    },
    clearMocks: true,
};
