module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    moduleFileExtensions: ['ts', 'js'],
    testMatch: ['**/*.spec.ts'],
    collectCoverage: true,
    collectCoverageFrom: ['src/**/*.ts', 'index.ts'],
    coverageDirectory: 'coverage',
    coverageReporters: ['text', 'lcov'], 
    coveragePathIgnorePatterns: ['interfaces/I.*.ts$'],
    coverageThreshold: {
        global: {
            branches: 100,
            functions: 100,
            lines: 100,
            statements: 100,
        }
    },
};
