module.exports = {
  parser: '@typescript-eslint/parser',
  parserOptions: {
    sourceType: 'module',
    project: "./tsconfig.json",
    tsconfigRootDir: __dirname,
    ecmaVersion: 2018,
  },
  plugins: [
    "@typescript-eslint/eslint-plugin",
    "eslint-plugin-tsdoc",
    "eslint-plugin-jsdoc",
  ],
  extends:  [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
  ],
  env : {
      node: true,
  },
  ignorePatterns: [".eslintrc.js", "test", "coverage", "dist"],
  overrides: [
    {
      files: ['*.ts'],
      rules: {
        'no-undef': 'off',
      },
    },
  ],
  rules: {
    "tsdoc/syntax": "error",
    "jsdoc/require-param": "error",
    "jsdoc/require-returns": "error",
    "jsdoc/require-jsdoc": [
      "error",
      {
        publicOnly: {
          cjs: true,
          esm: true,
          window: true,
        },
        require: {
          ArrowFunctionExpression: true,
          ClassDeclaration: true,
          ClassExpression: true,
          FunctionDeclaration: true,
          FunctionExpression: true,
          MethodDefinition: true,
        },
        contexts: [
          "TSInterfaceDeclaration",
          "TSTypeAliasDeclaration",
          "TSEnumDeclaration",
          "TSPropertySignature",
        ],
      },
    ]
  }
};
