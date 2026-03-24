import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@onlydoge/shared-kernel':
        '/Users/simonbetton/dev/sb/indexer/packages/shared-kernel/src/index.ts',
      '@onlydoge/api': '/Users/simonbetton/dev/sb/indexer/apps/api/src/index.ts',
      '@onlydoge/indexer-app': '/Users/simonbetton/dev/sb/indexer/apps/indexer/src/index.ts',
      '@onlydoge/platform': '/Users/simonbetton/dev/sb/indexer/packages/platform/src/index.ts',
      '@onlydoge/access-control':
        '/Users/simonbetton/dev/sb/indexer/packages/modules/access-control/src/index.ts',
      '@onlydoge/network-catalog':
        '/Users/simonbetton/dev/sb/indexer/packages/modules/network-catalog/src/index.ts',
      '@onlydoge/entity-labeling':
        '/Users/simonbetton/dev/sb/indexer/packages/modules/entity-labeling/src/index.ts',
      '@onlydoge/investigation-query':
        '/Users/simonbetton/dev/sb/indexer/packages/modules/investigation-query/src/index.ts',
      '@onlydoge/indexing-pipeline':
        '/Users/simonbetton/dev/sb/indexer/packages/modules/indexing-pipeline/src/index.ts',
    },
  },
  test: {
    globals: true,
    include: ['tests/**/*.test.ts'],
    environment: 'node',
    coverage: {
      reporter: ['text', 'html'],
    },
  },
});
