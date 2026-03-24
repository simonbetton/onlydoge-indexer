import { startApiServer } from '@onlydoge/api';
import { startIndexer } from '@onlydoge/indexer-app';
import { createRuntime } from '@onlydoge/platform';
import { Command } from 'commander';

interface CliOptions {
  database?: string;
  ip?: string;
  mode?: string;
  port?: string;
  storage?: string;
  warehouse?: string;
}

interface BindSettings {
  ip: string;
  port: number;
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    signal.addEventListener(
      'abort',
      () => {
        resolve();
      },
      { once: true },
    );
  });
}

function createProgram(): Command {
  const program = new Command();

  program
    .option('--mode <mode>', 'Runtime mode: both|http|indexer')
    .option('--database <uri>', 'Metadata database URI')
    .option('--storage <location>', 'Raw block storage location')
    .option('--warehouse <location>', 'Warehouse location')
    .option('--ip <address>', 'HTTP bind address')
    .option('--port <port>', 'HTTP port');

  return program;
}

function applyEnvironmentOverrides(options: CliOptions): void {
  if (options.database) {
    process.env.ONLYDOGE_DATABASE = options.database;
  }
  if (options.storage) {
    process.env.ONLYDOGE_STORAGE = options.storage;
  }
  if (options.warehouse) {
    process.env.ONLYDOGE_WAREHOUSE = options.warehouse;
  }
}

function resolveBindSettings(options: CliOptions): BindSettings {
  return {
    ip: options.ip ?? process.env.ONLYDOGE_IP ?? '127.0.0.1',
    port: options.port ? Number(options.port) : Number(process.env.ONLYDOGE_PORT ?? 2277),
  };
}

function logRuntimeStartup(runtime: Awaited<ReturnType<typeof createRuntime>>): void {
  console.log(
    `[onlydoge] runtime started mode=${runtime.settings.mode} database=${runtime.settings.database.driver} storage=${runtime.settings.storage.driver} warehouse=${runtime.settings.warehouse.driver}`,
  );
}

function shouldStartDegradedHealthServer(_error: unknown): boolean {
  return process.env.NODE_ENV === 'production';
}

async function startDegradedHealthServer(
  bind: BindSettings,
  signal: AbortSignal,
  error: unknown,
): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[onlydoge] startup failed: ${message}`);
  console.error(
    `[onlydoge] starting degraded health-only server on http://${bind.ip}:${bind.port}`,
  );

  const server = Bun.serve({
    hostname: bind.ip,
    port: bind.port,
    fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === '/up') {
        return new Response('ok', { status: 200 });
      }

      return Response.json(
        {
          error: 'service unavailable',
        },
        { status: 503 },
      );
    },
  });

  await waitForAbort(signal);
  server.stop();
}

async function main(): Promise<void> {
  const program = createProgram();
  program.parse();
  const options = program.opts<CliOptions>();
  applyEnvironmentOverrides(options);
  const bind = resolveBindSettings(options);
  const abortController = new AbortController();

  for (const event of ['SIGINT', 'SIGTERM']) {
    process.on(event, () => {
      abortController.abort();
    });
  }

  const runtime = await createRuntime({
    ...(options.mode ? { mode: options.mode } : {}),
    ...(bind.ip ? { ip: bind.ip } : {}),
    ...(Number.isFinite(bind.port) ? { port: bind.port } : {}),
  });
  logRuntimeStartup(runtime);

  const server = runtime.settings.isHttp ? await startApiServer(runtime) : null;
  if (runtime.settings.isHttp) {
    console.log(
      `[onlydoge] api listening on http://${runtime.settings.ip}:${runtime.settings.port}`,
    );
  }

  if (runtime.settings.isIndexer) {
    console.log('[onlydoge] indexer loop started');
    const indexerPromise = startIndexer(runtime, abortController.signal);
    if (runtime.settings.isHttp) {
      await waitForAbort(abortController.signal);
      await server?.stop();
    } else {
      await indexerPromise;
    }
  } else if (runtime.settings.isHttp) {
    await waitForAbort(abortController.signal);
    await server?.stop();
  }
}

void main().catch((error) => {
  const program = createProgram();
  program.parse();
  const options = program.opts<CliOptions>();
  applyEnvironmentOverrides(options);
  const bind = resolveBindSettings(options);
  const abortController = new AbortController();

  for (const event of ['SIGINT', 'SIGTERM']) {
    process.on(event, () => {
      abortController.abort();
    });
  }

  if (!shouldStartDegradedHealthServer(error)) {
    console.error(
      `[onlydoge] startup failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    process.exit(1);
  }

  void startDegradedHealthServer(bind, abortController.signal, error).catch((serverError) => {
    console.error(
      `[onlydoge] degraded health server failed: ${serverError instanceof Error ? serverError.message : String(serverError)}`,
    );
    process.exit(1);
  });
});
