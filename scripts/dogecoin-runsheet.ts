#!/usr/bin/env bun

import { Command } from 'commander';

interface CreatedApiKey {
  id: string;
  key: string;
}

interface NetworkResponse {
  architecture: 'dogecoin' | 'evm';
  blockTime: number;
  chainId: number;
  createdAt: string;
  id: string;
  name: string;
  rpcEndpoint: string;
  rps: number;
}

interface StatsResponse {
  networks: Array<{
    blockHeight: number;
    name: string;
    processed: number;
    synced: number;
  }>;
}

function normalizeBaseUrl(value: string): string {
  return value.endsWith('/') ? value : `${value}/`;
}

function buildUrl(baseUrl: string, path: string): URL {
  return new URL(path.replace(/^\/+/u, ''), normalizeBaseUrl(baseUrl));
}

function buildHeaders(apiToken?: string, includeJson = false): Record<string, string> {
  return {
    ...(includeJson ? { 'content-type': 'application/json' } : {}),
    ...(apiToken ? { 'x-api-token': apiToken } : {}),
  };
}

async function readErrorMessage(response: Response): Promise<string> {
  try {
    const payload = (await response.json()) as { error?: string };
    if (typeof payload.error === 'string' && payload.error) {
      return payload.error;
    }
  } catch {}

  const text = await response.text().catch(() => '');
  return text || `${response.status} ${response.statusText}`;
}

async function assertHealthy(baseUrl: string): Promise<void> {
  const response = await fetch(buildUrl(baseUrl, '/v1/heartbeat/'));
  if (response.status !== 204) {
    throw new Error(`heartbeat failed: ${await readErrorMessage(response)}`);
  }
}

async function createBootstrapKey(baseUrl: string): Promise<CreatedApiKey> {
  const response = await fetch(buildUrl(baseUrl, '/v1/keys/'), {
    method: 'POST',
    headers: buildHeaders(undefined, true),
    body: JSON.stringify({}),
  });

  if (response.status === 401) {
    throw new Error('API keys already exist. Re-run with --api-token <token>.');
  }

  if (!response.ok) {
    throw new Error(`could not create API key: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as CreatedApiKey;
}

async function createDogecoinNetwork(
  baseUrl: string,
  apiToken: string,
  input: {
    blockTime: number;
    chainId: number;
    name: string;
    rpcEndpoint: string;
    rps: number;
  },
): Promise<NetworkResponse> {
  const response = await fetch(buildUrl(baseUrl, '/v1/networks/'), {
    method: 'POST',
    headers: buildHeaders(apiToken, true),
    body: JSON.stringify({
      name: input.name,
      architecture: 'dogecoin',
      chainId: input.chainId,
      blockTime: input.blockTime,
      rpcEndpoint: input.rpcEndpoint,
      rps: input.rps,
    }),
  });

  if (!response.ok) {
    throw new Error(`could not create Dogecoin network: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as NetworkResponse;
}

async function getStats(baseUrl: string, apiToken: string): Promise<StatsResponse> {
  const response = await fetch(buildUrl(baseUrl, '/v1/stats/'), {
    headers: buildHeaders(apiToken),
  });

  if (!response.ok) {
    throw new Error(`could not read stats: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as StatsResponse;
}

function printCurlRunsheet(input: {
  apiToken: string;
  baseUrl: string;
  blockTime: number;
  chainId: number;
  name: string;
  rpcEndpoint: string;
  rps: number;
}): void {
  const payload = JSON.stringify({
    name: input.name,
    architecture: 'dogecoin',
    chainId: input.chainId,
    blockTime: input.blockTime,
    rpcEndpoint: input.rpcEndpoint,
    rps: input.rps,
  });

  console.log('');
  console.log('Equivalent curl commands:');
  console.log(
    `curl -X POST '${new URL('/v1/networks/', normalizeBaseUrl(input.baseUrl)).toString()}' \\`,
  );
  console.log(`  -H 'x-api-token: ${input.apiToken}' \\`);
  console.log("  -H 'content-type: application/json' \\");
  console.log(`  -d '${payload}'`);
  console.log('');
  console.log(`curl '${new URL('/v1/stats/', normalizeBaseUrl(input.baseUrl)).toString()}' \\`);
  console.log(`  -H 'x-api-token: ${input.apiToken}'`);
}

async function main(): Promise<void> {
  const program = new Command();

  program
    .name('dogecoin-runsheet')
    .description(
      'Bootstrap OnlyDoge for Dogecoin indexing by creating a key if needed and adding a Dogecoin network.',
    )
    .requiredOption('--rpc-endpoint <url>', 'Dogecoin JSON-RPC endpoint')
    .option('--base-url <url>', 'OnlyDoge API base URL', 'http://127.0.0.1:2277')
    .option('--api-token <token>', 'Existing OnlyDoge API token')
    .option('--name <name>', 'Network display name', 'Dogecoin Mainnet')
    .option('--chain-id <number>', 'Chain ID', '0')
    .option('--block-time <seconds>', 'Block time in seconds', '60')
    .option('--rps <number>', 'Per-network requests per second cap', '25');

  program.parse();
  const options = program.opts<{
    apiToken?: string;
    baseUrl: string;
    blockTime: string;
    chainId: string;
    name: string;
    rpcEndpoint: string;
    rps: string;
  }>();

  const chainId = Number.parseInt(options.chainId, 10);
  const blockTime = Number.parseInt(options.blockTime, 10);
  const rps = Number.parseInt(options.rps, 10);

  if (!Number.isInteger(chainId) || chainId < 0) {
    throw new Error(`invalid --chain-id: ${options.chainId}`);
  }
  if (!Number.isInteger(blockTime) || blockTime <= 0) {
    throw new Error(`invalid --block-time: ${options.blockTime}`);
  }
  if (!Number.isInteger(rps) || rps <= 0) {
    throw new Error(`invalid --rps: ${options.rps}`);
  }

  await assertHealthy(options.baseUrl);

  let apiToken = options.apiToken?.trim();
  let createdKey: CreatedApiKey | null = null;
  if (!apiToken) {
    createdKey = await createBootstrapKey(options.baseUrl);
    apiToken = createdKey.key;
  }

  const network = await createDogecoinNetwork(options.baseUrl, apiToken, {
    name: options.name,
    chainId,
    blockTime,
    rpcEndpoint: options.rpcEndpoint,
    rps,
  });
  const stats = await getStats(options.baseUrl, apiToken);

  console.log('Dogecoin indexing runsheet completed.');
  console.log(`Base URL: ${normalizeBaseUrl(options.baseUrl)}`);
  if (createdKey) {
    console.log(`Created API key: ${createdKey.id}`);
    console.log(`x-api-token: ${createdKey.key}`);
  } else {
    console.log('Used existing API token from --api-token.');
  }
  console.log(`Registered network: ${network.id} (${network.name})`);
  console.log(`Masked RPC endpoint: ${network.rpcEndpoint}`);
  console.log(
    'Indexing starts automatically when OnlyDoge is running in --mode=both or --mode=indexer.',
  );
  console.log('');
  console.log('Current stats:');
  console.log(JSON.stringify(stats, null, 2));

  printCurlRunsheet({
    apiToken,
    baseUrl: options.baseUrl,
    name: options.name,
    chainId,
    blockTime,
    rpcEndpoint: options.rpcEndpoint,
    rps,
  });
}

void main().catch((error) => {
  console.error(`[dogecoin-runsheet] ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
