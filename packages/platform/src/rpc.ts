import type { BlockchainRpcPort } from '@onlydoge/indexing-pipeline';

import type { NetworkRpcGateway } from '@onlydoge/network-catalog';
import { type ChainFamily, InfrastructureError } from '@onlydoge/shared-kernel';

export class HttpBlockchainRpcGateway implements NetworkRpcGateway, BlockchainRpcPort {
  private readonly rateLimitQueues = new Map<string, Promise<void>>();
  private readonly rateLimitState = new Map<string, number>();

  public constructor(private readonly timeoutMs = 10_000) {}

  public async assertHealthy(architecture: ChainFamily, rpcEndpoint: string): Promise<void> {
    await this.getBlockHeight({ architecture, rpcEndpoint, rps: Number.MAX_SAFE_INTEGER });
  }

  public async getBlockHeight(network: {
    architecture: ChainFamily;
    rpcEndpoint: string;
    rps: number;
  }): Promise<number> {
    if (network.architecture === 'evm') {
      const result = await this.callEvm<string>(
        network.rpcEndpoint,
        network.rps,
        'eth_blockNumber',
        [],
      );
      return Number.parseInt(result, 16);
    }

    return this.callDogecoin<number>(network.rpcEndpoint, network.rps, 'getblockcount', []);
  }

  public async getBlockSnapshot(
    network: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    blockHeight: number,
  ): Promise<Record<string, unknown>> {
    if (network.architecture === 'evm') {
      const hexHeight = `0x${blockHeight.toString(16)}`;
      const block = await this.callEvm<Record<string, unknown>>(
        network.rpcEndpoint,
        network.rps,
        'eth_getBlockByNumber',
        [hexHeight, true],
      );
      const transactions = Array.isArray(block.transactions)
        ? block.transactions.filter((transaction): transaction is { hash: string } =>
            Boolean(
              transaction &&
                typeof transaction === 'object' &&
                'hash' in transaction &&
                typeof transaction.hash === 'string',
            ),
          )
        : [];
      const receipts = await Promise.all(
        transactions.map((transaction) =>
          this.callEvm<Record<string, unknown>>(
            network.rpcEndpoint,
            network.rps,
            'eth_getTransactionReceipt',
            [transaction.hash],
          ),
        ),
      );

      return { block, receipts };
    }

    const hash = await this.callDogecoin<string>(network.rpcEndpoint, network.rps, 'getblockhash', [
      blockHeight,
    ]);
    const block = await this.callDogecoin<Record<string, unknown>>(
      network.rpcEndpoint,
      network.rps,
      'getblock',
      [hash, 2],
    );

    return { block };
  }

  private async callDogecoin<T>(
    rpcEndpoint: string,
    rps: number,
    method: string,
    params: unknown[],
  ): Promise<T> {
    try {
      const request = this.toRpcRequest(rpcEndpoint);
      await this.waitForRateLimit(request.url, rps);
      const response = await fetch(request.url, {
        method: 'POST',
        headers: request.headers,
        signal: AbortSignal.timeout(this.timeoutMs),
        body: JSON.stringify({
          jsonrpc: '1.0',
          id: 'onlydoge',
          method,
          params,
        }),
      });
      const payload: {
        error?: unknown;
        result?: T;
      } | null = await response.json().catch(() => null);

      if (!response.ok || !payload || payload.error || payload.result === undefined) {
        throw new InfrastructureError(`could not connect to \`${rpcEndpoint}\``);
      }

      return payload.result;
    } catch (error) {
      throw this.toInfrastructureError(rpcEndpoint, error);
    }
  }

  private async callEvm<T>(
    rpcEndpoint: string,
    rps: number,
    method: string,
    params: unknown[],
  ): Promise<T> {
    try {
      const request = this.toRpcRequest(rpcEndpoint);
      await this.waitForRateLimit(request.url, rps);
      const response = await fetch(request.url, {
        method: 'POST',
        headers: request.headers,
        signal: AbortSignal.timeout(this.timeoutMs),
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'onlydoge',
          method,
          params,
        }),
      });
      const payload: {
        error?: unknown;
        result?: T;
      } | null = await response.json().catch(() => null);

      if (!response.ok || !payload || payload.error || payload.result === undefined) {
        throw new InfrastructureError(`could not connect to \`${rpcEndpoint}\``);
      }

      return payload.result;
    } catch (error) {
      throw this.toInfrastructureError(rpcEndpoint, error);
    }
  }

  private toInfrastructureError(rpcEndpoint: string, error: unknown): InfrastructureError {
    if (error instanceof InfrastructureError) {
      return error;
    }

    return new InfrastructureError(`could not connect to \`${rpcEndpoint}\``, {
      ...(error instanceof Error ? { cause: error } : {}),
    });
  }

  private async waitForRateLimit(rpcEndpoint: string, rps: number): Promise<void> {
    if (!Number.isFinite(rps) || rps <= 0) {
      return;
    }

    const intervalMs = 1000 / rps;
    const previous = this.rateLimitQueues.get(rpcEndpoint) ?? Promise.resolve();
    let releaseCurrent: Promise<void> | null = null;
    releaseCurrent = previous.then(async () => {
      const now = Date.now();
      const nextAvailableAt = this.rateLimitState.get(rpcEndpoint) ?? now;
      const scheduledAt = Math.max(now, nextAvailableAt);
      this.rateLimitState.set(rpcEndpoint, scheduledAt + intervalMs);
      const delayMs = scheduledAt - now;
      if (delayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    });

    this.rateLimitQueues.set(rpcEndpoint, releaseCurrent);
    await releaseCurrent;
    if (this.rateLimitQueues.get(rpcEndpoint) === releaseCurrent) {
      this.rateLimitQueues.delete(rpcEndpoint);
    }
  }

  private toRpcRequest(rpcEndpoint: string): {
    headers: Record<string, string>;
    url: string;
  } {
    const url = new URL(rpcEndpoint);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
    };

    if (url.username || url.password) {
      const username = decodeURIComponent(url.username);
      const password = decodeURIComponent(url.password);
      headers.authorization = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
      url.username = '';
      url.password = '';
    }

    return {
      url: url.toString(),
      headers,
    };
  }
}
