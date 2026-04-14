import {
  formatAmountBase,
  type ProjectionUtxoOutput,
  type ProjectionWarehousePort,
  parseAmountBase,
} from '@onlydoge/indexing-pipeline';
import type { InfoResponse, InvestigationWarehousePort } from '@onlydoge/investigation-query';
import {
  NotFoundError,
  type PrimaryId,
  type RiskLevel,
  ValidationError,
} from '@onlydoge/shared-kernel';

import type {
  ExplorerActiveNetworkPort,
  ExplorerConfigPort,
  ExplorerLabelRef,
  ExplorerMetadataPort,
  ExplorerRawBlockPort,
  ExplorerWarehousePort,
} from '../contracts/ports';
import type {
  ExplorerAddressDetail,
  ExplorerAddressTransactionSummary,
  ExplorerAddressUtxo,
  ExplorerBlockSummary,
  ExplorerNetworkSummary,
  ExplorerSearchResult,
  ExplorerTransactionDetail,
  ExplorerTransactionInput,
  ExplorerTransactionOutput,
  ExplorerTransactionSummary,
} from '../domain/query-models';

interface DogecoinVin {
  coinbase?: string;
  txid?: string;
  vout?: number;
}

interface DogecoinVout {
  n?: number;
  value?: number | string;
  scriptPubKey?: {
    address?: string;
    addresses?: string[];
    type?: string;
  };
}

interface DogecoinTransaction {
  txid?: string;
  vin?: DogecoinVin[];
  vout?: DogecoinVout[];
}

interface ParsedDogecoinBlock {
  hash: string;
  height: number;
  time: number;
  tx: DogecoinTransaction[];
}

type ExplorerWarehouse = ExplorerWarehousePort &
  InvestigationWarehousePort &
  Pick<ProjectionWarehousePort, 'getUtxoOutputs'>;

export class ExplorerQueryService {
  public constructor(
    private readonly networks: ExplorerActiveNetworkPort,
    private readonly metadata: ExplorerMetadataPort,
    private readonly warehouse: ExplorerWarehouse,
    private readonly rawBlocks: ExplorerRawBlockPort,
    private readonly configs: ExplorerConfigPort,
  ) {}

  public async listNetworks(): Promise<{
    networks: ExplorerNetworkSummary[];
  }> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );
    const isSingleNetwork = activeNetworks.length === 1;

    return {
      networks: await Promise.all(
        activeNetworks.map(async (network) => ({
          id: network.id,
          name: network.name,
          chainId: network.chainId,
          blockTime: network.blockTime,
          blockHeight:
            (await this.configs.getJsonValue<number>(`block_height_n${network.networkId}`)) ?? 0,
          synced:
            (await this.configs.getJsonValue<number>(
              `indexer_sync_progress_n${network.networkId}`,
            )) ?? 0,
          processed:
            (await this.configs.getJsonValue<number>(
              `indexer_process_progress_n${network.networkId}`,
            )) ?? 0,
          isDefault: isSingleNetwork,
        })),
      ),
    };
  }

  public async search(
    query: string | undefined,
    networkId?: string,
  ): Promise<{
    matches: ExplorerSearchResult[];
  }> {
    const q = query?.trim() ?? '';
    if (!q) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const matches: ExplorerSearchResult[] = [];

    if (/^\d+$/u.test(q)) {
      const block = await this.getBlockByHeight(network.networkId, network.id, Number(q));
      if (block) {
        matches.push({
          type: 'block',
          network: network.id,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
        });
      }

      return { matches };
    }

    const txRef = await this.warehouse.getTransactionRef(network.networkId, q);
    if (txRef) {
      matches.push({
        type: 'transaction',
        network: network.id,
        txid: q,
        blockHeight: txRef.blockHeight,
        blockHash: txRef.blockHash,
        blockTime: txRef.blockTime,
      });
    }

    const blockRef = await this.warehouse.getAppliedBlockByHash(network.networkId, q);
    if (blockRef) {
      const block = await this.getBlockByHeight(
        network.networkId,
        network.id,
        blockRef.blockHeight,
      );
      if (block) {
        matches.push({
          type: 'block',
          network: network.id,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
        });
      }
    }

    const [summary, labelMap] = await Promise.all([
      this.warehouse.getAddressSummary(network.networkId, q),
      this.buildLabelMap(network.networkId, [q]),
    ]);
    const label = labelMap.get(q);
    const hasAddressMatch = Boolean(summary) || Boolean(label);
    if (hasAddressMatch) {
      matches.push({
        type: 'address',
        network: network.id,
        address: q,
        ...(summary
          ? {
              balance: summary.balance,
              txCount: summary.txCount,
            }
          : {}),
        hasLabel: Boolean(label),
        ...(label ? { riskLevel: label.riskLevel } : {}),
      });
    }

    return { matches };
  }

  public async listBlocks(
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    blocks: ExplorerBlockSummary[];
  }> {
    const network = await this.resolveNetwork(networkId);
    const refs = await this.warehouse.listAppliedBlocks(network.networkId, offset, limit ?? 20);
    const blocks = await Promise.all(
      refs.map(async (ref) =>
        this.getBlockByHeight(network.networkId, network.id, ref.blockHeight),
      ),
    );

    return {
      blocks: blocks.filter((block): block is ExplorerBlockSummary => Boolean(block)),
    };
  }

  public async getBlock(
    ref: string,
    networkId?: string,
  ): Promise<{
    block: ExplorerBlockSummary;
    transactions: ExplorerTransactionSummary[];
  }> {
    const network = await this.resolveNetwork(networkId);
    const parsed = await this.resolveBlockSnapshot(network.networkId, ref);
    const inputMap = await this.loadResolvedInputs(network.networkId, parsed.tx);

    return {
      block: this.serializeBlock(network.id, parsed),
      transactions: parsed.tx.map((transaction, txIndex) =>
        this.serializeTransactionSummary(network.id, parsed, transaction, txIndex, inputMap),
      ),
    };
  }

  public async getTransaction(
    txid: string,
    networkId?: string,
  ): Promise<ExplorerTransactionDetail> {
    const network = await this.resolveNetwork(networkId);
    const txRef = await this.warehouse.getTransactionRef(network.networkId, txid.trim());
    if (!txRef) {
      throw new NotFoundError('transaction not found');
    }

    const block = await this.loadBlockSnapshot(network.networkId, txRef.blockHeight);
    const transaction = block.tx.find(
      (candidate) => this.readString(candidate.txid) === txid.trim(),
    );
    if (!transaction) {
      throw new NotFoundError('transaction not found');
    }

    const inputMap = await this.loadResolvedInputs(network.networkId, [transaction]);
    const summary = this.serializeTransactionSummary(
      network.id,
      block,
      transaction,
      txRef.txIndex,
      inputMap,
    );
    const outputs = this.readOutputs(transaction.vout);
    const outputKeys = outputs.map((_output, index) => `${txid.trim()}:${index}`);
    const currentOutputs = await this.warehouse.getUtxoOutputs(network.networkId, outputKeys);
    const addresses = new Set<string>();
    const inputs: ExplorerTransactionInput[] = this.readInputs(transaction.vin).flatMap((input) => {
      if (input.coinbase) {
        return [];
      }

      const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
      const resolved = inputMap.get(outputKey);
      if (!resolved || !resolved.address) {
        return [];
      }
      addresses.add(resolved.address);
      return [
        {
          address: resolved.address,
          outputKey,
          valueBase: resolved.valueBase,
        },
      ];
    });
    const serializedOutputs: ExplorerTransactionOutput[] = outputs.map((output, index) => {
      const outputKey = `${txid.trim()}:${index}`;
      const current = currentOutputs.get(outputKey);
      const address = this.extractOutputAddress(output);
      if (address) {
        addresses.add(address);
      }
      return {
        address,
        vout: this.requireNumber(output.n ?? index, 'vout.n'),
        outputKey,
        valueBase: this.requireAmountBase(output.value),
        scriptType: output.scriptPubKey?.type?.trim() ?? '',
        isSpendable: Boolean(address),
        spentByTxid: current?.spentByTxid ?? null,
        spentInBlock: current?.spentInBlock ?? null,
      };
    });

    const labelMap = await this.buildLabelMap(network.networkId, [...addresses]);
    for (const input of inputs) {
      const label = labelMap.get(input.address);
      if (label) {
        input.label = label;
      }
    }
    for (const output of serializedOutputs) {
      const label = labelMap.get(output.address);
      if (label) {
        output.label = label;
      }
    }

    const transfers = this.projectTransfers(summary, inputs, serializedOutputs);

    return {
      transaction: summary,
      inputs,
      outputs: serializedOutputs,
      transfers,
      overlay: {
        labels: [...new Map([...labelMap.values()].map((label) => [label.entity, label])).values()],
      },
    };
  }

  public async getAddress(address: string, networkId?: string): Promise<ExplorerAddressDetail> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const summary = await this.warehouse.getAddressSummary(network.networkId, normalizedAddress);
    const overlay = await this.buildAddressOverlay(network.networkId, normalizedAddress);

    const labeledRecord = (await this.metadata.listAddressesByValues([normalizedAddress])).find(
      (candidate) => candidate.networkId === network.networkId,
    );
    if (!summary && !labeledRecord) {
      throw new NotFoundError('address not found');
    }

    return {
      address: {
        network: network.id,
        address: normalizedAddress,
        balance: summary?.balance ?? '0',
        receivedBase: summary?.receivedBase ?? '0',
        sentBase: summary?.sentBase ?? '0',
        txCount: summary?.txCount ?? 0,
        utxoCount: summary?.utxoCount ?? 0,
      },
      overlay,
    };
  }

  public async listAddressTransactions(
    address: string,
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    transactions: ExplorerAddressTransactionSummary[];
  }> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const rows = await this.warehouse.listAddressTransactions(
      network.networkId,
      normalizedAddress,
      offset,
      limit ?? 50,
    );
    const snapshotsByHeight = await this.loadSnapshotsByHeight(network.networkId, [
      ...new Set(rows.map((row) => row.blockHeight)),
    ]);

    return {
      transactions: rows.flatMap((row) => {
        const block = snapshotsByHeight.get(row.blockHeight);
        if (!block) {
          return [];
        }
        const transaction = block.tx.find(
          (candidate) => this.readString(candidate.txid) === row.txid,
        );
        if (!transaction) {
          return [];
        }
        const summary = this.serializeTransactionSummary(
          network.id,
          block,
          transaction,
          row.txIndex,
        );
        return [
          {
            transaction: summary,
            receivedBase: row.receivedBase,
            sentBase: row.sentBase,
          },
        ];
      }),
    };
  }

  public async listAddressUtxos(
    address: string,
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    utxos: ExplorerAddressUtxo[];
  }> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const utxos = await this.warehouse.listAddressUtxos(
      network.networkId,
      normalizedAddress,
      offset,
      limit ?? 50,
    );

    return {
      utxos: utxos.map((utxo) => ({
        network: network.id,
        address: utxo.address,
        blockHash: utxo.blockHash,
        blockHeight: utxo.blockHeight,
        blockTime: utxo.blockTime,
        outputKey: utxo.outputKey,
        scriptType: utxo.scriptType,
        spentByTxid: utxo.spentByTxid,
        spentInBlock: utxo.spentInBlock,
        txid: utxo.txid,
        txIndex: utxo.txIndex,
        valueBase: utxo.valueBase,
        vout: utxo.vout,
      })),
    };
  }

  private async resolveNetwork(networkId?: string): Promise<{
    id: string;
    name: string;
    networkId: PrimaryId;
  }> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );
    if (activeNetworks.length === 0) {
      throw new NotFoundError('dogecoin network not found');
    }

    if (networkId) {
      const network = activeNetworks.find((candidate) => candidate.id === networkId);
      if (!network) {
        throw new ValidationError(`invalid parameter for \`network\`: ${networkId}`);
      }

      return {
        id: network.id,
        name: network.name,
        networkId: network.networkId,
      };
    }

    if (activeNetworks.length > 1) {
      throw new ValidationError('missing parameter for `network`');
    }

    const [network] = activeNetworks;
    if (!network) {
      throw new NotFoundError('dogecoin network not found');
    }

    return {
      id: network.id,
      name: network.name,
      networkId: network.networkId,
    };
  }

  private async resolveBlockSnapshot(
    networkId: PrimaryId,
    ref: string,
  ): Promise<ParsedDogecoinBlock> {
    const normalized = ref.trim();
    if (/^\d+$/u.test(normalized)) {
      return this.loadBlockSnapshot(networkId, Number(normalized));
    }

    const blockRef = await this.warehouse.getAppliedBlockByHash(networkId, normalized);
    if (!blockRef) {
      throw new NotFoundError('block not found');
    }

    return this.loadBlockSnapshot(networkId, blockRef.blockHeight);
  }

  private async getBlockByHeight(
    networkId: PrimaryId,
    externalNetworkId: string,
    blockHeight: number,
  ): Promise<ExplorerBlockSummary | null> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      networkId,
      blockHeight,
      'block',
    );
    if (!snapshot) {
      return null;
    }

    return this.serializeBlock(externalNetworkId, this.parseBlock(snapshot));
  }

  private async loadBlockSnapshot(
    networkId: PrimaryId,
    blockHeight: number,
  ): Promise<ParsedDogecoinBlock> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      networkId,
      blockHeight,
      'block',
    );
    if (!snapshot) {
      throw new NotFoundError('block not found');
    }

    return this.parseBlock(snapshot);
  }

  private async loadSnapshotsByHeight(
    networkId: PrimaryId,
    heights: number[],
  ): Promise<Map<number, ParsedDogecoinBlock>> {
    const snapshots = await Promise.all(
      heights.map(
        async (height) =>
          [
            height,
            await this.rawBlocks.getPart<Record<string, unknown>>(networkId, height, 'block'),
          ] as const,
      ),
    );

    return new Map(
      snapshots
        .filter((entry): entry is readonly [number, Record<string, unknown>] => Boolean(entry[1]))
        .map(([height, snapshot]) => [height, this.parseBlock(snapshot)]),
    );
  }

  private serializeBlock(network: string, block: ParsedDogecoinBlock): ExplorerBlockSummary {
    return {
      network,
      hash: block.hash,
      height: block.height,
      time: block.time,
      txCount: block.tx.length,
    };
  }

  private async loadResolvedInputs(
    networkId: PrimaryId,
    transactions: DogecoinTransaction[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const outputKeys = [
      ...new Set(
        transactions.flatMap((transaction) =>
          this.readInputs(transaction.vin)
            .filter((input) => !input.coinbase)
            .map(
              (input) =>
                `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`,
            ),
        ),
      ),
    ];

    return this.warehouse.getUtxoOutputs(networkId, outputKeys);
  }

  private serializeTransactionSummary(
    network: string,
    block: ParsedDogecoinBlock,
    transaction: DogecoinTransaction,
    txIndex: number,
    resolvedInputs?: Map<string, ProjectionUtxoOutput>,
  ): ExplorerTransactionSummary {
    const txid = this.requireString(transaction.txid, 'tx.txid');
    const inputs = this.readInputs(transaction.vin);
    const outputs = this.readOutputs(transaction.vout);
    const isCoinbase = inputs.some((input) => Boolean(input.coinbase));

    let totalInput = 0n;
    for (const input of inputs) {
      if (input.coinbase) {
        continue;
      }

      const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
      const resolved = resolvedInputs?.get(outputKey);
      if (resolved) {
        totalInput += parseAmountBase(resolved.valueBase);
      }
    }

    const totalOutput = outputs.reduce(
      (sum, output) => sum + parseAmountBase(this.requireAmountBase(output.value)),
      0n,
    );
    const feeBase =
      isCoinbase || totalInput === 0n ? null : formatAmountBase(totalInput - totalOutput);

    return {
      network,
      txid,
      txIndex,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      isCoinbase,
      inputCount: inputs.length,
      outputCount: outputs.length,
      totalInputBase: formatAmountBase(totalInput),
      totalOutputBase: formatAmountBase(totalOutput),
      feeBase,
    };
  }

  private projectTransfers(
    summary: ExplorerTransactionSummary,
    inputs: ExplorerTransactionInput[],
    outputs: ExplorerTransactionOutput[],
  ): Array<{
    amountBase: string;
    from: string;
    to: string;
  }> {
    if (summary.isCoinbase || inputs.length === 0 || outputs.length === 0) {
      return [];
    }

    const inputTotals = new Map<string, bigint>();
    for (const input of inputs) {
      const current = inputTotals.get(input.address) ?? 0n;
      inputTotals.set(input.address, current + parseAmountBase(input.valueBase));
    }

    const totalInput = [...inputTotals.values()].reduce((sum, value) => sum + value, 0n);
    const addresses = [...inputTotals.keys()].sort((left, right) => left.localeCompare(right));
    const transfers: Array<{
      amountBase: string;
      from: string;
      to: string;
    }> = [];

    for (const output of outputs) {
      if (!output.address) {
        continue;
      }

      const allocations = this.allocateOutputAmount(
        parseAmountBase(output.valueBase),
        addresses,
        inputTotals,
        totalInput,
      );
      for (const allocation of allocations) {
        if (allocation.amount <= 0n) {
          continue;
        }

        transfers.push({
          from: allocation.address,
          to: output.address,
          amountBase: formatAmountBase(allocation.amount),
        });
      }
    }

    return transfers;
  }

  private allocateOutputAmount(
    target: bigint,
    inputAddresses: string[],
    inputTotals: Map<string, bigint>,
    totalInput: bigint,
  ): Array<{ address: string; amount: bigint }> {
    if (totalInput <= 0n || inputAddresses.length === 0) {
      return [];
    }

    const allocations: Array<{ address: string; amount: bigint }> = [];
    let allocated = 0n;
    for (const [index, address] of inputAddresses.entries()) {
      if (index === inputAddresses.length - 1) {
        allocations.push({
          address,
          amount: target - allocated,
        });
        continue;
      }

      const contribution = inputTotals.get(address) ?? 0n;
      const amount = (target * contribution) / totalInput;
      allocations.push({ address, amount });
      allocated += amount;
    }

    return allocations;
  }

  private async buildLabelMap(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<Map<string, ExplorerLabelRef>> {
    const addressRecords = (
      await this.metadata.listAddressesByValues([...new Set(addresses)])
    ).filter((address) => address.networkId === networkId);
    if (addressRecords.length === 0) {
      return new Map();
    }

    const entityIds = [...new Set(addressRecords.map((address) => address.entityId))];
    const [entities, joinedTags] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
    ]);
    const entityById = new Map(entities.map((entity) => [entity.entityId, entity]));
    const tagsByEntityId = new Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>();
    for (const tag of joinedTags) {
      const current = tagsByEntityId.get(tag.entityId) ?? [];
      current.push({
        id: tag.id,
        riskLevel: tag.riskLevel,
      });
      tagsByEntityId.set(tag.entityId, current);
    }

    return new Map(
      addressRecords.flatMap((record) => {
        const entity = entityById.get(record.entityId);
        if (!entity) {
          return [];
        }
        const tags = tagsByEntityId.get(record.entityId) ?? [];
        return [
          [
            record.address,
            {
              entity: entity.id,
              name: entity.name,
              tags: tags.map((tag) => tag.id),
              riskLevel: tags.some((tag) => tag.riskLevel === 'high') ? 'high' : 'low',
            } satisfies ExplorerLabelRef,
          ] as const,
        ];
      }),
    );
  }

  private async buildAddressOverlay(networkId: PrimaryId, address: string): Promise<InfoResponse> {
    const balances = (await this.warehouse.getBalancesByAddresses([address])).filter(
      (balance) => balance.networkId === networkId,
    );
    const links = (await this.warehouse.getDistinctLinksByAddresses([address])).filter(
      (link) => link.networkId === networkId,
    );
    const addressRecords = (
      await this.metadata.listAddressesByValues([
        ...new Set([address, ...links.map((link) => link.fromAddress)]),
      ])
    ).filter((candidate) => candidate.networkId === networkId);
    const entityIds = [...new Set(addressRecords.map((candidate) => candidate.entityId))];
    const [entities, joinedTags, networks] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
      this.metadata.listNetworksByInternalIds(
        addressRecords.map((candidate) => candidate.networkId),
      ),
    ]);

    const reasons = new Set<'entity' | 'source'>();
    let riskLevel: RiskLevel = 'low';
    for (const tag of joinedTags) {
      reasons.add('entity');
      if (tag.riskLevel === 'high') {
        riskLevel = 'high';
      }
    }
    if (links.length > 0) {
      reasons.add('source');
    }

    const tokens = (
      await this.warehouse.getTokensByAddresses(
        balances.map((balance) => balance.assetAddress).filter(Boolean),
      )
    ).filter((token) => token.networkId === networkId);
    const tokenByNetworkAndAddress = new Map(
      tokens.map((token) => [`${token.networkId}:${token.address}`, token]),
    );
    const networkById = new Map(networks.map((network) => [network.networkId, network]));
    const entityIdsByAddress = new Map(
      addressRecords.map((record) => [record.address, record.entityId]),
    );
    const tagIdsByEntityId = new Map<PrimaryId, string[]>();
    for (const tag of joinedTags) {
      const current = tagIdsByEntityId.get(tag.entityId) ?? [];
      current.push(tag.id);
      tagIdsByEntityId.set(tag.entityId, current);
    }

    return {
      addresses: [address],
      risk: {
        level: riskLevel,
        reasons: [...reasons],
      },
      assets: balances.map((balance) => {
        const tokenId = tokenByNetworkAndAddress.get(
          `${balance.networkId}:${balance.assetAddress}`,
        )?.id;
        return {
          network: networkById.get(balance.networkId)?.id ?? '',
          balance: balance.balance,
          ...(tokenId ? { token: tokenId } : {}),
        };
      }),
      tokens: tokens.map((token) => ({
        id: token.id,
        name: token.name,
        symbol: token.symbol,
        address: token.address,
        decimals: token.decimals,
      })),
      sources: links.map((link) => ({
        network: networkById.get(link.networkId)?.id ?? '',
        entity:
          entities.find(
            (candidate) => candidate.entityId === entityIdsByAddress.get(link.fromAddress),
          )?.id ?? '',
        from: link.fromAddress,
        to: link.toAddress,
        hops: link.transferCount,
      })),
      networks: networks.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
      entities: entities.map((candidate) => ({
        id: candidate.id,
        name: candidate.name,
        description: candidate.description,
        data: candidate.data,
        tags: tagIdsByEntityId.get(candidate.entityId) ?? [],
      })),
      tags: joinedTags.map((tag) => ({
        id: tag.id,
        name: tag.name,
        riskLevel: tag.riskLevel,
      })),
    };
  }

  private parseBlock(snapshot: Record<string, unknown>): ParsedDogecoinBlock {
    const candidate = this.requireRecord(snapshot.block, 'block');
    return {
      hash: this.requireString(this.readString(candidate.hash), 'block.hash'),
      height: this.requireNumber(this.readNumber(candidate.height), 'block.height'),
      time: this.requireNumber(this.readNumber(candidate.time), 'block.time'),
      tx: Array.isArray(candidate.tx) ? candidate.tx.filter(isDogecoinTransaction) : [],
    };
  }

  private readInputs(value: DogecoinTransaction['vin']): DogecoinVin[] {
    return Array.isArray(value) ? value : [];
  }

  private readOutputs(value: DogecoinTransaction['vout']): DogecoinVout[] {
    return Array.isArray(value) ? value : [];
  }

  private extractOutputAddress(output: DogecoinVout): string {
    const direct = output.scriptPubKey?.address?.trim();
    if (direct) {
      return direct;
    }

    const first = output.scriptPubKey?.addresses?.find((value) => value.trim());
    return first?.trim() ?? '';
  }

  private requireAmountBase(value: number | string | undefined): string {
    if (value === undefined) {
      throw new ValidationError('missing output value');
    }

    if (typeof value === 'number') {
      const [whole, fraction = ''] = value.toFixed(8).split('.');
      return `${whole}${fraction.padEnd(8, '0')}`.replace(/^0+(?=\d)/u, '') || '0';
    }

    const [whole = '', fraction = ''] = value.trim().split('.');
    if (!/^\d+$/u.test(whole) || !/^\d*$/u.test(fraction)) {
      throw new ValidationError(`invalid decimal amount: ${value}`);
    }

    return `${whole}${fraction.padEnd(8, '0').slice(0, 8)}`.replace(/^0+(?=\d)/u, '') || '0';
  }

  private requireNumber(value: number | undefined, field: string): number {
    if (value === undefined || !Number.isInteger(value) || value < 0) {
      throw new ValidationError(`invalid parameter for \`${field}\`: ${value ?? ''}`);
    }

    return value;
  }

  private requireRecord(value: unknown, field: string): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new ValidationError(`invalid parameter for \`${field}\`: `);
    }

    return Object.fromEntries(Object.entries(value));
  }

  private requireString(value: string | undefined, field: string): string {
    const trimmed = value?.trim();
    if (!trimmed) {
      throw new ValidationError(`invalid parameter for \`${field}\`: `);
    }

    return trimmed;
  }

  private readNumber(value: unknown): number | undefined {
    return typeof value === 'number' ? value : undefined;
  }

  private readString(value: unknown): string | undefined {
    return typeof value === 'string' ? value : undefined;
  }
}

function isDogecoinTransaction(value: unknown): value is DogecoinTransaction {
  return typeof value === 'object' && value !== null;
}
