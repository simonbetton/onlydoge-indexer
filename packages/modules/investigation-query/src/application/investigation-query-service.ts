import {
  type PrimaryId,
  type RiskLevel,
  type RiskReason,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type {
  ConfigReader,
  InvestigationMetadataPort,
  InvestigationWarehousePort,
} from '../contracts/ports';
import type { InfoResponse } from '../domain/query-models';

export class InvestigationQueryService {
  public constructor(
    private readonly metadata: InvestigationMetadataPort,
    private readonly warehouse: InvestigationWarehousePort,
    private readonly configs: ConfigReader,
  ) {}

  public async heartbeat(): Promise<void> {}

  public async stats(): Promise<{
    networks: Array<{
      blockHeight: number;
      name: string;
      processed: number;
      synced: number;
    }>;
  }> {
    const networks = await this.metadata.listActiveNetworks();
    const response = await Promise.all(
      networks.map(async (network) => ({
        name: network.name,
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
      })),
    );

    return { networks: response };
  }

  public async info(query: string | undefined): Promise<InfoResponse> {
    const q = query?.trim() ?? '';
    if (!q) {
      throw new ValidationError('missing input params');
    }

    const entity = await this.metadata.getEntityById(q);
    const addresses = entity
      ? (await this.metadata.listAddressesByEntityIds([entity.entityId])).map(
          (address) => address.address,
        )
      : [q];

    const [balances, links] = await Promise.all([
      this.warehouse.getBalancesByAddresses(addresses),
      this.warehouse.getDistinctLinksByAddresses(addresses),
    ]);
    const addressRecords = await this.metadata.listAddressesByValues([
      ...new Set([...addresses, ...links.map((link) => link.fromAddress)]),
    ]);
    const entityIds = [...new Set(addressRecords.map((address) => address.entityId))];
    const [entities, joinedTags, networks] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
      this.metadata.listNetworksByInternalIds(addressRecords.map((address) => address.networkId)),
    ]);

    const reasons = new Set<RiskReason>();
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

    const tokens = await this.warehouse.getTokensByAddresses(
      balances.map((balance) => balance.assetAddress).filter(Boolean),
    );
    const tokenByNetworkAndAddress = new Map(
      tokens.map((token) => [`${token.networkId}:${token.address}`, token]),
    );
    const networkById = new Map(networks.map((network) => [network.networkId, network]));
    const entityIdsByAddress = new Map(
      addressRecords.map((address) => [address.address, address.entityId]),
    );
    const tagIdsByEntityId = new Map<PrimaryId, string[]>();
    for (const tag of joinedTags) {
      const current = tagIdsByEntityId.get(tag.entityId) ?? [];
      current.push(tag.id);
      tagIdsByEntityId.set(tag.entityId, current);
    }

    return {
      addresses,
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
}
