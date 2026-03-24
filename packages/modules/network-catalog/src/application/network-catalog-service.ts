import {
  ConflictError,
  NotFoundError,
  type PrimaryId,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type {
  NetworkRepository,
  NetworkRpcGateway,
  TokenRepository,
} from '../contracts/repositories';
import { type CreateNetworkInput, Network } from '../domain/network';
import { type CreateTokenInput, Token } from '../domain/token';

export class NetworkCatalogService {
  public constructor(
    private readonly networks: NetworkRepository,
    private readonly tokens: TokenRepository,
    private readonly rpc: NetworkRpcGateway,
    private readonly lifecycle?: {
      markNetworksUpdated(): Promise<void>;
      softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void>;
    },
  ) {}

  public async createNetwork(input: CreateNetworkInput) {
    if (input.id && (await this.networks.getNetworkById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    const deletedName = await this.networks.getNetworkByName(input.name, true);
    if (deletedName?.isDeleted) {
      throw new TooEarlyError(`too early: network hasn't been deleted yet: ${input.name}`);
    }

    const duplicateName = await this.networks.getNetworkByName(input.name);
    if (duplicateName) {
      throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
    }

    const chainId = input.chainId ?? 0;
    const duplicateNetwork = await this.networks.getNetworkByArchitectureAndChainId(
      input.architecture,
      chainId,
    );
    if (duplicateNetwork) {
      throw new ConflictError(`duplicate found at \`chainId\`: ${chainId}`);
    }

    await this.rpc.assertHealthy(input.architecture, input.rpcEndpoint);

    const created = await this.networks.createNetwork(Network.create(input).record);
    await this.lifecycle?.markNetworksUpdated();
    return Network.rehydrate(created).toResponse();
  }

  public async listNetworks(
    offset?: number,
    limit?: number,
  ): Promise<{
    networks: ReturnType<Network['toResponse']>[];
  }> {
    return {
      networks: (await this.networks.listNetworks(offset, limit))
        .filter((network) => !network.isDeleted)
        .map((network) => Network.rehydrate(network).toResponse()),
    };
  }

  public async getNetwork(id: string): Promise<{ network: ReturnType<Network['toResponse']> }> {
    const network = await this.networks.getNetworkById(id);
    if (!network || network.isDeleted) {
      throw new NotFoundError();
    }

    return {
      network: Network.rehydrate(network).toResponse(),
    };
  }

  public async updateNetwork(id: string, input: Partial<CreateNetworkInput>): Promise<void> {
    const network = await this.networks.getNetworkById(id);
    if (!network || network.isDeleted) {
      throw new NotFoundError();
    }

    if (input.name) {
      const deletedName = await this.networks.getNetworkByName(input.name, true);
      if (deletedName?.isDeleted) {
        throw new TooEarlyError(`too early: network hasn't been deleted yet: ${input.name}`);
      }
      const duplicateName = await this.networks.getNetworkByName(input.name);
      if (duplicateName && duplicateName.id !== network.id) {
        throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
      }
    }

    if (input.chainId !== undefined) {
      const duplicateNetwork = await this.networks.getNetworkByArchitectureAndChainId(
        input.architecture ?? network.architecture,
        input.chainId,
      );
      if (duplicateNetwork && duplicateNetwork.id !== network.id) {
        throw new ConflictError(`duplicate found at \`chainId\`: ${input.chainId}`);
      }
    }

    if (input.rpcEndpoint) {
      await this.rpc.assertHealthy(input.architecture ?? network.architecture, input.rpcEndpoint);
    }

    const updated = Network.rehydrate(network).update(input);
    await this.networks.updateNetworkRecord(updated);
    await this.lifecycle?.markNetworksUpdated();
  }

  public async deleteNetworks(ids: string[]): Promise<void> {
    const deleted = await this.networks.softDeleteNetworks(ids);
    await this.lifecycle?.softDeleteAddressesByNetworkIds(
      deleted.map((network) => network.networkId),
    );
    await this.lifecycle?.markNetworksUpdated();
  }

  public async createToken(input: Omit<CreateTokenInput, 'networkId'> & { network: string }) {
    if (input.id && (await this.tokens.getTokenById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    const network = await this.networks.getNetworkById(input.network);
    if (!network || network.isDeleted) {
      throw new ValidationError(`invalid parameter for \`network\`: ${input.network}`);
    }

    const address = input.address?.trim() ?? '';
    const duplicate = await this.tokens.getTokenByNetworkAndAddress(network.networkId, address);
    if (duplicate) {
      throw new ConflictError(`duplicate found at \`address\`: ${address}`);
    }

    const created = await this.tokens.createToken(
      Token.create({
        ...input,
        address,
        networkId: network.networkId,
      }).record,
    );

    return Token.rehydrate(created).toResponse();
  }

  public async listTokens(
    offset?: number,
    limit?: number,
  ): Promise<{
    networks: ReturnType<Network['toResponse']>[];
    tokens: ReturnType<Token['toResponse']>[];
  }> {
    const tokens = await this.tokens.listTokens(offset, limit);
    const networkIds = [...new Set(tokens.map((token) => token.networkId))];
    const networks = (
      await Promise.all(
        networkIds.map((networkId) => this.networks.getNetworkByInternalId(networkId)),
      )
    )
      .filter((network): network is NonNullable<typeof network> => Boolean(network))
      .map((network) => Network.rehydrate(network).toResponse());

    return {
      tokens: tokens.map((token) => Token.rehydrate(token).toResponse()),
      networks,
    };
  }

  public async getToken(id: string): Promise<{
    token: ReturnType<Token['toResponse']>;
    networks: ReturnType<Network['toResponse']>[];
  }> {
    const token = await this.tokens.getTokenById(id);
    if (!token) {
      throw new NotFoundError();
    }

    const network = await this.networks.getNetworkByInternalId(token.networkId);
    return {
      token: Token.rehydrate(token).toResponse(),
      networks: network ? [Network.rehydrate(network).toResponse()] : [],
    };
  }

  public async deleteTokens(ids: string[]): Promise<void> {
    await this.tokens.deleteTokens(ids);
  }

  public async getActiveNetworkById(id: string) {
    const network = await this.networks.getNetworkById(id);
    if (!network || network.isDeleted) {
      return null;
    }

    return network;
  }

  public async getActiveNetworksByInternalIds(networkIds: PrimaryId[]) {
    return (
      await Promise.all(
        networkIds.map((networkId) => this.networks.getNetworkByInternalId(networkId)),
      )
    ).filter((network): network is NonNullable<typeof network> =>
      Boolean(network && !network.isDeleted),
    );
  }
}
