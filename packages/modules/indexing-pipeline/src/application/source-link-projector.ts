import type { ProjectionStateStorePort } from '../contracts/ports';
import type { SourceLinkRecord, TrackedAddress } from '../domain/projection-models';

interface PathNode {
  firstSeenBlockHeight: number;
  hopCount: number;
  lastSeenBlockHeight: number;
  pathAddresses: string[];
  pathTransferCount: number;
  toAddress: string;
}

export class SourceLinkProjector {
  public constructor(private readonly warehouse: ProjectionStateStorePort) {}

  public async rebuild(
    networkId: number,
    seed: TrackedAddress,
    input?: { frontierBatchSize?: number },
  ): Promise<void> {
    const frontierBatchSize = Math.max(1, input?.frontierBatchSize ?? 128);
    const visited = new Set<string>([seed.address]);
    const queue: PathNode[] = [
      {
        toAddress: seed.address,
        hopCount: 0,
        pathTransferCount: 0,
        pathAddresses: [seed.address],
        firstSeenBlockHeight: Number.MAX_SAFE_INTEGER,
        lastSeenBlockHeight: 0,
      },
    ];
    const rows: SourceLinkRecord[] = [];

    while (queue.length > 0) {
      const frontier = queue.splice(0, frontierBatchSize);
      const frontierAddresses = frontier.map((node) => node.toAddress);
      const edges = await this.warehouse.listDirectLinksFromAddresses(networkId, frontierAddresses);
      const edgesByFromAddress = new Map<string, typeof edges>();

      for (const edge of edges) {
        const current = edgesByFromAddress.get(edge.fromAddress) ?? [];
        current.push(edge);
        edgesByFromAddress.set(edge.fromAddress, current);
      }

      for (const current of frontier) {
        const nextEdges = edgesByFromAddress.get(current.toAddress) ?? [];
        nextEdges.sort((left, right) => left.toAddress.localeCompare(right.toAddress));

        for (const edge of nextEdges) {
          if (visited.has(edge.toAddress)) {
            continue;
          }

          visited.add(edge.toAddress);
          const nextPathAddresses = [...current.pathAddresses, edge.toAddress];
          const firstSeenBlockHeight =
            current.hopCount === 0
              ? edge.firstSeenBlockHeight
              : Math.min(current.firstSeenBlockHeight, edge.firstSeenBlockHeight);
          const lastSeenBlockHeight = Math.max(
            current.lastSeenBlockHeight,
            edge.lastSeenBlockHeight,
          );
          const pathTransferCount = current.pathTransferCount + edge.transferCount;
          const hopCount = current.hopCount + 1;
          queue.push({
            toAddress: edge.toAddress,
            hopCount,
            pathTransferCount,
            pathAddresses: nextPathAddresses,
            firstSeenBlockHeight,
            lastSeenBlockHeight,
          });
          rows.push({
            networkId,
            sourceAddressId: seed.addressId,
            sourceAddress: seed.address,
            toAddress: edge.toAddress,
            hopCount,
            pathTransferCount,
            pathAddresses: nextPathAddresses,
            firstSeenBlockHeight,
            lastSeenBlockHeight,
          });
        }
      }
    }

    await this.warehouse.replaceSourceLinks(networkId, seed.addressId, rows);
  }
}
