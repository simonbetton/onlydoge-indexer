import type { ProjectionStateStorePort } from '../contracts/ports';
import type {
  DirectLinkRecord,
  SourceLinkRecord,
  TrackedAddress,
} from '../domain/projection-models';

interface PathNode {
  firstSeenBlockHeight: number;
  hopCount: number;
  lastSeenBlockHeight: number;
  pathAddresses: string[];
  pathTransferCount: number;
  toAddress: string;
}

interface TraversalState {
  queue: PathNode[];
  rows: SourceLinkRecord[];
  visited: Set<string>;
}

export class SourceLinkProjector {
  public constructor(private readonly warehouse: ProjectionStateStorePort) {}

  public async rebuild(
    networkId: number,
    seed: TrackedAddress,
    input?: { frontierBatchSize?: number },
  ): Promise<void> {
    const frontierBatchSize = Math.max(1, input?.frontierBatchSize ?? 128);
    const traversal = createTraversalState(seed);

    while (traversal.queue.length > 0) {
      const frontier = traversal.queue.splice(0, frontierBatchSize);
      const frontierAddresses = frontier.map((node) => node.toAddress);
      const edges = await this.warehouse.listDirectLinksFromAddresses(networkId, frontierAddresses);
      this.expandFrontier(networkId, seed, frontier, groupEdgesByFromAddress(edges), traversal);
    }

    await this.warehouse.replaceSourceLinks(networkId, seed.addressId, traversal.rows);
  }

  private expandFrontier(
    networkId: number,
    seed: TrackedAddress,
    frontier: PathNode[],
    edgesByFromAddress: Map<string, DirectLinkRecord[]>,
    traversal: TraversalState,
  ): void {
    for (const current of frontier) {
      this.expandNode(networkId, seed, current, edgesByFromAddress, traversal);
    }
  }

  private expandNode(
    networkId: number,
    seed: TrackedAddress,
    current: PathNode,
    edgesByFromAddress: Map<string, DirectLinkRecord[]>,
    traversal: TraversalState,
  ): void {
    const nextEdges = sortedEdges(edgesByFromAddress.get(current.toAddress));
    for (const edge of nextEdges) {
      this.appendReachableEdge(networkId, seed, current, edge, traversal);
    }
  }

  private appendReachableEdge(
    networkId: number,
    seed: TrackedAddress,
    current: PathNode,
    edge: DirectLinkRecord,
    traversal: TraversalState,
  ): void {
    if (traversal.visited.has(edge.toAddress)) {
      return;
    }

    traversal.visited.add(edge.toAddress);
    const nextNode = nextPathNode(current, edge);
    traversal.queue.push(nextNode);
    traversal.rows.push(sourceLinkRow(networkId, seed, nextNode));
  }
}

function createTraversalState(seed: TrackedAddress): TraversalState {
  return {
    visited: new Set<string>([seed.address]),
    queue: [
      {
        toAddress: seed.address,
        hopCount: 0,
        pathTransferCount: 0,
        pathAddresses: [seed.address],
        firstSeenBlockHeight: Number.MAX_SAFE_INTEGER,
        lastSeenBlockHeight: 0,
      },
    ],
    rows: [],
  };
}

function groupEdgesByFromAddress(edges: DirectLinkRecord[]): Map<string, DirectLinkRecord[]> {
  const edgesByFromAddress = new Map<string, DirectLinkRecord[]>();
  for (const edge of edges) {
    const current = edgesByFromAddress.get(edge.fromAddress) ?? [];
    current.push(edge);
    edgesByFromAddress.set(edge.fromAddress, current);
  }

  return edgesByFromAddress;
}

function sortedEdges(edges: DirectLinkRecord[] | undefined): DirectLinkRecord[] {
  return [...(edges ?? [])].sort((left, right) => left.toAddress.localeCompare(right.toAddress));
}

function nextPathNode(current: PathNode, edge: DirectLinkRecord): PathNode {
  const hopCount = current.hopCount + 1;

  return {
    toAddress: edge.toAddress,
    hopCount,
    pathTransferCount: current.pathTransferCount + edge.transferCount,
    pathAddresses: [...current.pathAddresses, edge.toAddress],
    firstSeenBlockHeight: firstSeenHeight(current, edge),
    lastSeenBlockHeight: Math.max(current.lastSeenBlockHeight, edge.lastSeenBlockHeight),
  };
}

function firstSeenHeight(current: PathNode, edge: DirectLinkRecord): number {
  return current.hopCount === 0
    ? edge.firstSeenBlockHeight
    : Math.min(current.firstSeenBlockHeight, edge.firstSeenBlockHeight);
}

function sourceLinkRow(networkId: number, seed: TrackedAddress, node: PathNode): SourceLinkRecord {
  return {
    networkId,
    sourceAddressId: seed.addressId,
    sourceAddress: seed.address,
    toAddress: node.toAddress,
    hopCount: node.hopCount,
    pathTransferCount: node.pathTransferCount,
    pathAddresses: node.pathAddresses,
    firstSeenBlockHeight: node.firstSeenBlockHeight,
    lastSeenBlockHeight: node.lastSeenBlockHeight,
  };
}
