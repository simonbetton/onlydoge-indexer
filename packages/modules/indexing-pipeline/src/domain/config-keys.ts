import type { PrimaryId } from '@onlydoge/shared-kernel';

export function configKeyPrimary(): string {
  return 'primary';
}

export function configKeyBlockHeight(networkId: PrimaryId): string {
  return `block_height_n${networkId}`;
}

export function configKeyNetworksUpdated(): string {
  return 'networks_updated';
}

export function configKeyNewlyAddedAddress(networkId: PrimaryId, addressId: PrimaryId): string {
  return `newly_added_address_n${networkId}_a${addressId}`;
}

export function configKeyIndexerSyncTail(networkId: PrimaryId): string {
  return `indexer_sync_tail_n${networkId}`;
}

export function configKeyIndexerSyncProgress(networkId: PrimaryId): string {
  return `indexer_sync_progress_n${networkId}`;
}

export function configKeyIndexerProcessTail(networkId: PrimaryId): string {
  return `indexer_process_tail_n${networkId}`;
}

export function configKeyIndexerProcessProgress(networkId: PrimaryId): string {
  return `indexer_process_progress_n${networkId}`;
}

export function configKeyIndexerLink(networkId: PrimaryId, addressId: PrimaryId): string {
  return `indexer_link_n${networkId}_a${addressId}`;
}
