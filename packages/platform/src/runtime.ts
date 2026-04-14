import { AccessControlService } from '@onlydoge/access-control';
import { EntityLabelingService } from '@onlydoge/entity-labeling';
import { ExplorerQueryService } from '@onlydoge/explorer-query';
import { IndexingPipelineService } from '@onlydoge/indexing-pipeline';
import { InvestigationQueryService } from '@onlydoge/investigation-query';
import { NetworkCatalogService } from '@onlydoge/network-catalog';

import { RelationalMetadataStore } from './metadata-store';
import { createRawBlockStorage } from './raw-block-storage';
import { HttpBlockchainRpcGateway } from './rpc';
import { type AppSettings, loadSettings } from './settings';
import { createWarehouse } from './warehouse';

export interface Runtime {
  accessControl: AccessControlService;
  entityLabeling: EntityLabelingService;
  explorerQuery: ExplorerQueryService;
  indexingPipeline: IndexingPipelineService;
  investigationQuery: InvestigationQueryService;
  metadata: RelationalMetadataStore;
  networkCatalog: NetworkCatalogService;
  settings: AppSettings;
}

export async function createRuntime(input?: {
  ip?: string;
  mode?: string;
  port?: number;
}): Promise<Runtime> {
  const settings = loadSettings(input);
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const rawBlockStorage = createRawBlockStorage(settings.storage);
  const rpc = new HttpBlockchainRpcGateway();
  const warehouse = await createWarehouse(settings.warehouse);

  const accessControl = new AccessControlService(metadata);
  const entityLabeling = new EntityLabelingService(
    metadata,
    metadata,
    metadata,
    metadata,
    metadata,
    metadata,
  );
  const networkCatalog = new NetworkCatalogService(metadata, metadata, rpc, {
    markNetworksUpdated: () => metadata.setJsonValue('networks_updated', 1),
    softDeleteAddressesByNetworkIds: (networkIds) =>
      entityLabeling.softDeleteAddressesByNetworkIds(networkIds),
  });
  const investigationQuery = new InvestigationQueryService(metadata, warehouse, metadata);
  const explorerQuery = new ExplorerQueryService(
    metadata,
    metadata,
    warehouse,
    rawBlockStorage,
    metadata,
  );
  const indexingPipeline = new IndexingPipelineService(
    metadata,
    metadata,
    metadata,
    rawBlockStorage,
    rpc,
    warehouse,
    settings.indexer,
  );

  return {
    settings,
    metadata,
    accessControl,
    networkCatalog,
    entityLabeling,
    explorerQuery,
    investigationQuery,
    indexingPipeline,
  };
}
