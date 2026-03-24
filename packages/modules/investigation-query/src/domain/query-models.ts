import type { RiskLevel, RiskReason } from '@onlydoge/shared-kernel';

export interface InfoResponse {
  addresses: string[];
  risk: {
    level: RiskLevel;
    reasons: RiskReason[];
  };
  assets: Array<{
    network: string;
    token?: string;
    balance: string;
  }>;
  tokens: Array<{
    id: string;
    name: string;
    symbol: string;
    address: string;
    decimals: number;
  }>;
  sources: Array<{
    network: string;
    entity: string;
    from: string;
    to: string;
    hops: number;
  }>;
  networks: Array<{
    id: string;
    name: string;
    chainId: number;
  }>;
  entities: Array<{
    id: string;
    name: string | null;
    description: string;
    data: Record<string, unknown>;
    tags?: string[];
  }>;
  tags: Array<{
    id: string;
    name: string;
    riskLevel: RiskLevel;
  }>;
}
