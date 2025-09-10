export interface LineageNode {
  id: string;
  name: string;
  type: 'source' | 'integration' | 'transform' | 'enrichment';
  // icon can be an emoji/text or an image descriptor
  icon?: string | { src: string; alt?: string; size?: number };
  // keep style loosely typed to avoid React import here
  style?: { [key: string]: string | number };
}

export interface LineageEdge {
  source: string;
  target: string;
  transform: string;
}

export class LineageDAG {
  /**
   * Simple lineage: DB2 (PNG icon) → Fivetran → Bronze → Silver → Current Dataset
   * Kept the method name for compatibility with existing imports/usages.
   */
  static createSalesforceLineage(): { nodes: LineageNode[]; edges: LineageEdge[] } {
    const nodes: LineageNode[] = [
      {
        id: 'db2',
        name: 'DB2 for z/OS',
        type: 'source',
        icon: { src: '/db2.png', alt: 'DB2', size: 16 }, // <-- PNG icon
        style: { backgroundColor: 'green', color: 'white' },
      },
      {
        id: 'fivetran',
        name: 'Fivetran MDLS',
        type: 'integration',
        icon: '🔄',
        style: { backgroundColor: '#0ea5e9', color: 'white' },
      },
      {
        id: 'Snowflake',
        name: 'Snowflake',
        type: 'CDW',
        icon: { src: '/Snowflake2.png', alt: 'CDW', size: 16, scale: 2 },
        style: { backgroundColor: '#6b7280', color: 'white' },
      },
      {
        id: 'transform_silver',
        name: 'Silver',
        type: 'transform',
        icon: '🥈',
        style: { backgroundColor: '#9ca3af', color: 'black' },
      },
      {
        id: 'current_dataset',
        name: 'Current Dataset',
        type: 'enrichment',
        icon: '🗄️',
        style: { backgroundColor: '#f3f4f6', color: '#111827' },
      },
    ];

    const edges: LineageEdge[] = [
      { source: 'db2', target: 'fivetran', transform: 'replication' },
      { source: 'fivetran', target: 'transform_bronze', transform: 'raw data' },
      { source: 'transform_bronze', target: 'transform_silver', transform: 'transform' },
      { source: 'transform_silver', target: 'current_dataset', transform: 'enriched' },
    ];

    return { nodes, edges };
  }
}
