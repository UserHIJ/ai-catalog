export interface LineageNode {
  id: string;
  name: string;
  type: 'source' | 'integration' | 'transform' | 'enrichment';
  icon: string;
}

export interface LineageEdge {
  source: string;
  target: string;
  transform: string;
}

export class LineageDAG {
  static createSalesforceLineage(): { nodes: LineageNode[], edges: LineageEdge[] } {
    const nodes: LineageNode[] = [
      {
        id: 'salesforce',
        name: 'Salesforce',
        type: 'source',
        icon: '🔄'
      },
      {
        id: 'fivetran',
        name: 'Fivetran',
        type: 'integration', 
        icon: '📡'
      },
      {
        id: 'Snowflake',
        name: 'Snowflake',
        type: 'cdw', 
        icon: '📡'
      },
      {
        id: 'transform_bronze',
        name: 'Transform Bronze',
        type: 'transform',
        icon: '🛠️'
      },
      {
        id: 'transform_silver',
        name: 'Transform Silver',
        type: 'transform',
        icon: '⚡'
      },
      {
        id: 'Analytics',
        name: 'PowerBI',
        type: 'enrichment',
        icon: '🗄️'
      }
    ];

    const edges: LineageEdge[] = [
      { source: 'salesforce', target: 'fivetran', transform: 'replication' },
      { source: 'fivetran', target: 'transform_bronze', transform: 'ingestion' },
      { source: 'transform_bronze', target: 'transform_silver', transform: 'cleansing' },
      { source: 'transform_silver', target: 'current_dataset', transform: 'enrichment' }
    ];

    return { nodes, edges };
  }
}