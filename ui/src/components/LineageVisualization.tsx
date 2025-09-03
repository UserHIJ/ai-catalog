import React from 'react';
import { LineageDAG } from '@/utils/lineageDAG';

interface LineageVisualizationProps {
  datasetId: string;
}

export const LineageVisualization: React.FC<LineageVisualizationProps> = ({ datasetId }) => {
  const { nodes, edges } = LineageDAG.createSalesforceLineage();

  return (
    <div style={{
      width: '100%',
      padding: '20px',
      backgroundColor: '#f5f5f5',
      borderRadius: '8px',
      position: 'relative',
      left: '-.1in',
      transform: 'scale(0.95)', // ← Shrinks everything by 5%
      transformOrigin: 'left center' // ← Keeps it aligned left

    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexWrap: 'wrap',
        gap: '16px',
        marginBottom: '16px'
      }}>
        {nodes.map((node, index) => (
          <React.Fragment key={node.id}>
            <div style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              padding: '12px',
              backgroundColor: 'white',
              border: '2px solid #ddd',
              borderRadius: '8px',
              minWidth: '100px',
              boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
            }}>
              <div style={{
                fontSize: '24px',
                marginBottom: '8px'
              }}>{node.icon}</div>
              <div style={{
                fontWeight: '500',
                textAlign: 'center'
              }}>{node.name}</div>
            </div>
            {index < nodes.length - 1 && (
              <div style={{
                fontSize: '20px',
                color: '#666',
                padding: '0 8px'
              }}>→</div>
            )}
          </React.Fragment>
        ))}
      </div>
      
      {edges.length > 0 && (
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          gap: '16px'
        }}>
          {edges.map((edge, index) => (
            <div key={index} style={{
              padding: '4px 8px',
              backgroundColor: '#e3f2fd',
              borderRadius: '4px',
              fontSize: '12px',
              color: '#1976d2'
            }}>
              {edge.transform}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};