import { useState } from 'react'
import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './SchemaView.css'

interface SchemaViewProps {
  metadata: ParquetPageMetadata
}

function SchemaView({ metadata }: SchemaViewProps) {
  const { fileMetadata, rowGroups } = metadata
  const [expandedColumns, setExpandedColumns] = useState<Set<number>>(new Set())
  const [showAllColumns, setShowAllColumns] = useState(false)

  const MAX_COLUMNS_INITIAL = 100 // Show only first 100 columns initially for performance

  const toggleColumn = (index: number) => {
    const newExpanded = new Set(expandedColumns)
    if (newExpanded.has(index)) {
      newExpanded.delete(index)
    } else {
      newExpanded.add(index)
    }
    setExpandedColumns(newExpanded)
  }

  const renderSchemaElement = (element: any, depth: number = 0) => {
    if (depth === 0 && element.name === 'schema') {
      // Skip root element
      return null
    }

    const indent = depth * 20

    return (
      <div key={element.name} className="schema-element" style={{ marginLeft: `${indent}px` }}>
        <div className="schema-element-header">
          <span className="schema-element-name">{element.name}</span>
          {element.type && <span className="schema-element-type">{element.type}</span>}
          {element.logicalType && (
            <span className="schema-element-logical">
              {JSON.stringify(element.logicalType)}
            </span>
          )}
          {element.repetition_type !== undefined && (
            <span className="schema-element-repetition">
              {element.repetition_type === 0 ? 'REQUIRED' : element.repetition_type === 1 ? 'OPTIONAL' : 'REPEATED'}
            </span>
          )}
        </div>
      </div>
    )
  }

  // Get column statistics from the first row group
  const firstRowGroup = rowGroups[0]
  const columnStats = firstRowGroup?.columns || []

  // Determine how many columns to show
  const isLargeFile = columnStats.length > MAX_COLUMNS_INITIAL
  const columnsToDisplay = showAllColumns || !isLargeFile
    ? columnStats
    : columnStats.slice(0, MAX_COLUMNS_INITIAL)

  return (
    <div className="schema-view">
      <section className="schema-section">
        <h2>Schema Definition</h2>
        <div className="schema-tree">
          {fileMetadata.schema.map((element) => renderSchemaElement(element, 0))}
        </div>
      </section>

      <section className="schema-section">
        <h2>Column Details</h2>
        {isLargeFile && !showAllColumns && (
          <div className="large-file-notice">
            ⚠️ Large file detected ({columnStats.length} columns). Showing first {MAX_COLUMNS_INITIAL} columns.
            <button className="show-all-button" onClick={() => setShowAllColumns(true)}>
              Show All Columns
            </button>
          </div>
        )}
        {isLargeFile && showAllColumns && (
          <div className="large-file-notice">
            Showing all {columnStats.length} columns. This may impact performance.
          </div>
        )}
        <div className="column-list">
          {columnsToDisplay.map((col, idx) => (
            <div key={idx} className="column-card">
              <div
                className="column-card-header"
                onClick={() => toggleColumn(idx)}
                style={{ cursor: 'pointer' }}
              >
                <div className="column-card-title">
                  <span className="column-index">#{idx}</span>
                  <span className="column-name">{col.columnName}</span>
                  <span className="expand-icon">
                    {expandedColumns.has(idx) ? '▼' : '▶'}
                  </span>
                </div>
                <div className="column-card-subtitle">
                  {col.physicalType} | {col.compressionCodec}
                </div>
              </div>

              {expandedColumns.has(idx) && (
                <div className="column-card-body">
                  <div className="column-detail-grid">
                    <div className="column-detail-item">
                      <span className="detail-label">Physical Type:</span>
                      <span className="detail-value">{col.physicalType}</span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Compression:</span>
                      <span className="detail-value">{col.compressionCodec}</span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Encodings:</span>
                      <span className="detail-value">{col.encodings.join(', ')}</span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Total Values:</span>
                      <span className="detail-value">{col.totalValues.toLocaleString()}</span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Compressed Size:</span>
                      <span className="detail-value">
                        {col.totalCompressedSize.toLocaleString()} bytes
                      </span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Uncompressed Size:</span>
                      <span className="detail-value">
                        {col.totalUncompressedSize.toLocaleString()} bytes
                      </span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Compression Ratio:</span>
                      <span className="detail-value">{col.compressionRatio}x</span>
                    </div>
                    <div className="column-detail-item">
                      <span className="detail-label">Pages:</span>
                      <span className="detail-value">{col.numPages}</span>
                    </div>
                    {col.dictionaryPageOffset !== undefined && (
                      <div className="column-detail-item">
                        <span className="detail-label">Dictionary Page:</span>
                        <span className="detail-value">Yes</span>
                      </div>
                    )}
                    {col.bloomFilter && (
                      <div className="column-detail-item">
                        <span className="detail-label">Bloom Filter:</span>
                        <span className="detail-value">
                          {col.bloomFilter.length.toLocaleString()} bytes
                        </span>
                      </div>
                    )}
                  </div>

                  {col.statistics && (
                    <div className="column-statistics">
                      <h4>Column Statistics</h4>
                      <div className="column-detail-grid">
                        {col.statistics.nullCount !== undefined && (
                          <div className="column-detail-item">
                            <span className="detail-label">Null Count:</span>
                            <span className="detail-value">
                              {col.statistics.nullCount.toLocaleString()}
                            </span>
                          </div>
                        )}
                        {col.statistics.distinctCount !== undefined && (
                          <div className="column-detail-item">
                            <span className="detail-label">Distinct Count:</span>
                            <span className="detail-value">
                              {col.statistics.distinctCount.toLocaleString()}
                            </span>
                          </div>
                        )}
                        {col.statistics.min !== undefined && (
                          <div className="column-detail-item">
                            <span className="detail-label">Min Value:</span>
                            <span className="detail-value">
                              {String(col.statistics.min).substring(0, 50)}
                            </span>
                          </div>
                        )}
                        {col.statistics.max !== undefined && (
                          <div className="column-detail-item">
                            <span className="detail-label">Max Value:</span>
                            <span className="detail-value">
                              {String(col.statistics.max).substring(0, 50)}
                            </span>
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}

export default SchemaView
