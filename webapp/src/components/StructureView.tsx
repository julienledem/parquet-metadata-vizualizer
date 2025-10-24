import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './StructureView.css'

interface StructureViewProps {
  metadata: ParquetPageMetadata
}

function StructureView({ metadata }: StructureViewProps) {
  const { fileMetadata, rowGroups } = metadata

  // Calculate byte positions for visual layout
  const calculateLayout = () => {
    const layout: Array<{
      type: string
      label: string
      start: number
      size: number
      children?: Array<{ type: string; label: string; start: number; size: number }>
    }> = []

    // Header (magic number)
    layout.push({
      type: 'header',
      label: 'Header (PAR1 Magic Number)',
      start: 0,
      size: 4,
    })

    let currentOffset = 4

    // Row groups and column chunks
    const MAX_COLUMNS_TO_DISPLAY = 100 // Limit detail view for performance

    rowGroups.forEach((rg, rgIndex) => {
      const rgStart = currentOffset
      const columns: Array<{ type: string; label: string; start: number; size: number }> = []

      // Show details for first MAX_COLUMNS_TO_DISPLAY columns, then summarize
      const columnsToShow = Math.min(rg.columns.length, MAX_COLUMNS_TO_DISPLAY)

      for (let colIndex = 0; colIndex < columnsToShow; colIndex++) {
        const col = rg.columns[colIndex]
        const colSize = Number(col.totalCompressedSize)
        columns.push({
          type: 'column',
          label: `Column ${colIndex}: ${col.columnName} (${colSize.toLocaleString()} bytes)`,
          start: currentOffset,
          size: colSize,
        })
        currentOffset += colSize
      }

      // If there are more columns, add summary and calculate their size
      if (rg.columns.length > MAX_COLUMNS_TO_DISPLAY) {
        const remainingColumns = rg.columns.slice(MAX_COLUMNS_TO_DISPLAY)
        const remainingSize = remainingColumns.reduce((sum, col) => sum + Number(col.totalCompressedSize), 0)

        columns.push({
          type: 'column-summary',
          label: `... and ${remainingColumns.length} more columns (${remainingSize.toLocaleString()} bytes total)`,
          start: currentOffset,
          size: remainingSize,
        })
        currentOffset += remainingSize
      }

      layout.push({
        type: 'rowgroup',
        label: `Row Group ${rgIndex} (${rg.numRows.toLocaleString()} rows, ${rg.columns.length} columns, ${Number(rg.totalCompressedSize || rg.totalByteSize).toLocaleString()} bytes)`,
        start: rgStart,
        size: currentOffset - rgStart,
        children: columns,
      })
    })

    // Footer (exact size from metadata)
    const footerStart = currentOffset
    const footerSize = fileMetadata.footerLength || 0 // Use actual size from metadata
    layout.push({
      type: 'footer',
      label: `Footer (Metadata + PAR1 Magic Number)`,
      start: footerStart,
      size: footerSize,
    })

    return layout
  }

  const layout = calculateLayout()

  return (
    <div className="structure-view">
      <div className="structure-header">
        <h2>File Structure Layout</h2>
        <p className="structure-description">
          Visual representation of the physical layout of the Parquet file showing headers, row groups, column chunks, and footer.
        </p>
      </div>

      <div className="structure-diagram">
        {layout.map((section, idx) => (
          <div key={idx} className={`structure-section ${section.type}`}>
            <div className="section-header">
              <span className="section-label">{section.label}</span>
              <span className="section-offset">
                Offset: {section.start.toLocaleString()} | Size: {section.size.toLocaleString()} bytes
              </span>
            </div>
            {section.children && section.children.length > 0 && (
              <div className="section-children">
                {section.children.map((child, childIdx) => (
                  <div key={childIdx} className={`structure-section ${child.type}`}>
                    <div className="section-header child">
                      <span className="section-label">{child.label}</span>
                      <span className="section-offset">
                        Offset: {child.start.toLocaleString()}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="structure-summary">
        <h3>Layout Summary</h3>
        <div className="summary-grid">
          <div className="summary-item">
            <span className="summary-label">Total Row Groups:</span>
            <span className="summary-value">{fileMetadata.numRowGroups}</span>
          </div>
          <div className="summary-item">
            <span className="summary-label">Columns per Row Group:</span>
            <span className="summary-value">{fileMetadata.numColumns}</span>
          </div>
          <div className="summary-item">
            <span className="summary-label">Total Rows:</span>
            <span className="summary-value">{fileMetadata.numRows.toLocaleString()}</span>
          </div>
          <div className="summary-item">
            <span className="summary-label">Total Compressed Size:</span>
            <span className="summary-value">
              {metadata.aggregateStats.totalCompressedBytes.toLocaleString()} bytes
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}

export default StructureView
