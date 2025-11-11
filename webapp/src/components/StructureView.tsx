import { useState } from 'react'
import type { ParquetPageMetadata, ParquetFileMetadata, RowGroupMetadata, ColumnChunkMetadata } from '../../../src/lib/parquet-parsing'
import './StructureView.css'
import type {SchemaElement, RowGroup, ColumnChunk} from "hyparquet";

interface StructureViewProps {
  metadata: ParquetPageMetadata
  onColumnClick: (rowGroupIndex: number, columnIndex: number) => void
}

export default function StructureView({ metadata, onColumnClick }: StructureViewProps) {
  const fileMetadata: ParquetFileMetadata = metadata.fileMetadata;
  const rowGroups: RowGroupMetadata[] = metadata.rowGroups;

  // État pour gérer les sections réduites
  const [collapsedSections, setCollapsedSections] = useState<Set<string>>(new Set())

  // Fonction pour basculer l'état réduit/développé
  const toggleCollapse = (sectionId: string) => {
    const newCollapsed = new Set(collapsedSections)
    if (newCollapsed.has(sectionId)) {
      newCollapsed.delete(sectionId)
    } else {
      newCollapsed.add(sectionId)
    }
    setCollapsedSections(newCollapsed)
  }

  // State to track how many columns to display per row group
  const [columnsPerRowGroup, setColumnsPerRowGroup] = useState<Record<number, number>>({})

  // Default batch size for loading columns
  const MAX_COLUMNS_TO_DISPLAY = 100

  // Calculate byte positions for visual layout
  const calculateLayout = () => {
    const layout: Array<{
      type: string
      label: string
      start: number
      size: number
      children?: Array<{
        type: string
        label: string
        start: number
        size: number
        rowGroupIndex?: number
        columnIndex?: number
        columnName?: string
        columnMetadata?: SchemaElement
        remainingCount?: number
        remainingSize?: number
        children?: Array<any>
      }>
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
    rowGroups.forEach((rg, rgIndex) => {
      const rgStart = currentOffset
      const columns: Array<{
        type: string
        label: string
        start: number
        size: number
        rowGroupIndex?: number
        columnIndex?: number
        columnName?: string
        columnMetadata?: SchemaElement
        remainingCount?: number
        remainingSize?: number
      }> = []

      // Show details based on state or default to MAX_COLUMNS_TO_DISPLAY
      const columnsToShow = Math.min(
        rg.columns.length,
        columnsPerRowGroup[rgIndex] || MAX_COLUMNS_TO_DISPLAY
      )

      for (let colIndex = 0; colIndex < columnsToShow; colIndex++) {
        const col: ColumnChunkMetadata = rg.columns[colIndex]
        const columnName = col.columnName;
        const colSize = Number(col.totalCompressedSize)
        const foundCol = columnMetadataForColumnChunkMetadata(fileMetadata, columnName)
        columns.push({
          type: 'column',
          label: `Column ${colIndex}: ${columnName} (${colSize.toLocaleString()} bytes)`,
          start: currentOffset,
          size: colSize,
          rowGroupIndex: rgIndex,
          columnIndex: colIndex,
          columnName: columnName,
          columnMetadata: foundCol.schema,
        })
        currentOffset += colSize
      }

      // If there are more columns, add "Load More" button
      if (columnsToShow < rg.columns.length) {
        const remainingColumns = rg.columns.slice(columnsToShow)
        const remainingSize = remainingColumns.reduce((sum, col) => sum + Number(col.totalCompressedSize), 0)

        columns.push({
          type: 'load-more-button',
          label: `Load Next ${Math.min(MAX_COLUMNS_TO_DISPLAY, remainingColumns.length)} Columns`,
          start: currentOffset,
          size: remainingSize,
          rowGroupIndex: rgIndex,
          remainingCount: remainingColumns.length,
          remainingSize: remainingSize,
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

    // Calculate offset index and column index sizes
    let totalOffsetIndexSize = 0
    let totalColumnIndexSize = 0
    let offsetIndexCount = 0
    let columnIndexCount = 0

    fileMetadata.rowGroups.forEach((rg: RowGroup) => {
      rg.columns.forEach((col: ColumnChunk) => {
        if (col.offset_index_offset !== undefined && col.offset_index_length !== undefined) {
          totalOffsetIndexSize += col.offset_index_length
          offsetIndexCount++
        }
        if (col.column_index_offset !== undefined && col.column_index_length !== undefined) {
          totalColumnIndexSize += col.column_index_length
          columnIndexCount++
        }
      })
    })

    // Offset indexes (if present)
    // see thrift definition for "struct OffsetIndex" in https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1138
    if (totalOffsetIndexSize > 0) {
      const offsetStart = currentOffset
      const MAX_INDEX_SIZE_TO_DISPLAY = 1024 * 1024 // 1MB

      // Skip detailed parsing if indexes are too large (performance optimization)
      if (totalOffsetIndexSize > MAX_INDEX_SIZE_TO_DISPLAY) {
        layout.push({
          type: 'offset-index',
          label: `Offset Indexes (${offsetIndexCount} indexes, page location metadata) - ${totalOffsetIndexSize.toLocaleString()} bytes (too large to display details)`,
          start: offsetStart,
          size: totalOffsetIndexSize,
        })
      } else {
        const offsetChildren: Array<{
          type: string
          label: string
          start: number
          size: number
          rowGroupIndex?: number
          children?: Array<{
            type: string
            label: string
            start: number
            size: number
            rowGroupIndex?: number
            columnIndex?: number
            columnName?: string
            columnMetadata?: SchemaElement
            offset_index_offset?: number
            offset_index_length?: number
          }>
        }> = []

        let rgRunningOffset = 0
        fileMetadata.rowGroups.forEach((rg: RowGroup, rgIndex: number) => {
          const rgOffsetSize = rg.columns.reduce((sum: number, col: any) => {
            return sum + (col.offset_index_length !== undefined ? Number(col.offset_index_length) : 0)
          }, 0)

          if (rgOffsetSize > 0) {
            let colRunningOffset = 0
            const colChildren: Array<any> = []
            rg.columns.forEach((col: ColumnChunk, colIndex: number) => {
              const columnMetadata = col.meta_data!!;
              const columnName: string = columnMetadata.path_in_schema.join('.');
              if (col.offset_index_length !== undefined && col.offset_index_length > 0) {
                const foundCol = columnMetadataForColumnChunkMetadata(fileMetadata, columnName)
                colChildren.push({
                  type: 'offset-index-column',
                  label: `Column ${colIndex}: ${columnName} (${col.offset_index_length.toLocaleString()} bytes)`,
                  start: offsetStart + rgRunningOffset + colRunningOffset,
                  size: col.offset_index_length,
                  rowGroupIndex: rgIndex,
                  columnIndex: colIndex,
                  columnName: columnName,
                  columnMetadata: foundCol.schema,
                  offset_index_offset: col.offset_index_offset,
                  offset_index_length: col.offset_index_length,
                })
                colRunningOffset += col.offset_index_length
              }
            })

            offsetChildren.push({
              type: 'offset-index-rowgroup',
              label: `Row Group ${rgIndex} Offset Indexes (${rgOffsetSize.toLocaleString()} bytes)`,
              start: offsetStart + rgRunningOffset,
              size: rgOffsetSize,
              rowGroupIndex: rgIndex,
              children: colChildren,
            })
            rgRunningOffset += rgOffsetSize
          }
        })

        layout.push({
          type: 'offset-index',
          label: `Offset Indexes (${offsetIndexCount} indexes, page location metadata)`,
          start: offsetStart,
          size: totalOffsetIndexSize,
          children: offsetChildren,
        })
      }
      currentOffset += totalOffsetIndexSize
    }

    // Column indexes (if present)
    // see thrift definition for "struct ColumnIndex" in https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1163
    if (totalColumnIndexSize > 0) {
      const columnStart = currentOffset
      const MAX_INDEX_SIZE_TO_DISPLAY = 1024 * 1024 // 1MB

      // Skip detailed parsing if indexes are too large (performance optimization)
      if (totalColumnIndexSize > MAX_INDEX_SIZE_TO_DISPLAY) {
        layout.push({
          type: 'column-index',
          label: `Column Indexes (${columnIndexCount} indexes, statistics per page) - ${totalColumnIndexSize.toLocaleString()} bytes (too large to display details)`,
          start: columnStart,
          size: totalColumnIndexSize,
        })
      } else {
        const columnChildren: Array<{
          type: string
          label: string
          start: number
          size: number
          rowGroupIndex?: number
          children?: Array<{
            type: string
            label: string
            start: number
            size: number
            rowGroupIndex?: number
            columnIndex?: number
            columnName?: string
            columnMetadata?: SchemaElement
            column_index_offset?: number
            column_index_length?: number
          }>
        }> = []

        let rgColRunningOffset = 0
        fileMetadata.rowGroups.forEach((rg: RowGroup, rgIndex: number) => {
          const rgColumnIndexSize = rg.columns.reduce((sum: number, col: any) => {
            return sum + (col.column_index_length !== undefined ? Number(col.column_index_length) : 0)
          }, 0)

          if (rgColumnIndexSize > 0) {
            let colRunningOffset = 0
            const colChildren: Array<any> = []
            rg.columns.forEach((col: ColumnChunk, colIndex: number) => {
              const columnName: string = col.meta_data?.path_in_schema.join('.') || '';
              if (col.column_index_length !== undefined && col.column_index_length > 0) {
                const foundCol = columnMetadataForColumnChunkMetadata(fileMetadata, columnName)
                colChildren.push({
                  type: 'column-index-column',
                  label: `Column ${colIndex}: ${columnName} (${col.column_index_length.toLocaleString()} bytes)`,
                  start: columnStart + rgColRunningOffset + colRunningOffset,
                  size: col.column_index_length,
                  rowGroupIndex: rgIndex,
                  columnIndex: colIndex,
                  columnName,
                  columnMetadata: foundCol.schema,
                  column_index_offset: col.column_index_offset,
                  column_index_length: col.column_index_length,
                })
                colRunningOffset += col.column_index_length
              }
            })

            columnChildren.push({
              type: 'column-index-rowgroup',
              label: `Row Group ${rgIndex} Column Indexes (${rgColumnIndexSize.toLocaleString()} bytes)`,
              start: columnStart + rgColRunningOffset,
              size: rgColumnIndexSize,
              rowGroupIndex: rgIndex,
              children: colChildren,
            })
            rgColRunningOffset += rgColumnIndexSize
          }
        })

        layout.push({
          type: 'column-index',
          label: `Column Indexes (${columnIndexCount} indexes, statistics per page)`,
          start: columnStart,
          size: totalColumnIndexSize,
          children: columnChildren,
        })
      }
      currentOffset += totalColumnIndexSize
    }

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

  // Handler to load more columns for a specific row group
  const handleLoadMore = (rowGroupIndex: number) => {
    const currentCount = columnsPerRowGroup[rowGroupIndex] || MAX_COLUMNS_TO_DISPLAY
    const newCount = currentCount + MAX_COLUMNS_TO_DISPLAY
    setColumnsPerRowGroup({
      ...columnsPerRowGroup,
      [rowGroupIndex]: newCount,
    })
  }

  const layout = calculateLayout()

  return (
    <div className="structure-view">
      <div className="structure-header">
        <h2>File Structure Layout</h2>
        <p className="structure-description">
          Visual representation of the physical layout of the Parquet file showing headers, row groups, column chunks, page indexes, and footer.
        </p>
      </div>

      <div className="structure-diagram">
        {layout.map((section, idx) => (
          <div key={idx} className={`structure-section ${section.type}`}>
            <div
              className="section-header"
              onClick={() => section.children && toggleCollapse(`${section.type}-${idx}`)}
              style={{ cursor: section.children ? 'pointer' : 'default' }}
            >
              {section.children && (
                <span className="collapse-icon">
                  {collapsedSections.has(`${section.type}-${idx}`) ? '▶' : '▼'}
                </span>
              )}
              <span className="section-label">{section.label}</span>
              <span className="section-offset">
                Offset: {section.start.toLocaleString()} | Size: {section.size.toLocaleString()} bytes
              </span>
            </div>
            {section.children && section.children.length > 0 && !collapsedSections.has(`${section.type}-${idx}`) && (
              <div className="section-children">
                {section.children.map((child, childIdx) => {
                  // Handle pagination button for row groups
                  if (child.type === 'load-more-button') {
                    return (
                      <div
                        key={childIdx}
                        className="structure-section load-more-button"
                        style={{ cursor: 'pointer', padding: '12px', backgroundColor: '#f0f0f0', borderRadius: '4px' }}
                        onClick={() => {
                          if (child.rowGroupIndex !== undefined) {
                            handleLoadMore(child.rowGroupIndex)
                          }
                        }}
                      >
                        <div className="section-header child">
                          <span className="section-label" style={{ fontWeight: 'bold', color: '#0066cc' }}>
                            {child.label}
                          </span>
                          <span className="section-offset">
                            ({child.remainingCount?.toLocaleString()} remaining - {child.remainingSize?.toLocaleString()} bytes)
                          </span>
                        </div>
                      </div>
                    )
                  }

                  // Handle nested children (offset-index and column-index)
                  return (
                    <div
                      key={childIdx}
                      className={`structure-section ${child.type} ${child.type === 'column' ? 'clickable' : ''}`}
                      onClick={(e) => {
                        e.stopPropagation()
                        if (child.children) {
                          toggleCollapse(`${section.type}-${idx}-${child.type}-${childIdx}`)
                        } else if (child.type === 'column' && child.rowGroupIndex !== undefined && child.columnIndex !== undefined) {
                          onColumnClick(child.rowGroupIndex, child.columnIndex)
                        }
                      }}
                      style={{ cursor: child.children || child.type === 'column' ? 'pointer' : 'default' }}
                    >
                      <div className="section-header child">
                        {child.children && (
                          <span className="collapse-icon">
                            {collapsedSections.has(`${section.type}-${idx}-${child.type}-${childIdx}`) ? '▶' : '▼'}
                          </span>
                        )}
                        <span className="section-label">{child.label}</span>
                        <span className="section-offset">
                          Offset: {child.start.toLocaleString()}
                        </span>
                      </div>
                      {child.children && child.children.length > 0 && !collapsedSections.has(`${section.type}-${idx}-${child.type}-${childIdx}`) && (
                        <div className="section-children">
                          {child.children.map((colChild, colChildIdx) => (
                            <div
                              key={colChildIdx}
                              className={`structure-section ${colChild.type}`}
                              style={{ marginLeft: 16 }}
                              onClick={(e) => e.stopPropagation()}
                            >
                              <div className="section-header child">
                                <span className="section-label">{colChild.label}</span>
                                <span className="section-offset">
                                  Offset: {(colChild.offset_index_offset ?? colChild.column_index_offset)?.toLocaleString()}
                                </span>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )
                })}
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

interface FoundSchemaElement {
  columnName: string;
  schema?: SchemaElement
}
function columnMetadataForColumnChunkMetadata(fileMetadata: ParquetFileMetadata, columnName: string): FoundSchemaElement {
  const schema = fileMetadata.schema.find(col => col.name === columnName);
  return { columnName, schema };
}
