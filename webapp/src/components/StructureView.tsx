import type { ParquetPageMetadata, ParquetFileMetadata, RowGroupMetadata, ColumnChunkMetadata } from '../../../src/lib/parquet-parsing'
// import type { ParquetPageMetadata, ParquetFileMetadata, RowGroupMetadata, ColumnChunkMetadata } from "hyparquet/src/metadata.js"
import './StructureView.css'
import type {SchemaElement, RowGroup, ColumnChunk, ColumnMetaData} from "hyparquet";
import { useState } from 'react'

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
        columnIndex?: number,
        columnMetadata?: SchemaElement,
        children?: Array<any> // TODO
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
    const MAX_COLUMNS_TO_DISPLAY = 100 // Limit detail view for performance

    rowGroups.forEach((rg, rgIndex) => {
      const rgStart = currentOffset
      const columns: Array<{
        type: string
        label: string
        start: number
        size: number
        rowGroupIndex?: number
        columnIndex?: number,
        columnName?: string,
        columnMetadata?: SchemaElement
      }> = []

      // Show details for first MAX_COLUMNS_TO_DISPLAY columns, then summarize
      const columnsToShow = Math.min(rg.columns.length, MAX_COLUMNS_TO_DISPLAY)

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

    // Calculate offset index and column index sizes
    let totalOffsetIndexSize = 0
    let totalColumnIndexSize = 0
    let offsetIndexCount = 0
    let columnIndexCount = 0

    fileMetadata.rowGroups.forEach((rg: any) => {
      rg.columns.forEach((col: any) => {
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
    if (totalOffsetIndexSize > 0) {
      const offsetStart = currentOffset
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
          columnName?: string, // deprecated
          columnMetadata?: SchemaElement,
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
            const columnName: string = columnMetadata.path_in_schema.reduce((l,r)=> l + '.' + r);
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
      currentOffset += totalOffsetIndexSize
    }

    // Column indexes (if present)
    if (totalColumnIndexSize > 0) {
      const columnStart = currentOffset
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
            const columnName: string = col.meta_data?.path_in_schema.reduce((l,r) => l + '.' + r) || '';
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
                {section.children.map((child, childIdx) => (
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

interface FoundSchemaElement {
  columnName: string;
  schema?: SchemaElement
}
function columnMetadataForColumnChunkMetadata(fileMetadata: ParquetFileMetadata, columnName: string): FoundSchemaElement {
  const schema = fileMetadata.schema.find(col => col.name === columnName);
  return { columnName, schema };
}
