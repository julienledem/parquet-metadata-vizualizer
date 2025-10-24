import { useState } from 'react'
import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './PagesView.css'

interface PagesViewProps {
  metadata: ParquetPageMetadata
}

function PagesView({ metadata }: PagesViewProps) {
  const { rowGroups } = metadata
  const [selectedRowGroup, setSelectedRowGroup] = useState<number>(0)
  const [selectedColumn, setSelectedColumn] = useState<number>(0)

  const currentRowGroup = rowGroups[selectedRowGroup]
  const currentColumn = currentRowGroup?.columns[selectedColumn]

  return (
    <div className="pages-view">
      <section className="pages-section">
        <h2>Page-Level Analysis</h2>
        <p className="pages-description">
          Explore pages within column chunks across row groups. Pages are the smallest unit of data storage in Parquet files.
        </p>

        <div className="filter-controls">
          <div className="filter-group">
            <label htmlFor="rowgroup-select">Row Group:</label>
            <select
              id="rowgroup-select"
              value={selectedRowGroup}
              onChange={(e) => {
                setSelectedRowGroup(Number(e.target.value))
                setSelectedColumn(0)
              }}
            >
              {rowGroups.map((rg, idx) => (
                <option key={idx} value={idx}>
                  Row Group {idx} ({rg.numRows.toLocaleString()} rows)
                </option>
              ))}
            </select>
          </div>

          <div className="filter-group">
            <label htmlFor="column-select">Column:</label>
            <select
              id="column-select"
              value={selectedColumn}
              onChange={(e) => setSelectedColumn(Number(e.target.value))}
            >
              {currentRowGroup?.columns.map((col, idx) => (
                <option key={idx} value={idx}>
                  {col.columnName}
                </option>
              ))}
            </select>
          </div>
        </div>
      </section>

      {currentColumn && (
        <>
          <section className="pages-section">
            <h3>Column Chunk Summary</h3>
            <div className="column-chunk-summary">
              <div className="summary-grid">
                <div className="summary-item">
                  <span className="summary-label">Column Name:</span>
                  <span className="summary-value">{currentColumn.columnName}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Physical Type:</span>
                  <span className="summary-value">{currentColumn.physicalType}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Compression:</span>
                  <span className="summary-value">{currentColumn.compressionCodec}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Total Pages:</span>
                  <span className="summary-value">{currentColumn.numPages}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Total Values:</span>
                  <span className="summary-value">{currentColumn.totalValues.toLocaleString()}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Compressed Size:</span>
                  <span className="summary-value">
                    {currentColumn.totalCompressedSize.toLocaleString()} bytes
                  </span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Compression Ratio:</span>
                  <span className="summary-value">{currentColumn.compressionRatio}x</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Encodings:</span>
                  <span className="summary-value">{currentColumn.encodings.join(', ')}</span>
                </div>
              </div>
            </div>
          </section>

          {currentColumn.encodingStats && currentColumn.encodingStats.length > 0 && (
            <section className="pages-section">
              <h3>Encoding Statistics</h3>
              <div className="encoding-stats">
                {currentColumn.encodingStats.map((stat, idx) => (
                  <div key={idx} className="encoding-stat-card">
                    <div className="stat-header">
                      <span className="stat-type">{stat.pageType}</span>
                      <span className="stat-count">{stat.count} pages</span>
                    </div>
                    <div className="stat-encoding">{stat.encoding}</div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {currentColumn.pages.length > 0 && (
            <section className="pages-section">
              <h3>Page Statistics Summary</h3>
              {(() => {
                // Calculate statistics grouped by page type
                const statsByType = currentColumn.pages.reduce((acc, page) => {
                  const type = page.pageType || 'UNKNOWN'
                  if (!acc[type]) {
                    acc[type] = {
                      count: 0,
                      totalCompressedSize: 0,
                      totalUncompressedSize: 0,
                      minCompressedSize: Infinity,
                      maxCompressedSize: -Infinity,
                      minUncompressedSize: Infinity,
                      maxUncompressedSize: -Infinity,
                      pages: []
                    }
                  }
                  acc[type].count++
                  if (page.compressedSize !== undefined) {
                    acc[type].totalCompressedSize += page.compressedSize
                    acc[type].minCompressedSize = Math.min(acc[type].minCompressedSize, page.compressedSize)
                    acc[type].maxCompressedSize = Math.max(acc[type].maxCompressedSize, page.compressedSize)
                  }
                  if (page.uncompressedSize !== undefined) {
                    acc[type].totalUncompressedSize += page.uncompressedSize
                    acc[type].minUncompressedSize = Math.min(acc[type].minUncompressedSize, page.uncompressedSize)
                    acc[type].maxUncompressedSize = Math.max(acc[type].maxUncompressedSize, page.uncompressedSize)
                  }
                  acc[type].pages.push(page)
                  return acc
                }, {} as Record<string, {
                  count: number
                  totalCompressedSize: number
                  totalUncompressedSize: number
                  minCompressedSize: number
                  maxCompressedSize: number
                  minUncompressedSize: number
                  maxUncompressedSize: number
                  pages: typeof currentColumn.pages
                }>)

                return (
                  <div className="page-stats-grid">
                    {Object.entries(statsByType).map(([pageType, stats]) => {
                      const avgCompressed = stats.count > 0 ? stats.totalCompressedSize / stats.count : 0
                      const avgUncompressed = stats.count > 0 ? stats.totalUncompressedSize / stats.count : 0
                      const avgCompressionRatio = avgCompressed > 0 ? avgUncompressed / avgCompressed : 0

                      // Calculate values per page if we have row information
                      const pagesWithRows = stats.pages.filter(p => p.firstRowIndex !== undefined)
                      let avgValuesPerPage = 0
                      if (pagesWithRows.length > 1) {
                        const rowDiffs = []
                        for (let i = 1; i < pagesWithRows.length; i++) {
                          const diff = Number(pagesWithRows[i].firstRowIndex!) - Number(pagesWithRows[i-1].firstRowIndex!)
                          if (diff > 0) rowDiffs.push(diff)
                        }
                        if (rowDiffs.length > 0) {
                          avgValuesPerPage = rowDiffs.reduce((a, b) => a + b, 0) / rowDiffs.length
                        }
                      }

                      return (
                        <div key={pageType} className="page-stat-card">
                          <div className="page-stat-header">
                            <span className={`page-type-badge ${pageType.toLowerCase()}`}>
                              {pageType}
                            </span>
                            <span className="page-stat-count">{stats.count} pages</span>
                          </div>
                          <div className="page-stat-body">
                            {stats.totalCompressedSize > 0 && (
                              <>
                                <div className="stat-row">
                                  <span className="stat-label">Compressed Size:</span>
                                  <div className="stat-values">
                                    <div>Avg: {avgCompressed.toLocaleString(undefined, {maximumFractionDigits: 0})} bytes</div>
                                    <div className="stat-range">
                                      Min: {stats.minCompressedSize.toLocaleString()} |
                                      Max: {stats.maxCompressedSize.toLocaleString()}
                                    </div>
                                  </div>
                                </div>
                                <div className="stat-row">
                                  <span className="stat-label">Uncompressed Size:</span>
                                  <div className="stat-values">
                                    <div>Avg: {avgUncompressed.toLocaleString(undefined, {maximumFractionDigits: 0})} bytes</div>
                                    <div className="stat-range">
                                      Min: {stats.minUncompressedSize.toLocaleString()} |
                                      Max: {stats.maxUncompressedSize.toLocaleString()}
                                    </div>
                                  </div>
                                </div>
                                <div className="stat-row">
                                  <span className="stat-label">Compression Ratio:</span>
                                  <div className="stat-values">
                                    <div className="stat-highlight">
                                      {avgCompressionRatio.toFixed(2)}x
                                    </div>
                                  </div>
                                </div>
                              </>
                            )}
                            {avgValuesPerPage > 0 && (
                              <div className="stat-row">
                                <span className="stat-label">Avg Values/Page:</span>
                                <div className="stat-values">
                                  <div className="stat-highlight">
                                    {avgValuesPerPage.toLocaleString(undefined, {maximumFractionDigits: 0})}
                                  </div>
                                </div>
                              </div>
                            )}
                            {stats.totalCompressedSize === 0 && stats.totalUncompressedSize === 0 && (
                              <div className="stat-row">
                                <span className="stat-info">Size information not available</span>
                              </div>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )
              })()}
            </section>
          )}

          <section className="pages-section">
            <h3>Page Size Histogram</h3>
            {currentColumn.pages.length > 0 && currentColumn.pages.some(p => p.compressedSize || p.uncompressedSize) ? (
              <>
                <div className="histogram-legend">
                  <div className="legend-item">
                    <div className="legend-color compressed"></div>
                    <span>Compressed Size</span>
                  </div>
                  <div className="legend-item">
                    <div className="legend-color uncompressed"></div>
                    <span>Uncompressed Size</span>
                  </div>
                  <div className="legend-divider"></div>
                  <div className="legend-item">
                    <div className="legend-color data-page-color"></div>
                    <span>Data Page</span>
                  </div>
                  <div className="legend-item">
                    <div className="legend-color dictionary-page-color"></div>
                    <span>Dictionary Page</span>
                  </div>
                  <div className="legend-item">
                    <div className="legend-color unknown-page-color"></div>
                    <span>Unknown Type</span>
                  </div>
                </div>
                <div className="histogram-container">
                  {(() => {
                    // Filter pages with size information
                    const pagesWithSizes = currentColumn.pages.filter(p => p.compressedSize || p.uncompressedSize)

                    // Find max size for scaling
                    const maxSize = Math.max(
                      ...pagesWithSizes.map(p => Math.max(p.compressedSize || 0, p.uncompressedSize || 0))
                    )

                    return (
                      <div className="histogram">
                        {pagesWithSizes.map((page, idx) => {
                          const pageType = (page.pageType || 'UNKNOWN').toLowerCase().replace('_', '-')
                          const compressedHeight = page.compressedSize ? (page.compressedSize / maxSize) * 100 : 0
                          const uncompressedHeight = page.uncompressedSize ? (page.uncompressedSize / maxSize) * 100 : 0

                          return (
                            <div key={idx} className="histogram-bar-group" title={`Page ${page.pageNumber}: ${page.pageType || 'UNKNOWN'}`}>
                              <div className="histogram-bars">
                                {page.compressedSize && (
                                  <div
                                    className={`histogram-bar compressed ${pageType}`}
                                    style={{ height: `${compressedHeight}%` }}
                                    title={`Compressed: ${page.compressedSize.toLocaleString()} bytes`}
                                  >
                                    <span className="bar-label">{(page.compressedSize / 1024).toFixed(1)}K</span>
                                  </div>
                                )}
                                {page.uncompressedSize && (
                                  <div
                                    className={`histogram-bar uncompressed ${pageType}`}
                                    style={{ height: `${uncompressedHeight}%` }}
                                    title={`Uncompressed: ${page.uncompressedSize.toLocaleString()} bytes`}
                                  >
                                    <span className="bar-label">{(page.uncompressedSize / 1024).toFixed(1)}K</span>
                                  </div>
                                )}
                              </div>
                              <div className="histogram-label">P{page.pageNumber}</div>
                            </div>
                          )
                        })}
                      </div>
                    )
                  })()}
                </div>
                <div className="histogram-info">
                  <p>Showing {currentColumn.pages.filter(p => p.compressedSize || p.uncompressedSize).length} pages with size information</p>
                </div>
              </>
            ) : (
              <p className="no-pages">No page size information available for visualization.</p>
            )}
          </section>
        </>
      )}
    </div>
  )
}

export default PagesView
