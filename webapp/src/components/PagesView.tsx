import { useState, useEffect } from 'react'
import type { ParquetPageMetadata, PageInfo } from '../../../src/lib/parquet-parsing'
import { parseParquetPage } from '../../../src/lib/parquet-parsing'
import './PagesView.css'

interface PagesViewProps {
  metadata: ParquetPageMetadata
  file: File
}

function PagesView({ metadata, file }: PagesViewProps) {
  const { rowGroups } = metadata
  const [selectedRowGroup, setSelectedRowGroup] = useState<number>(0)
  const [selectedColumn, setSelectedColumn] = useState<number>(0)
  const [pages, setPages] = useState<PageInfo[]>([])
  const [isLoadingPages, setIsLoadingPages] = useState(false)

  const currentRowGroup = rowGroups[selectedRowGroup]
  const currentColumn = currentRowGroup?.columns[selectedColumn]

  // Load pages dynamically when column selection changes
  useEffect(() => {
    if (!currentColumn) return

    const loadPages = async () => {
      setIsLoadingPages(true)
      try {
        // Get the raw column chunk from metadata
        const rawColumnChunk = metadata.fileMetadata.rowGroups[selectedRowGroup].columns[selectedColumn]

        // Create a byte range reader using File.slice
        const byteRangeReader = async (offset: number, length: number): Promise<ArrayBuffer> => {
          const slice = file.slice(offset, offset + length)
          return await slice.arrayBuffer()
        }

        // Parse actual page headers
        const parsedPages = await parseParquetPage(rawColumnChunk, byteRangeReader)
        setPages(parsedPages)
      } catch (error) {
        console.error('Error loading pages:', error)
        setPages([])
      } finally {
        setIsLoadingPages(false)
      }
    }

    loadPages()
  }, [selectedRowGroup, selectedColumn, currentColumn, metadata.fileMetadata, file])

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
                  <span className="summary-value">
                    {isLoadingPages ? 'Loading...' : pages.length}
                  </span>
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

          {!isLoadingPages && pages.length > 0 && (
            <section className="pages-section">
              <h3>Page Statistics Summary</h3>
              {(() => {
                // Calculate statistics grouped by page type
                const statsByType = pages.reduce((acc, page) => {
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
                      totalNumValues: 0,
                      pagesWithNumValues: 0,
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
                  if (page.numValues !== undefined) {
                    acc[type].totalNumValues += page.numValues
                    acc[type].pagesWithNumValues++
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
                  totalNumValues: number
                  pagesWithNumValues: number
                  pages: PageInfo[]
                }>)

                return (
                  <div className="page-stats-grid">
                    {Object.entries(statsByType).map(([pageType, stats]) => {
                      const avgCompressed = stats.count > 0 ? stats.totalCompressedSize / stats.count : 0
                      const avgUncompressed = stats.count > 0 ? stats.totalUncompressedSize / stats.count : 0
                      const avgCompressionRatio = avgCompressed > 0 ? avgUncompressed / avgCompressed : 0

                      // Calculate average values per page from numValues field
                      const avgValuesPerPage = stats.pagesWithNumValues > 0
                        ? stats.totalNumValues / stats.pagesWithNumValues
                        : 0

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
            {isLoadingPages ? (
              <p className="no-pages">Loading page data...</p>
            ) : pages.length > 0 && pages.some(p => p.compressedSize || p.uncompressedSize) ? (
              <>
                <div className="histogram-legend">
                  {(() => {
                    // Get unique encodings from pages
                    const encodings = new Set(pages.map(p => p.encoding).filter(Boolean))
                    return Array.from(encodings).sort().map(encoding => (
                      <div key={encoding} className="legend-item">
                        <div className={`legend-color encoding-${encoding?.toLowerCase().replace(/_/g, '-')}`}></div>
                        <span>{encoding}</span>
                      </div>
                    ))
                  })()}
                </div>
                <div className="histogram-container">
                  {(() => {
                    // Filter pages with size information
                    const pagesWithSizes = pages.filter(p => p.compressedSize || p.uncompressedSize)

                    // Find max size for scaling
                    const maxSize = Math.max(
                      ...pagesWithSizes.map(p => Math.max(p.compressedSize || 0, p.uncompressedSize || 0))
                    )

                    return (
                      <div className="histogram">
                        {pagesWithSizes.map((page, idx) => {
                          const encoding = (page.encoding || 'unknown').toLowerCase().replace(/_/g, '-')
                          const compressedHeight = page.compressedSize ? (page.compressedSize / maxSize) * 100 : 0
                          const uncompressedHeight = page.uncompressedSize ? (page.uncompressedSize / maxSize) * 100 : 0

                          return (
                            <div key={idx} className="histogram-bar-group" title={`Page ${page.pageNumber}: ${page.pageType || 'UNKNOWN'} - ${page.encoding || 'Unknown Encoding'}`}>
                              <div className="histogram-bars">
                                {/* Uncompressed bar (background) */}
                                {page.uncompressedSize && (
                                  <div
                                    className={`histogram-bar uncompressed encoding-${encoding}`}
                                    style={{ height: `${uncompressedHeight}%` }}
                                    title={`Uncompressed: ${page.uncompressedSize.toLocaleString()} bytes\nCompressed: ${page.compressedSize?.toLocaleString() || 'N/A'} bytes\nEncoding: ${page.encoding || 'Unknown'}\nType: ${page.pageType || 'Unknown'}`}
                                  >
                                    <span className="bar-label bar-label-uncompressed">{(page.uncompressedSize / 1024).toFixed(1)}K</span>
                                  </div>
                                )}
                                {/* Compressed bar (foreground) */}
                                {page.compressedSize && (
                                  <div
                                    className={`histogram-bar compressed encoding-${encoding}`}
                                    style={{ height: `${compressedHeight}%` }}
                                    title={`Compressed: ${page.compressedSize.toLocaleString()} bytes\nUncompressed: ${page.uncompressedSize?.toLocaleString() || 'N/A'} bytes\nEncoding: ${page.encoding || 'Unknown'}\nType: ${page.pageType || 'Unknown'}`}
                                  >
                                    <span className="bar-label bar-label-compressed">{(page.compressedSize / 1024).toFixed(1)}K</span>
                                  </div>
                                )}
                              </div>
                              <div className="histogram-labels">
                                <div className="histogram-label">P{page.pageNumber}</div>
                                {page.pageType === 'DICTIONARY_PAGE' ? (
                                  <div className="histogram-label-dict">DIC</div>
                                ) : page.pageType === 'DATA_PAGE' ? (
                                  <div className="histogram-label-data">DV1</div>
                                ) : page.pageType === 'DATA_PAGE_V2' ? (
                                  <div className="histogram-label-data">DV2</div>
                                ) : (
                                  <div className="histogram-label-dict histogram-label-placeholder"></div>
                                )}
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    )
                  })()}
                </div>
                <div className="histogram-info">
                  <p>Showing {pages.filter(p => p.compressedSize || p.uncompressedSize).length} pages with size information</p>
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
