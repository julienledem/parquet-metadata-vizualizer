import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './InfoView.css'

interface InfoViewProps {
  metadata: ParquetPageMetadata
}

function InfoView({ metadata }: InfoViewProps) {
  const { fileMetadata, aggregateStats } = metadata

  const formatBytes = (bytes: number | bigint) => {
    const numBytes = Number(bytes)
    if (numBytes < 1024) return `${numBytes} B`
    if (numBytes < 1024 * 1024) return `${(numBytes / 1024).toFixed(2)} KB`
    if (numBytes < 1024 * 1024 * 1024) return `${(numBytes / (1024 * 1024)).toFixed(2)} MB`
    return `${(numBytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
  }

  return (
    <div className="info-view">
      <section className="info-section">
        <h2>File Information</h2>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Parquet Version:</span>
            <span className="info-value">{fileMetadata.version}</span>
          </div>
          {fileMetadata.createdBy && (
            <div className="info-item">
              <span className="info-label">Created By:</span>
              <span className="info-value">{fileMetadata.createdBy}</span>
            </div>
          )}
          <div className="info-item">
            <span className="info-label">Total Rows:</span>
            <span className="info-value">{fileMetadata.numRows.toLocaleString()}</span>
          </div>
          <div className="info-item">
            <span className="info-label">Row Groups:</span>
            <span className="info-value">{fileMetadata.numRowGroups}</span>
          </div>
          <div className="info-item">
            <span className="info-label">Columns:</span>
            <span className="info-value">{fileMetadata.numColumns}</span>
          </div>
        </div>
      </section>

      <section className="info-section">
        <h2>Compression Statistics</h2>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Total Compressed Size:</span>
            <span className="info-value">
              {formatBytes(aggregateStats.totalCompressedBytes)}
              <span className="info-subvalue">
                ({aggregateStats.totalCompressedBytes.toLocaleString()} bytes)
              </span>
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Total Uncompressed Size:</span>
            <span className="info-value">
              {formatBytes(aggregateStats.totalUncompressedBytes)}
              <span className="info-subvalue">
                ({aggregateStats.totalUncompressedBytes.toLocaleString()} bytes)
              </span>
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Overall Compression Ratio:</span>
            <span className="info-value highlight">
              {aggregateStats.overallCompressionRatio}x
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Compression Codecs:</span>
            <span className="info-value">
              {aggregateStats.codecsUsed.join(', ')}
            </span>
          </div>
        </div>
      </section>

      <section className="info-section">
        <h2>Page Statistics</h2>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Total Column Chunks:</span>
            <span className="info-value">{aggregateStats.totalColumnChunks}</span>
          </div>
          <div className="info-item">
            <span className="info-label">Total Pages:</span>
            <span className="info-value">{aggregateStats.totalPages}</span>
          </div>
          <div className="info-item">
            <span className="info-label">Avg Pages per Column Chunk:</span>
            <span className="info-value">{aggregateStats.averagePagesPerColumnChunk}</span>
          </div>
          <div className="info-item">
            <span className="info-label">Encodings Used:</span>
            <span className="info-value">
              {aggregateStats.encodingsUsed.join(', ')}
            </span>
          </div>
        </div>
      </section>

      <section className="info-section">
        <h2>Metadata Features</h2>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Columns with Dictionary Pages:</span>
            <span className="info-value">
              {aggregateStats.columnsWithDictionary} / {aggregateStats.totalColumnChunks}
              <span className="info-subvalue">
                ({((aggregateStats.columnsWithDictionary / aggregateStats.totalColumnChunks) * 100).toFixed(1)}%)
              </span>
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Columns with Statistics:</span>
            <span className="info-value">
              {aggregateStats.columnsWithStatistics} / {aggregateStats.totalColumnChunks}
              <span className="info-subvalue">
                ({((aggregateStats.columnsWithStatistics / aggregateStats.totalColumnChunks) * 100).toFixed(1)}%)
              </span>
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Columns with Bloom Filters:</span>
            <span className="info-value">
              {aggregateStats.columnsWithBloomFilter} / {aggregateStats.totalColumnChunks}
              <span className="info-subvalue">
                ({((aggregateStats.columnsWithBloomFilter / aggregateStats.totalColumnChunks) * 100).toFixed(1)}%)
              </span>
            </span>
          </div>
          <div className="info-item">
            <span className="info-label">Columns with Offset Index:</span>
            <span className="info-value">
              {aggregateStats.columnsWithOffsetIndex} / {aggregateStats.totalColumnChunks}
              <span className="info-subvalue">
                ({((aggregateStats.columnsWithOffsetIndex / aggregateStats.totalColumnChunks) * 100).toFixed(1)}%)
              </span>
            </span>
          </div>
        </div>
      </section>
    </div>
  )
}

export default InfoView
