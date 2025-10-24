import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './MetadataView.css'

interface MetadataViewProps {
  metadata: ParquetPageMetadata
}

function MetadataView({ metadata }: MetadataViewProps) {
  const { fileMetadata, rowGroups } = metadata

  // Collect all column-level key-value metadata
  const columnMetadata: Array<{
    rowGroupIndex: number
    columnIndex: number
    columnName: string
    metadata: Array<{ key: string; value?: string }>
  }> = []

  rowGroups.forEach((rg) => {
    rg.columns.forEach((col) => {
      if (col.keyValueMetadata && col.keyValueMetadata.length > 0) {
        columnMetadata.push({
          rowGroupIndex: rg.rowGroupIndex,
          columnIndex: col.columnIndex,
          columnName: col.columnName,
          metadata: col.keyValueMetadata,
        })
      }
    })
  })

  return (
    <div className="metadata-view">
      <section className="metadata-section">
        <h2>File-Level Metadata</h2>
        {fileMetadata.keyValueMetadata && fileMetadata.keyValueMetadata.length > 0 ? (
          <div className="metadata-table">
            <table>
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {fileMetadata.keyValueMetadata.map((kv, idx) => (
                  <tr key={idx}>
                    <td className="key-cell">{kv.key}</td>
                    <td className="value-cell">{kv.value || <em>No value</em>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="no-metadata">No file-level key-value metadata found.</p>
        )}
      </section>

      {columnMetadata.length > 0 && (
        <section className="metadata-section">
          <h2>Column-Level Metadata</h2>
          <div className="column-metadata-list">
            {columnMetadata.map((col, idx) => (
              <div key={idx} className="column-metadata-card">
                <div className="column-metadata-header">
                  <h3>{col.columnName}</h3>
                  <span className="column-location">
                    Row Group {col.rowGroupIndex}, Column {col.columnIndex}
                  </span>
                </div>
                <div className="metadata-table">
                  <table>
                    <thead>
                      <tr>
                        <th>Key</th>
                        <th>Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {col.metadata.map((kv, kvIdx) => (
                        <tr key={kvIdx}>
                          <td className="key-cell">{kv.key}</td>
                          <td className="value-cell">{kv.value || <em>No value</em>}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {!fileMetadata.keyValueMetadata?.length && columnMetadata.length === 0 && (
        <section className="metadata-section">
          <div className="no-metadata-message">
            <svg
              className="no-metadata-icon"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
            <h3>No Metadata Found</h3>
            <p>This Parquet file does not contain any custom key-value metadata at the file or column level.</p>
          </div>
        </section>
      )}
    </div>
  )
}

export default MetadataView
