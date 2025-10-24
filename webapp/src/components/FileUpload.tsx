import { useState } from 'react'
import { readParquetPagesFromFile } from '../../../src/lib/parquet-parsing'
import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './FileUpload.css'

interface FileUploadProps {
  onFileLoaded: (fileName: string, metadata: ParquetPageMetadata) => void
}

function FileUpload({ onFileLoaded }: FileUploadProps) {
  const [isDragging, setIsDragging] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  const handleFile = async (file: File) => {
    if (!file.name.endsWith('.parquet')) {
      setError('Please upload a .parquet file')
      return
    }

    setError(null)
    setIsLoading(true)

    try {
      console.log(`[FileUpload] Starting to read file: ${file.name}`)
      console.log(`[FileUpload] File size: ${(file.size / 1024 / 1024).toFixed(2)} MB`)

      // Use chunked reading that only loads the footer, not the entire file
      console.log(`[FileUpload] Reading file footer (memory-efficient mode)...`)
      const metadata = await readParquetPagesFromFile(file)

      console.log(`[FileUpload] Metadata parsed successfully`)
      console.log(`[FileUpload] Row groups: ${metadata.fileMetadata.numRowGroups}`)
      console.log(`[FileUpload] Total columns: ${metadata.fileMetadata.numColumns}`)
      console.log(`[FileUpload] Total rows: ${metadata.fileMetadata.numRows}`)

      onFileLoaded(file.name, metadata)
    } catch (err) {
      console.error('[FileUpload] Error parsing file:', err)
      if (err instanceof Error) {
        console.error('[FileUpload] Error stack:', err.stack)
      }
      setError(`Error parsing file: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      setIsLoading(false)
    }
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)

    const file = e.dataTransfer.files[0]
    if (file) {
      handleFile(file)
    }
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(true)
  }

  const handleDragLeave = () => {
    setIsDragging(false)
  }

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      handleFile(file)
    }
  }

  return (
    <div className="file-upload-container">
      <div
        className={`drop-zone ${isDragging ? 'dragging' : ''} ${isLoading ? 'loading' : ''}`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        {isLoading ? (
          <div className="loading-message">
            <div className="spinner"></div>
            <p>Parsing Parquet file...</p>
          </div>
        ) : (
          <>
            <svg
              className="upload-icon"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
              />
            </svg>
            <p className="drop-message">
              Drop your .parquet file here
              <br />
              or
            </p>
            <label className="file-input-label">
              <input
                type="file"
                accept=".parquet"
                onChange={handleFileInput}
                className="file-input"
              />
              Choose File
            </label>
          </>
        )}
      </div>

      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
    </div>
  )
}

export default FileUpload
