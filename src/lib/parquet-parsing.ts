/**
 * Browser-compatible Parquet I/O functions
 * This file contains only I/O operations using browser File API
 */

import { parseParquetPages } from './parquet-parsing-core.js'

// Re-export all types from core
export type {
  ParquetFileMetadata,
  PageInfo,
  ColumnChunkMetadata,
  RowGroupMetadata,
  ParquetPageMetadata
} from './parquet-parsing-core.js'

import type {
  ParquetPageMetadata
} from './parquet-parsing-core.js'

/**
 * Read footer buffer from a browser File object
 *
 * @param file - File object from browser
 * @returns Footer buffer and total footer length
 */
async function readFooterBuffer(file: File): Promise<{ footerBuffer: ArrayBuffer; footerLength: number }> {
  console.log('[parquet-parsing] Reading footer from file (browser)')

  // Read last 8 bytes to get footer size
  const footerLengthSlice = file.slice(file.size - 8, file.size)
  const footerLengthBuffer = await footerLengthSlice.arrayBuffer()
  const footerLengthView = new DataView(footerLengthBuffer)
  const footerLength = footerLengthView.getUint32(0, true) // little-endian

  console.log(`[parquet-parsing] Footer length: ${footerLength} bytes`)

  // Read the footer + the 8 bytes we just read
  const footerStart = file.size - footerLength - 8
  const footerSlice = file.slice(footerStart, file.size)
  const footerBuffer = await footerSlice.arrayBuffer()

  console.log('[parquet-parsing] Footer read successfully')

  return {
    footerBuffer,
    footerLength: footerLength + 8 // Include the trailing 8 bytes
  }
}

/**
 * Read page-level metadata from a Parquet file (browser File object)
 *
 * @param file - File object from browser
 * @returns Complete page-level metadata including footer and page information
 */
export async function readParquetPagesFromFile(file: File): Promise<ParquetPageMetadata> {
  // Read footer buffer and length
  const { footerBuffer, footerLength } = await readFooterBuffer(file)

  console.log('[parquet-parsing] Parsing metadata')

  // Create a byte range reader using File.slice
  const byteRangeReader = async (offset: number, length: number): Promise<ArrayBuffer> => {
    const slice = file.slice(offset, offset + length)
    return await slice.arrayBuffer()
  }

  // Use core parsing logic with footer length
  const result = await parseParquetPages(footerBuffer, byteRangeReader, footerLength)

  console.log('[parquet-parsing] Processing complete')

  return result
}
