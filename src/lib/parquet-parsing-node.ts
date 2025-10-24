/**
 * Node.js-specific Parquet I/O functions
 * This file contains only I/O operations using Node.js fs module
 */

import { openSync, readSync, fstatSync, closeSync } from 'fs'
import { parseParquetFooter, parseParquetPages } from './parquet-parsing-core.js'

// Re-export all types from core
export type {
  ParquetFileMetadata,
  PageInfo,
  ColumnChunkMetadata,
  RowGroupMetadata,
  ParquetPageMetadata
} from './parquet-parsing-core.js'

import type {
  ParquetFileMetadata,
  ParquetPageMetadata
} from './parquet-parsing-core.js'

/**
 * Read a byte range from a file descriptor
 */
function readByteRange(fd: number, offset: number, length: number): ArrayBuffer {
  const buffer = Buffer.allocUnsafe(length)
  readSync(fd, buffer, 0, length, offset)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Read footer buffer from a Parquet file
 *
 * @param filePath - Path to the Parquet file
 * @returns Footer buffer and file descriptor for further reading
 */
function readFooterBuffer(filePath: string): { footerBuffer: ArrayBuffer; fd: number; fileSize: number } {
  const fd = openSync(filePath, 'r')
  const fileStats = fstatSync(fd)
  const fileSize = fileStats.size

  // Read the last 8 bytes to get the footer size
  const footerLengthBuffer = Buffer.allocUnsafe(8)
  readSync(fd, footerLengthBuffer, 0, 8, fileSize - 8)
  const footerLength = footerLengthBuffer.readUInt32LE(0)

  // Read the footer + the 8 bytes we just read
  const footerBuffer = Buffer.allocUnsafe(footerLength + 8)
  readSync(fd, footerBuffer, 0, footerLength + 8, fileSize - footerLength - 8)

  const arrayBuffer = footerBuffer.buffer.slice(
    footerBuffer.byteOffset,
    footerBuffer.byteOffset + footerBuffer.byteLength
  )

  return { footerBuffer: arrayBuffer, fd, fileSize }
}

/**
 * Read footer metadata from a Parquet file on disk
 *
 * @param filePath - Path to the Parquet file
 * @returns Parsed footer metadata
 */
export function readParquetFooterFromFile(filePath: string): ParquetFileMetadata {
  const { footerBuffer, fd } = readFooterBuffer(filePath)
  try {
    return parseParquetFooter(footerBuffer)
  } finally {
    closeSync(fd)
  }
}

/**
 * Read page-level metadata from a Parquet file on disk
 *
 * @param filePath - Path to the Parquet file
 * @returns Complete page-level metadata including footer and page information
 */
export async function readParquetPagesFromFile(filePath: string): Promise<ParquetPageMetadata> {
  const { footerBuffer, fd } = readFooterBuffer(filePath)

  try {
    // Create a byte range reader using the open file descriptor
    const byteRangeReader = async (offset: number, length: number): Promise<ArrayBuffer> => {
      return readByteRange(fd, offset, length)
    }

    // Use core parsing logic
    return await parseParquetPages(footerBuffer, byteRangeReader)
  } finally {
    closeSync(fd)
  }
}
