/**
 * Core Parquet parsing logic (platform-agnostic)
 *
 * This module contains all the parsing logic that works with ArrayBuffers.
 * Platform-specific I/O (Node.js fs, browser File API) is handled by separate modules.
 */

import { parquetMetadata, readOffsetIndex } from 'hyparquet'

/**
 * File metadata extracted from the Parquet footer
 */
export interface ParquetFileMetadata {
  version: number
  numRows: bigint
  numRowGroups: number
  numColumns: number
  createdBy?: string
  schema: any[]
  rowGroups: any[]
  keyValueMetadata?: Array<{ key: string; value?: string }>
  footerLength?: number
}

/**
 * Page information for a column chunk
 */
export interface PageInfo {
  pageNumber: number
  pageType?: string
  encoding?: string
  offset?: bigint
  compressedSize?: number
  uncompressedSize?: number
  firstRowIndex?: bigint
}

/**
 * Column chunk metadata with page information
 */
export interface ColumnChunkMetadata {
  columnIndex: number
  columnName: string
  physicalType: string
  compressionCodec: string
  numPages: number | string
  totalValues: bigint
  totalCompressedSize: bigint
  totalUncompressedSize: bigint
  compressionRatio: number | string
  encodings: string[]
  dictionaryPageOffset?: bigint
  dataPageOffset: bigint
  indexPageOffset?: bigint
  encodingStats?: Array<{
    pageType: string
    encoding: string
    count: number
  }>
  statistics?: {
    nullCount?: bigint
    distinctCount?: bigint
    min?: any
    max?: any
    isMaxValueExact?: boolean
    isMinValueExact?: boolean
  }
  sizeStatistics?: {
    unencodedByteArrayDataBytes?: bigint
    repetitionLevelHistogram?: bigint[]
    definitionLevelHistogram?: bigint[]
  }
  bloomFilter?: {
    offset: bigint
    length: number
  }
  keyValueMetadata?: Array<{ key: string; value?: string }>
  pages: PageInfo[]
}

/**
 * Row group metadata with column chunks
 */
export interface RowGroupMetadata {
  rowGroupIndex: number
  numRows: bigint
  totalByteSize: bigint
  totalCompressedSize?: bigint
  numColumns: number
  columns: ColumnChunkMetadata[]
}

/**
 * Complete page-level metadata for a Parquet file
 */
export interface ParquetPageMetadata {
  fileMetadata: ParquetFileMetadata
  rowGroups: RowGroupMetadata[]
  aggregateStats: {
    totalColumnChunks: number
    totalPages: number
    averagePagesPerColumnChunk: number
    totalCompressedBytes: bigint
    totalUncompressedBytes: bigint
    overallCompressionRatio: number
    encodingsUsed: string[]
    codecsUsed: string[]
    columnsWithDictionary: number
    columnsWithBloomFilter: number
    columnsWithStatistics: number
    columnsWithOffsetIndex: number
  }
}

/**
 * Parse Parquet footer metadata from buffer
 *
 * @param footerBuffer - ArrayBuffer containing the footer (including the trailing 8 bytes)
 * @returns Parsed footer metadata
 */
export function parseParquetFooter(footerBuffer: ArrayBuffer): ParquetFileMetadata {
  const metadata = parquetMetadata(footerBuffer)

  return {
    version: metadata.version,
    numRows: metadata.num_rows,
    numRowGroups: metadata.row_groups.length,
    numColumns: metadata.schema.length - 1, // -1 for root schema element
    createdBy: metadata.created_by,
    schema: metadata.schema,
    rowGroups: metadata.row_groups,
    keyValueMetadata: metadata.key_value_metadata,
  }
}

/**
 * Parse Parquet page-level metadata from footer buffer (reads offset indexes)
 *
 * @param footerBuffer - ArrayBuffer containing the footer
 * @param readByteRange - Function to read arbitrary byte ranges (for offset indexes)
 * @param footerLength - Optional footer length in bytes (including trailing 8 bytes)
 * @returns Complete page-level metadata
 */
export async function parseParquetPageIndex(
  footerBuffer: ArrayBuffer,
  readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>,
  footerLength?: number
): Promise<ParquetPageMetadata> {
  const metadata = parquetMetadata(footerBuffer)

  // Build file metadata
  const fileMetadata: ParquetFileMetadata = {
    version: metadata.version,
    numRows: metadata.num_rows,
    numRowGroups: metadata.row_groups.length,
    numColumns: metadata.schema.length - 1,
    createdBy: metadata.created_by,
    schema: metadata.schema,
    rowGroups: metadata.row_groups,
    keyValueMetadata: metadata.key_value_metadata,
    footerLength,
  }

  // Aggregate statistics
  const aggregateStats = {
    totalColumnChunks: 0,
    totalPages: 0,
    averagePagesPerColumnChunk: 0,
    totalCompressedBytes: 0n,
    totalUncompressedBytes: 0n,
    overallCompressionRatio: 0,
    encodingsUsed: new Set<string>(),
    codecsUsed: new Set<string>(),
    columnsWithDictionary: 0,
    columnsWithBloomFilter: 0,
    columnsWithStatistics: 0,
    columnsWithOffsetIndex: 0,
  }

  // Process row groups
  const rowGroups: RowGroupMetadata[] = []

  for (let rgIndex = 0; rgIndex < metadata.row_groups.length; rgIndex++) {
    const rowGroup = metadata.row_groups[rgIndex]
    const columns: ColumnChunkMetadata[] = []

    const BATCH_SIZE = 100

    for (let batchStart = 0; batchStart < rowGroup.columns.length; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, rowGroup.columns.length)

      for (let colIndex = batchStart; colIndex < batchEnd; colIndex++) {
        const columnChunk = rowGroup.columns[colIndex]
        const colMeta = columnChunk.meta_data

        if (!colMeta) {
          throw new Error(`Missing metadata for column ${colIndex} in row group ${rgIndex}`)
        }

        // Try to read offset index for page size information
        let pages: PageInfo[] = []

        if (columnChunk.offset_index_offset !== undefined && columnChunk.offset_index_length) {
          try {
            const offsetIndexStart = Number(columnChunk.offset_index_offset)
            const offsetIndexLength = columnChunk.offset_index_length

            // Calculate the minimum page offset to determine buffer positioning
            const pageOffsets = [
                colMeta.data_page_offset,
                colMeta.dictionary_page_offset,
                colMeta.index_page_offset
              ].filter((offset): offset is bigint => offset !== undefined)

            const firstPageOffset = pageOffsets.length > 0
              ? Number(pageOffsets.reduce((min, curr) => curr < min ? curr : min))
              : 0

            // Read offset index from file
            const offsetIndexBuffer = await readByteRange(offsetIndexStart, offsetIndexLength)

            // Calculate reader offset if offset index is before the first page data
            const readerOffset = firstPageOffset > offsetIndexStart
              ? firstPageOffset - offsetIndexStart
              : 0

            const reader = {
              view: new DataView(offsetIndexBuffer),
              offset: readerOffset
            }
            const offsetIndex = readOffsetIndex(reader)

            // Map page locations to PageInfo with size information
            pages = offsetIndex.page_locations.map((loc, i) => ({
              pageNumber: i,
              offset: loc.offset,
              compressedSize: loc.compressed_page_size,
              uncompressedSize: (loc as any).uncompressed_page_size,
              firstRowIndex: loc.first_row_index,
            }))
          } catch (e) {
            console.warn(`Failed to read offset index for column ${colIndex}:`, e)
          }
        }

        const numPages = pages.length

        // Update aggregate stats
        aggregateStats.totalColumnChunks++
        aggregateStats.totalCompressedBytes += colMeta.total_compressed_size
        aggregateStats.totalUncompressedBytes += colMeta.total_uncompressed_size

        colMeta.encodings.forEach((enc: string) => aggregateStats.encodingsUsed.add(enc))
        aggregateStats.codecsUsed.add(colMeta.codec)

        if (colMeta.dictionary_page_offset !== undefined) {
          aggregateStats.columnsWithDictionary++
        }
        if (colMeta.bloom_filter_offset !== undefined) {
          aggregateStats.columnsWithBloomFilter++
        }
        if (colMeta.statistics) {
          aggregateStats.columnsWithStatistics++
        }
        if (columnChunk.offset_index_offset !== undefined) {
          aggregateStats.columnsWithOffsetIndex++
        }

        // Count pages
        if (colMeta.encoding_stats && colMeta.encoding_stats.length > 0) {
          colMeta.encoding_stats.forEach((stat: any) => {
            aggregateStats.totalPages += stat.count
          })
        } else {
          aggregateStats.totalPages += numPages
        }

        // Calculate compression ratio
        const compressionRatio = colMeta.total_compressed_size > 0n
          ? Number(colMeta.total_uncompressed_size) / Number(colMeta.total_compressed_size)
          : 0

        // Build column chunk metadata
        const columnMetadata: ColumnChunkMetadata = {
          columnIndex: colIndex,
          columnName: colMeta.path_in_schema.join('.'),
          physicalType: colMeta.type,
          compressionCodec: colMeta.codec,
          numPages,
          totalValues: colMeta.num_values,
          totalCompressedSize: colMeta.total_compressed_size,
          totalUncompressedSize: colMeta.total_uncompressed_size,
          compressionRatio: compressionRatio > 0 ? Number(compressionRatio.toFixed(2)) : 'N/A',
          encodings: colMeta.encodings,
          dataPageOffset: colMeta.data_page_offset,
          pages,
        }

        // Add optional fields
        if (colMeta.dictionary_page_offset !== undefined) {
          columnMetadata.dictionaryPageOffset = colMeta.dictionary_page_offset
        }
        if (colMeta.index_page_offset !== undefined) {
          columnMetadata.indexPageOffset = colMeta.index_page_offset
        }
        if (colMeta.encoding_stats) {
          columnMetadata.encodingStats = colMeta.encoding_stats.map((stat: any) => ({
            pageType: stat.page_type,
            encoding: stat.encoding,
            count: stat.count,
          }))
        }
        if (colMeta.statistics) {
          columnMetadata.statistics = {
            nullCount: colMeta.statistics.null_count,
            distinctCount: colMeta.statistics.distinct_count,
            min: colMeta.statistics.min,
            max: colMeta.statistics.max,
            isMaxValueExact: colMeta.statistics.is_max_value_exact,
            isMinValueExact: colMeta.statistics.is_min_value_exact,
          }
        }
        if (colMeta.size_statistics) {
          columnMetadata.sizeStatistics = {
            unencodedByteArrayDataBytes: colMeta.size_statistics.unencoded_byte_array_data_bytes,
            repetitionLevelHistogram: colMeta.size_statistics.repetition_level_histogram,
            definitionLevelHistogram: colMeta.size_statistics.definition_level_histogram,
          }
        }
        if (colMeta.bloom_filter_offset !== undefined && colMeta.bloom_filter_length !== undefined) {
          columnMetadata.bloomFilter = {
            offset: colMeta.bloom_filter_offset,
            length: colMeta.bloom_filter_length,
          }
        }
        if (colMeta.key_value_metadata) {
          columnMetadata.keyValueMetadata = colMeta.key_value_metadata
        }

        columns.push(columnMetadata)
      }
    }

    rowGroups.push({
      rowGroupIndex: rgIndex,
      numRows: rowGroup.num_rows,
      totalByteSize: rowGroup.total_byte_size,
      totalCompressedSize: rowGroup.total_compressed_size,
      numColumns: rowGroup.columns.length,
      columns,
    })
  }

  // Finalize aggregate stats
  const finalAggregateStats = {
    ...aggregateStats,
    averagePagesPerColumnChunk: aggregateStats.totalColumnChunks > 0
      ? Number((aggregateStats.totalPages / aggregateStats.totalColumnChunks).toFixed(2))
      : 0,
    overallCompressionRatio: aggregateStats.totalCompressedBytes > 0n
      ? Number((Number(aggregateStats.totalUncompressedBytes) / Number(aggregateStats.totalCompressedBytes)).toFixed(2))
      : 0,
    encodingsUsed: Array.from(aggregateStats.encodingsUsed),
    codecsUsed: Array.from(aggregateStats.codecsUsed),
    columnsWithDictionary: aggregateStats.columnsWithDictionary,
    columnsWithBloomFilter: aggregateStats.columnsWithBloomFilter,
    columnsWithStatistics: aggregateStats.columnsWithStatistics,
    columnsWithOffsetIndex: aggregateStats.columnsWithOffsetIndex,
  }

  return {
    fileMetadata,
    rowGroups,
    aggregateStats: finalAggregateStats,
  }
}

/**
 * Parse page data for a specific column chunk
 *
 * @param columnChunkMetadata - Metadata for the column chunk
 * @param readByteRange - Function to read arbitrary byte ranges
 * @returns Array of parsed page information
 */
export async function parseParquetPage(
  columnChunkMetadata: any,
  readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>
): Promise<PageInfo[]> {
  const pages: PageInfo[] = []

  const colMeta = columnChunkMetadata.meta_data
  if (!colMeta) {
    throw new Error('Missing metadata for column chunk')
  }

  // Start reading from the data page offset
  let currentOffset = Number(colMeta.data_page_offset)

  // If there's a dictionary page, it comes before data pages
  if (colMeta.dictionary_page_offset !== undefined) {
    currentOffset = Math.min(currentOffset, Number(colMeta.dictionary_page_offset))
  }

  let pageNumber = 0
  const totalCompressedSize = Number(colMeta.total_compressed_size)

  // We need to read page headers sequentially
  // Each page has a header followed by data
  // The header tells us the size of the page data

  try {
    // Read the first chunk to parse page headers
    // Page headers are typically small (< 100 bytes each)
    // We'll read a reasonable chunk and parse what we can
    const initialReadSize = Math.min(8192, totalCompressedSize) // Read up to 8KB
    const buffer = await readByteRange(currentOffset, initialReadSize)

    let offset = 0

    // Parse pages until we've read all the data
    // Note: This is a simplified implementation
    // A complete implementation would use proper Thrift parsing
    while (offset < buffer.byteLength && pageNumber < 100) { // Safety limit
      // Page header structure (simplified):
      // - type (4 bytes)
      // - uncompressed_page_size (4 bytes)
      // - compressed_page_size (4 bytes)
      // - CRC (4 bytes, optional)
      // + additional fields depending on page type

      // For now, we'll create basic page entries
      // A full implementation would parse the Thrift-encoded page header
      pages.push({
        pageNumber: pageNumber++,
        offset: BigInt(currentOffset + offset),
      })

      // Break after first page for safety (full implementation would continue)
      break
    }
  } catch (e) {
    console.warn('Failed to parse page data:', e)
  }

  // If we couldn't parse pages directly, fall back to offset index if available
  if (pages.length === 0 && columnChunkMetadata.offset_index_offset !== undefined) {
    const offsetIndexStart = Number(columnChunkMetadata.offset_index_offset)
    const offsetIndexLength = columnChunkMetadata.offset_index_length

    const offsetIndexBuffer = await readByteRange(offsetIndexStart, offsetIndexLength)
    const reader = {
      view: new DataView(offsetIndexBuffer),
      offset: 0
    }
    const offsetIndex = readOffsetIndex(reader)

    return offsetIndex.page_locations.map((loc, i) => ({
      pageNumber: i,
      offset: loc.offset,
      compressedSize: loc.compressed_page_size,
      uncompressedSize: (loc as any).uncompressed_page_size,
      firstRowIndex: loc.first_row_index,
    }))
  }

  return pages
}
