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
 * Parse Parquet page-level metadata from footer buffer
 *
 * @param footerBuffer - ArrayBuffer containing the footer
 * @param readByteRange - Function to read arbitrary byte ranges (for offset indexes)
 * @returns Complete page-level metadata
 */
export async function parseParquetPages(
  footerBuffer: ArrayBuffer,
  readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>
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

            // Read offset index from file
            const offsetIndexBuffer = await readByteRange(offsetIndexStart, offsetIndexLength)
            const reader = {
              view: new DataView(offsetIndexBuffer),
              offset: 0
            }
            const offsetIndex = readOffsetIndex(reader)

            // Calculate compression ratio
            const compressionRatio = colMeta.total_compressed_size > 0n
              ? Number(colMeta.total_uncompressed_size) / Number(colMeta.total_compressed_size)
              : 1

            // Map page locations to PageInfo with size information
            pages = offsetIndex.page_locations.map((loc, i) => ({
              pageNumber: i,
              offset: loc.offset,
              compressedSize: loc.compressed_page_size,
              uncompressedSize: Math.round(loc.compressed_page_size * compressionRatio),
              firstRowIndex: loc.first_row_index,
            }))
          } catch (e) {
            console.warn(`Failed to read offset index for column ${colIndex}:`, e)
          }
        }

        // Fallback: use encoding stats if available
        if (pages.length === 0 && colMeta.encoding_stats && colMeta.encoding_stats.length > 0) {
          let pageNumber = 0
          colMeta.encoding_stats.forEach((stat: any) => {
            for (let i = 0; i < stat.count; i++) {
              pages.push({
                pageNumber: pageNumber++,
                pageType: stat.page_type,
                encoding: stat.encoding,
              })
            }
          })
        }

        // Final fallback
        if (pages.length === 0) {
          pages.push({
            pageNumber: 0,
            pageType: 'DATA_PAGE',
          })
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
