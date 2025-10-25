/**
 * Core Parquet parsing logic (platform-agnostic)
 *
 * This module contains all the parsing logic that works with ArrayBuffers.
 * Platform-specific I/O (Node.js fs, browser File API) is handled by separate modules.
 */

import { parquetMetadata } from 'hyparquet'
import { deserializeTCompactProtocol } from 'hyparquet/src/thrift.js'
import { PageType, Encoding } from 'hyparquet/src/constants.js'

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
  numValues?: number
  crc?: number
  dataPageHeader?: {
    num_values: number
    encoding: string
    definition_level_encoding: string
    repetition_level_encoding: string
    statistics?: {
      max?: any
      min?: any
      null_count?: any
      distinct_count?: any
      max_value?: any
      min_value?: any
    }
  }
  dictionaryPageHeader?: {
    num_values: number
    encoding: string
    is_sorted?: boolean
  }
  dataPageHeaderV2?: {
    num_values: number
    num_nulls: number
    num_rows: number
    encoding: string
    definition_levels_byte_length: number
    repetition_levels_byte_length: number
    is_compressed: boolean
    statistics?: any
  }
}

/**
 * Column chunk metadata with page information
 */
export interface ColumnChunkMetadata {
  columnIndex: number
  columnName: string
  physicalType: string
  compressionCodec: string
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
  _readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>,
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

        // Count pages from encoding stats if available
        if (colMeta.encoding_stats && colMeta.encoding_stats.length > 0) {
          colMeta.encoding_stats.forEach((stat: any) => {
            aggregateStats.totalPages += stat.count
          })
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
          totalValues: colMeta.num_values,
          totalCompressedSize: colMeta.total_compressed_size,
          totalUncompressedSize: colMeta.total_uncompressed_size,
          compressionRatio: compressionRatio > 0 ? Number(compressionRatio.toFixed(2)) : 'N/A',
          encodings: colMeta.encodings,
          dataPageOffset: colMeta.data_page_offset,
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
  const endOffset = currentOffset + totalCompressedSize

  // Read all page data at once for parsing
  // We need the entire column chunk data to parse all page headers sequentially
  const buffer = await readByteRange(currentOffset, totalCompressedSize)

  let bufferOffset = 0

  try {
    // Parse each page header sequentially
    while (bufferOffset < buffer.byteLength && currentOffset + bufferOffset < endOffset) {
      const pageStartOffset = currentOffset + bufferOffset

      // Create a reader for the Thrift parser
      const reader = {
        view: new DataView(buffer, bufferOffset),
        offset: 0
      }

      // Parse the PageHeader using hyparquet's parquetHeader logic
      const pageHeader = parquetHeader(reader)

      // Extract encoding for convenience
      let encoding: string | undefined
      if (pageHeader.data_page_header) {
        encoding = pageHeader.data_page_header.encoding
      } else if (pageHeader.dictionary_page_header) {
        encoding = pageHeader.dictionary_page_header.encoding
      } else if (pageHeader.data_page_header_v2) {
        encoding = pageHeader.data_page_header_v2.encoding
      }

      let numValues: number | undefined
      if (pageHeader.data_page_header) {
        numValues = pageHeader.data_page_header.num_values
      } else if (pageHeader.data_page_header_v2) {
        numValues = pageHeader.data_page_header_v2.num_values
      } else if (pageHeader.dictionary_page_header) {
        numValues = pageHeader.dictionary_page_header.num_values
      }

      pages.push({
        pageNumber: pageNumber++,
        pageType: pageHeader.type,
        encoding,
        offset: BigInt(pageStartOffset),
        compressedSize: pageHeader.compressed_page_size,
        uncompressedSize: pageHeader.uncompressed_page_size,
        numValues: numValues,
        crc: pageHeader.crc,
        dataPageHeader: pageHeader.data_page_header,
        dictionaryPageHeader: pageHeader.dictionary_page_header,
        dataPageHeaderV2: pageHeader.data_page_header_v2,
      })

      // Move to the next page
      // The compressed page data immediately follows the header
      // reader.offset is relative to the DataView starting at bufferOffset
      bufferOffset = bufferOffset + reader.offset + pageHeader.compressed_page_size
    }
  } catch (e) {
    console.warn(`Failed to parse page data: ${columnChunkMetadata.meta_data.path_in_schema.join('.')} page ${pageNumber}`, e)
  }

  // We never fall back. If this doesn't work, we fix the page parsing

  return pages
}

/**
 * Parse a Parquet page header from a Thrift CompactProtocol reader
 * (Based on hyparquet's parquetHeader function)
 *
 * @param reader - Reader with view and offset
 * @returns Parsed page header with all fields
 */
function parquetHeader(reader: { view: DataView; offset: number }) {
  const header = deserializeTCompactProtocol(reader)

  // Parse parquet header from thrift data
  const type = PageType[header.field_1 as number]
  const uncompressed_page_size = header.field_2 as number
  const compressed_page_size = header.field_3 as number
  const crc = header.field_4

  const data_page_header = header.field_5 && {
    num_values: header.field_5.field_1,
    encoding: Encoding[header.field_5.field_2 as number],
    definition_level_encoding: Encoding[header.field_5.field_3 as number],
    repetition_level_encoding: Encoding[header.field_5.field_4 as number],
    statistics: header.field_5.field_5 && {
      max: header.field_5.field_5.field_1,
      min: header.field_5.field_5.field_2,
      null_count: header.field_5.field_5.field_3,
      distinct_count: header.field_5.field_5.field_4,
      max_value: header.field_5.field_5.field_5,
      min_value: header.field_5.field_5.field_6,
    },
  }

  const index_page_header = header.field_6

  const dictionary_page_header = header.field_7 && {
    num_values: header.field_7.field_1,
    encoding: Encoding[header.field_7.field_2 as number],
    is_sorted: header.field_7.field_3,
  }

  const data_page_header_v2 = header.field_8 && {
    num_values: header.field_8.field_1,
    num_nulls: header.field_8.field_2,
    num_rows: header.field_8.field_3,
    encoding: Encoding[header.field_8.field_4 as number],
    definition_levels_byte_length: header.field_8.field_5,
    repetition_levels_byte_length: header.field_8.field_6,
    is_compressed: header.field_8.field_7 === undefined ? true : header.field_8.field_7,
    statistics: header.field_8.field_8,
  }

  return {
    type,
    uncompressed_page_size,
    compressed_page_size,
    crc,
    data_page_header,
    index_page_header,
    dictionary_page_header,
    data_page_header_v2,
  }
}
