/**
 * Core Parquet parsing logic (platform-agnostic)
 *
 * This module contains all the parsing logic that works with ArrayBuffers.
 * Platform-specific I/O (Node.js fs, browser File API) is handled by separate modules.
 */

import {parquetMetadata, RowGroup, snappyUncompress} from 'hyparquet'
import { deserializeTCompactProtocol } from 'hyparquet/src/thrift.js'
import { PageType, Encoding } from 'hyparquet/src/constants.js'
import { decompress as zstdDecompress } from 'fzstd'
import { SchemaElement } from 'hyparquet/src/types.js'

/**
 * File metadata extracted from the Parquet footer
 */
export interface ParquetFileMetadata {
  version: number
  numRows: bigint
  numRowGroups: number
  numColumns: number
  createdBy?: string
  schema: SchemaElement[]
  rowGroups: RowGroup[]
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
  headerSize?: number
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
 * Size breakdown of a page's components
 */
export interface PageSizeBreakdown {
  pageNumber: number
  pageType: string
  headerSize: number
  repetitionLevelsSize: number
  definitionLevelsSize: number
  valuesSize: number
  totalDataSize: number
  nullCount?: number
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
 * Count the number of leaf columns in a Parquet schema
 * Leaf columns are the actual data columns (not intermediate struct/list containers)
 *
 * @param schema - Parquet schema array
 * @returns Number of leaf columns
 */
function countLeafColumns(schema: any[]): number {
  if (!schema || schema.length === 0) return 0

  let leafCount = 0

  for (const element of schema) {
    // Skip the root element
    if (!element.name) continue

    // A leaf column is one that has no children
    // num_children is undefined/null/0 for leaf columns
    const hasChildren = element.num_children && element.num_children > 0

    if (!hasChildren) {
      leafCount++
    }
  }

  return leafCount
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
    numColumns: countLeafColumns(metadata.schema),
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
    numColumns: countLeafColumns(metadata.schema),
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
  if (colMeta.data_page_offset > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Data page offset ${colMeta.data_page_offset} exceeds JavaScript's safe integer limit`)
  }
  let currentOffset = Number(colMeta.data_page_offset)

  // If there's a dictionary page, it comes before data pages
  if (colMeta.dictionary_page_offset !== undefined) {
    if (colMeta.dictionary_page_offset > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Dictionary page offset ${colMeta.dictionary_page_offset} exceeds JavaScript's safe integer limit`)
    }
    currentOffset = Math.min(currentOffset, Number(colMeta.dictionary_page_offset))
  }

  let pageNumber = 0
  // Convert bigint to number - JavaScript number can safely represent integers up to 2^53-1 (~8 PB)
  // This is well above practical parquet column chunk sizes
  if (colMeta.total_compressed_size > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Column chunk size ${colMeta.total_compressed_size} exceeds JavaScript's safe integer limit`)
  }
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

      // Capture the header size (reader.offset now contains bytes read for the header)
      const headerSize = reader.offset

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
        headerSize: headerSize,
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

/**
 * Read a variable-length integer (varint)
 * Used in RLE/bit-packed hybrid encoding
 *
 * @param view - DataView
 * @param offset - Current offset (will be modified)
 * @returns Object with value and new offset
 */
function readVarInt(view: DataView, offset: number): { value: number; offset: number } {
  let value = 0
  let shift = 0
  let byte = 0

  do {
    byte = view.getUint8(offset++)
    value |= (byte & 0x7f) << shift
    shift += 7
  } while (byte & 0x80)

  return { value, offset }
}

/**
 * Decode RLE/bit-packed hybrid definition levels
 * Returns array of definition level values
 *
 * @param view - DataView of the page data
 * @param offset - Starting offset (after length prefix)
 * @param length - Length of encoded data in bytes
 * @param bitWidth - Bit width for values
 * @param numValues - Number of values to decode
 * @returns Array of definition level values
 */
function decodeRleBitPackedLevels(
  view: DataView,
  offset: number,
  length: number,
  bitWidth: number,
  numValues: number
): number[] {
  const result: number[] = []
  const endOffset = offset + length
  let currentOffset = offset

  while (result.length < numValues && currentOffset < endOffset) {
    // Read header
    const { value: header, offset: newOffset } = readVarInt(view, currentOffset)
    currentOffset = newOffset

    if (header & 1) {
      // Bit-packed run
      const count = (header >> 1) << 3 // number of values
      const mask = (1 << bitWidth) - 1

      // Read bit-packed values
      let data = 0
      let bitsInData = 0

      for (let i = 0; i < count && result.length < numValues; i++) {
        // Load more bits if needed
        while (bitsInData < bitWidth && currentOffset < endOffset) {
          data |= view.getUint8(currentOffset++) << bitsInData
          bitsInData += 8
        }

        // Extract value
        const value = data & mask
        result.push(value)

        // Shift out used bits
        data >>>= bitWidth
        bitsInData -= bitWidth
      }
    } else {
      // RLE run
      const count = header >>> 1
      const byteWidth = Math.ceil(bitWidth / 8)

      // Read value
      let value = 0
      for (let i = 0; i < byteWidth && currentOffset < endOffset; i++) {
        value |= view.getUint8(currentOffset++) << (i * 8)
      }

      // Repeat value count times
      for (let i = 0; i < count && result.length < numValues; i++) {
        result.push(value)
      }
    }
  }

  return result
}

/**
 * Count nulls from definition levels
 * Values with definition level < maxDefinitionLevel are null/missing
 *
 * @param definitionLevels - Array of definition level values
 * @param maxDefinitionLevel - Maximum definition level
 * @returns Number of null values
 */
function countNullsFromDefinitionLevels(
  definitionLevels: number[],
  maxDefinitionLevel: number
): number {
  let nullCount = 0
  for (const level of definitionLevels) {
    if (level < maxDefinitionLevel) {
      nullCount++
    }
  }
  return nullCount
}

/**
 * Read the size of RLE/Bit-packed encoded data
 * Used for reading repetition and definition levels in Data Page V1
 *
 * @param view - DataView of the page data
 * @param offset - Starting offset
 * @param maxLevel - Maximum level value (determines bit width)
 * @param _numValues - Number of values to read (reserved for future use)
 * @returns Number of bytes consumed
 */
function readRleBitPackedLevelSize(
  view: DataView,
  offset: number,
  maxLevel: number,
  _numValues: number
): number {
  if (maxLevel === 0) {
    // No levels needed
    return 0
  }

  // NOTE: For full RLE/bit-packed decoding, we would calculate:
  // const bitWidth = Math.ceil(Math.log2(maxLevel + 1))
  // But for size calculation, we only need to read the length prefix

  // First 4 bytes contain the total byte length for levels in Data Page V1
  // This is a length-prefixed encoding
  const totalLength = view.getUint32(offset, true) // little-endian

  return 4 + totalLength
}

/**
 * Decompress page data based on compression codec
 *
 * @param compressedData - Compressed page data
 * @param uncompressedSize - Expected uncompressed size
 * @param codec - Compression codec used
 * @returns Uncompressed data as Uint8Array
 */
export function decompressPageData(
  compressedData: ArrayBuffer,
  uncompressedSize: number,
  codec: string
): Uint8Array {
  const compressedBytes = new Uint8Array(compressedData)

  // No decompression needed
  if (codec === 'UNCOMPRESSED') {
    return compressedBytes
  }

  // SNAPPY decompression using hyparquet's built-in decompressor
  if (codec === 'SNAPPY') {
    const outputBuffer = new Uint8Array(uncompressedSize)
    snappyUncompress(compressedBytes, outputBuffer)
    return outputBuffer
  }

  // ZSTD decompression using fzstd library
  if (codec === 'ZSTD') {
    return zstdDecompress(compressedBytes)
  }

  // For other codecs, we would need additional libraries
  // For now, throw an error indicating unsupported codec
  throw new Error(`Decompression for codec ${codec} is not yet supported. Supported codecs: UNCOMPRESSED, SNAPPY, ZSTD`)
}

/**
 * Calculate maximum repetition and definition levels for a column from schema
 *
 * @param schema - Complete Parquet schema array
 * @param columnPath - Path to the column (e.g., ['user', 'name'])
 * @returns Object with maxRepetitionLevel and maxDefinitionLevel
 */
export function calculateMaxLevels(
  schema: any[],
  columnPath: string[]
): { maxRepetitionLevel: number; maxDefinitionLevel: number } {
  let maxRepetitionLevel = 0
  let maxDefinitionLevel = 0

  // Traverse the schema following the column path
  let currentPath: string[] = []

  for (let i = 1; i < schema.length; i++) {
    const schemaElement = schema[i]
    const elementPath = schemaElement.name ? [...currentPath, schemaElement.name] : currentPath

    // Check if this element is on the path to our column
    const isOnPath = columnPath.length >= elementPath.length &&
      columnPath.slice(0, elementPath.length).every((p, idx) => p === elementPath[idx])

    if (isOnPath || elementPath.length === 0) {
      // Check repetition type
      if (schemaElement.repetition_type === 'REPEATED') {
        maxRepetitionLevel++
      }

      // Check if field is optional (increases definition level)
      if (schemaElement.repetition_type === 'OPTIONAL') {
        maxDefinitionLevel++
      }

      // If field is repeated, it also affects definition level
      if (schemaElement.repetition_type === 'REPEATED') {
        maxDefinitionLevel++
      }

      // If this is our target column, we're done
      if (elementPath.length === columnPath.length &&
          elementPath.every((p, idx) => p === columnPath[idx])) {
        break
      }

      currentPath = elementPath
    }
  }

  return { maxRepetitionLevel, maxDefinitionLevel }
}

/**
 * Parse page data and extract size breakdown of components
 *
 * This function analyzes the uncompressed page data to determine the size
 * of repetition levels, definition levels, and values within a page.
 *
 * @param pageInfo - Page metadata from parseParquetPage
 * @param uncompressedPageData - Uncompressed page data (excluding header)
 * @param maxRepetitionLevel - Maximum repetition level for the column (0 for non-nested)
 * @param maxDefinitionLevel - Maximum definition level for the column
 * @returns Size breakdown of page components
 */
export function parsePageDataSizes(
  pageInfo: PageInfo,
  uncompressedPageData: ArrayBuffer | Uint8Array,
  maxRepetitionLevel: number,
  maxDefinitionLevel: number
): PageSizeBreakdown {
  // Convert to ArrayBuffer if needed
  const buffer = uncompressedPageData instanceof Uint8Array
    ? uncompressedPageData.buffer
    : uncompressedPageData
  const byteLength = uncompressedPageData instanceof Uint8Array
    ? uncompressedPageData.byteLength
    : uncompressedPageData.byteLength
  const view = new DataView(buffer)
  let offset = 0

  let repetitionLevelsSize = 0
  let definitionLevelsSize = 0
  let valuesSize = 0
  let nullCount: number | undefined

  const pageType = pageInfo.pageType || 'UNKNOWN'

  // Handle Data Page V2
  if (pageInfo.dataPageHeaderV2) {
    const header = pageInfo.dataPageHeaderV2

    // V2 pages have the byte lengths in the header
    repetitionLevelsSize = header.repetition_levels_byte_length
    definitionLevelsSize = header.definition_levels_byte_length

    // Values size is the remaining data
    valuesSize = byteLength - repetitionLevelsSize - definitionLevelsSize

    // Extract null count from V2 header if available
    if (header.num_nulls !== undefined && header.num_nulls !== null) {
      nullCount = header.num_nulls
    }
  }
  // Handle Data Page V1
  else if (pageInfo.dataPageHeader) {
    const header = pageInfo.dataPageHeader
    const numValues = header.num_values

    // Store the start of definition levels for potential decoding
    let definitionLevelsOffset = offset

    // Read repetition levels (if any)
    if (maxRepetitionLevel > 0) {
      repetitionLevelsSize = readRleBitPackedLevelSize(
        view,
        offset,
        maxRepetitionLevel,
        numValues
      )
      offset += repetitionLevelsSize
      definitionLevelsOffset = offset
    }

    // Read definition levels (if any)
    if (maxDefinitionLevel > 0) {
      definitionLevelsSize = readRleBitPackedLevelSize(
        view,
        offset,
        maxDefinitionLevel,
        numValues
      )
      offset += definitionLevelsSize

      // If definition levels are small (<1kB), decode them to count nulls
      if (definitionLevelsSize > 0 && definitionLevelsSize < 1024) {
        try {
          // The size includes the 4-byte length prefix
          const levelDataLength = view.getUint32(definitionLevelsOffset, true)
          const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1))

          // Decode definition levels
          const definitionLevels = decodeRleBitPackedLevels(
            view,
            definitionLevelsOffset + 4, // skip length prefix
            levelDataLength,
            bitWidth,
            numValues
          )

          // Count nulls (values with definition level < maxDefinitionLevel)
          nullCount = countNullsFromDefinitionLevels(definitionLevels, maxDefinitionLevel)
        } catch (error) {
          // If decoding fails, just skip null counting
          console.warn('Failed to decode definition levels for null counting:', error)
        }
      }
    }

    // Remaining data is values
    valuesSize = byteLength - offset
  }
  // Handle Dictionary Page (no levels)
  else if (pageInfo.dictionaryPageHeader) {
    // Dictionary pages don't have repetition or definition levels
    repetitionLevelsSize = 0
    definitionLevelsSize = 0
    valuesSize = byteLength
  }
  // Handle other page types
  else {
    // Unknown page type, treat all as values
    valuesSize = byteLength
  }

  const result: PageSizeBreakdown = {
    pageNumber: pageInfo.pageNumber,
    pageType,
    headerSize: pageInfo.headerSize || 0,
    repetitionLevelsSize,
    definitionLevelsSize,
    valuesSize,
    totalDataSize: byteLength,
  }

  // Add null count if available
  if (nullCount !== undefined) {
    result.nullCount = nullCount
  }

  return result
}

/**
 * Parse all pages in a column chunk and extract size breakdowns
 *
 * This function reads and decompresses all pages in a column chunk,
 * then analyzes each page to determine the size breakdown.
 *
 * @param columnChunkMetadata - Column chunk metadata
 * @param readByteRange - Function to read arbitrary byte ranges
 * @param maxRepetitionLevel - Maximum repetition level for the column
 * @param maxDefinitionLevel - Maximum definition level for the column
 * @returns Array of size breakdowns for each page
 */
export async function parseColumnChunkPageSizes(
  columnChunkMetadata: any,
  readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>,
  maxRepetitionLevel: number,
  maxDefinitionLevel: number
): Promise<PageSizeBreakdown[]> {
  // First, get the page headers
  const pages = await parseParquetPage(columnChunkMetadata, readByteRange)

  const pageSizes: PageSizeBreakdown[] = []

  const colMeta = columnChunkMetadata.meta_data
  if (!colMeta) {
    throw new Error('Missing metadata for column chunk')
  }

  const codec = colMeta.codec

  // For each page, read the data and calculate sizes
  for (const page of pages) {
    const pageOffset = Number(page.offset)
    const headerSize = page.headerSize || 0
    const compressedSize = page.compressedSize || 0
    const uncompressedSize = page.uncompressedSize || compressedSize

    // Read the compressed page data (after header)
    const compressedData = await readByteRange(
      pageOffset + headerSize,
      compressedSize
    )

    // Decompress the page data
    // For DATA_PAGE_V2, check the is_compressed flag
    let uncompressedBytes: Uint8Array
    if (page.dataPageHeaderV2 && page.dataPageHeaderV2.is_compressed === false) {
      // Data is already uncompressed
      uncompressedBytes = new Uint8Array(compressedData)
    } else {
      // Normal decompression based on codec
      uncompressedBytes = decompressPageData(
        compressedData,
        uncompressedSize,
        codec
      )
    }

    // Parse the page data sizes
    const sizeBreakdown = parsePageDataSizes(
      page,
      uncompressedBytes,
      maxRepetitionLevel,
      maxDefinitionLevel
    )

    pageSizes.push(sizeBreakdown)
  }

  return pageSizes
}
