/**
 * Parquet file parsing library using hyparquet
 *
 * This module provides APIs for extracting metadata from Parquet files,
 * including footer metadata and page-level metadata.
 */

/**
 * Browser-compatible Parquet parsing library using hyparquet
 * For Node.js-specific functions (reading from file paths), use parquet-parsing-node.ts
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
 * Read page headers for a column chunk
 */
function readPageHeaders(fileBuffer: ArrayBuffer, columnChunk: any): PageInfo[] {
  const colMeta = columnChunk.meta_data
  if (!colMeta) return []

  const pages: PageInfo[] = []

  // Check if we have offset index (most reliable)
  if (columnChunk.offset_index_offset !== undefined && columnChunk.offset_index_length) {
    try {
      const offsetIndexStart = Number(columnChunk.offset_index_offset)
      const offsetIndexLength = columnChunk.offset_index_length
      const offsetIndexBuffer = fileBuffer.slice(offsetIndexStart, offsetIndexStart + offsetIndexLength)
      const reader = {
        view: new DataView(offsetIndexBuffer),
        offset: 0
      }
      const offsetIndex = readOffsetIndex(reader)

      // Calculate compression ratio for the column to estimate uncompressed size
      const compressionRatio = colMeta.total_compressed_size > 0n
        ? Number(colMeta.total_uncompressed_size) / Number(colMeta.total_compressed_size)
        : 1

      // Each page_location represents one page
      return offsetIndex.page_locations.map((loc, i) => ({
        pageNumber: i,
        offset: loc.offset,
        compressedSize: loc.compressed_page_size,
        uncompressedSize: Math.round(loc.compressed_page_size * compressionRatio),
        firstRowIndex: loc.first_row_index,
      }))
    } catch (e) {
      // Fall through to other methods
    }
  }

  // Fallback: use encoding stats if available
  if (colMeta.encoding_stats && colMeta.encoding_stats.length > 0) {
    let pageNumber = 0

    // Iterate through each encoding stat and create pages for it
    colMeta.encoding_stats.forEach((stat: any) => {
      for (let i = 0; i < stat.count; i++) {
        pages.push({
          pageNumber: pageNumber++,
          pageType: stat.page_type,
          encoding: stat.encoding,
        })
      }
    })

    return pages
  }

  // Final fallback: infer minimum page count from metadata
  let minPageCount = 1

  if (colMeta.dictionary_page_offset !== undefined) {
    pages.push({
      pageNumber: 0,
      pageType: 'DICTIONARY_PAGE',
    })
    minPageCount++
  }

  pages.push({
    pageNumber: minPageCount - 1,
    pageType: 'DATA_PAGE',
  })

  return pages
}


/**
 * Read footer metadata from a Parquet file
 *
 * @param fileBuffer - ArrayBuffer containing the entire Parquet file
 * @returns Structured footer metadata
 */
export function readParquetFooter(fileBuffer: ArrayBuffer): ParquetFileMetadata {
  const metadata = parquetMetadata(fileBuffer)

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
 * Read page-level metadata from a Parquet file (browser File object)
 *
 * @param file - File object from browser
 * @returns Complete page-level metadata including footer and page information
 */
export async function readParquetPagesFromFile(file: File): Promise<ParquetPageMetadata> {
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

  console.log('[parquet-parsing] Footer read successfully, parsing metadata')

  const metadata = parquetMetadata(footerBuffer)

  console.log('[parquet-parsing] Metadata parsed, processing row groups')

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
    footerLength: footerLength + 8,
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

        // Try to read offset index for page size information (critical for histogram!)
        let pages: PageInfo[] = []

        if (columnChunk.offset_index_offset !== undefined && columnChunk.offset_index_length) {
          try {
            const offsetIndexStart = Number(columnChunk.offset_index_offset)
            const offsetIndexLength = columnChunk.offset_index_length

            // Read offset index from file
            const offsetIndexSlice = file.slice(offsetIndexStart, offsetIndexStart + offsetIndexLength)
            const offsetIndexBuffer = await offsetIndexSlice.arrayBuffer()
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

  console.log('[parquet-parsing] Processing complete')

  return {
    fileMetadata,
    rowGroups,
    aggregateStats: finalAggregateStats,
  }
}

/**
 * Read page-level metadata from a Parquet file
 *
 * @param fileBuffer - ArrayBuffer containing the entire Parquet file
 * @returns Complete page-level metadata including footer and page information
 */
export function readParquetPages(fileBuffer: ArrayBuffer): ParquetPageMetadata {
  const metadata = parquetMetadata(fileBuffer)

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
  const rowGroups: RowGroupMetadata[] = metadata.row_groups.map((rowGroup: any, rgIndex: number) => {
    const columns: ColumnChunkMetadata[] = rowGroup.columns.map((columnChunk: any, colIndex: number) => {
      const colMeta = columnChunk.meta_data
      if (!colMeta) {
        throw new Error(`Missing metadata for column ${colIndex} in row group ${rgIndex}`)
      }

      // Read page information
      const pages = readPageHeaders(fileBuffer, columnChunk)
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

      // Count pages for aggregate
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
        columnMetadata.encodingStats = colMeta.encoding_stats
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
      if (colMeta.bloom_filter_offset !== undefined) {
        columnMetadata.bloomFilter = {
          offset: colMeta.bloom_filter_offset,
          length: colMeta.bloom_filter_length,
        }
      }
      if (colMeta.key_value_metadata) {
        columnMetadata.keyValueMetadata = colMeta.key_value_metadata
      }

      return columnMetadata
    })

    return {
      rowGroupIndex: rgIndex,
      numRows: rowGroup.num_rows,
      totalByteSize: rowGroup.total_byte_size,
      totalCompressedSize: rowGroup.total_compressed_size,
      numColumns: rowGroup.columns.length,
      columns,
    }
  })

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
