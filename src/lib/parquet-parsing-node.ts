/**
 * Node.js-specific Parquet parsing functions
 * This file contains functions that use Node.js APIs like fs and Buffer
 */

import { openSync, readSync, fstatSync, closeSync } from 'fs'
import { parquetMetadata, readOffsetIndex } from 'hyparquet'

// Re-export all browser-compatible types and functions
export * from './parquet-parsing.js'
export type {
  ParquetFileMetadata,
  PageInfo,
  ColumnChunkMetadata,
  RowGroupMetadata,
  ParquetPageMetadata
} from './parquet-parsing.js'

import type {
  ParquetFileMetadata,
  PageInfo,
  ColumnChunkMetadata,
  RowGroupMetadata,
  ParquetPageMetadata
} from './parquet-parsing.js'

/**
 * Read only the footer from a file on disk (more memory efficient)
 *
 * @param filePath - Path to the Parquet file
 * @returns Structured footer metadata
 */
export function readParquetFooterFromFile(filePath: string): ParquetFileMetadata {
  const fd = openSync(filePath, 'r')
  try {
    const fileStats = fstatSync(fd)
    const fileSize = fileStats.size

    // Read the last 8 bytes to get the footer size
    const footerLengthBuffer = Buffer.allocUnsafe(8)
    readSync(fd, footerLengthBuffer, 0, 8, fileSize - 8)

    // First 4 bytes are footer length, last 4 bytes are magic number "PAR1"
    const footerLength = footerLengthBuffer.readUInt32LE(0)

    // Read the footer + the 8 bytes we just read
    const footerBuffer = Buffer.allocUnsafe(footerLength + 8)
    readSync(fd, footerBuffer, 0, footerLength + 8, fileSize - footerLength - 8)

    const arrayBuffer = footerBuffer.buffer.slice(
      footerBuffer.byteOffset,
      footerBuffer.byteOffset + footerBuffer.byteLength
    )

    const metadata = parquetMetadata(arrayBuffer)

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
  } finally {
    closeSync(fd)
  }
}

/**
 * Helper function to read a specific byte range from a file
 */
function readFileRange(fd: number, start: number, length: number): ArrayBuffer {
  const buffer = Buffer.allocUnsafe(length)
  readSync(fd, buffer, 0, length, start)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Read page-level metadata from a Parquet file on disk (Node.js)
 *
 * @param filePath - Path to the Parquet file
 * @returns Complete page-level metadata including footer and page information
 */
export function readParquetPagesFromFile(filePath: string): ParquetPageMetadata {
  const fd = openSync(filePath, 'r')
  try {
    const fileStats = fstatSync(fd)
    const fileSize = fileStats.size

    // Read the footer to get metadata
    const footerLengthBuffer = Buffer.allocUnsafe(8)
    readSync(fd, footerLengthBuffer, 0, 8, fileSize - 8)
    const footerLength = footerLengthBuffer.readUInt32LE(0)

    const footerBuffer = Buffer.allocUnsafe(footerLength + 8)
    readSync(fd, footerBuffer, 0, footerLength + 8, fileSize - footerLength - 8)

    const footerArrayBuffer = footerBuffer.buffer.slice(
      footerBuffer.byteOffset,
      footerBuffer.byteOffset + footerBuffer.byteLength
    )

    const metadata = parquetMetadata(footerArrayBuffer)

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

    // Process row groups in a streaming fashion
    const rowGroups: RowGroupMetadata[] = []

    for (let rgIndex = 0; rgIndex < metadata.row_groups.length; rgIndex++) {
      const rowGroup = metadata.row_groups[rgIndex]
      const columns: ColumnChunkMetadata[] = []

      // Process columns in batches to avoid memory spikes
      const BATCH_SIZE = 100

      for (let batchStart = 0; batchStart < rowGroup.columns.length; batchStart += BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE, rowGroup.columns.length)

        for (let colIndex = batchStart; colIndex < batchEnd; colIndex++) {
          const columnChunk = rowGroup.columns[colIndex]
          const colMeta = columnChunk.meta_data

          if (!colMeta) {
            throw new Error(`Missing metadata for column ${colIndex} in row group ${rgIndex}`)
          }

          // Read offset index if available (only read the specific bytes needed)
          let pages: PageInfo[] = []
          // Calculate the minimum offset from all page offsets to determine start position
          const offsets = [
            colMeta.data_page_offset,
            colMeta.dictionary_page_offset,
            colMeta.index_page_offset
          ].filter(offset => offset !== undefined).map(offset => Number(offset));

          if (offsets.length === 0) {
            throw new Error(`Missing page offsets for column ${colIndex} in row group ${rgIndex}`)
          }
          const startOffsetIndex = Math.min(...offsets);

          if (columnChunk.offset_index_offset !== undefined && columnChunk.offset_index_length) {
              let offsetIndexStart = Number(columnChunk.offset_index_offset)
              let offsetIndexLength = columnChunk.offset_index_length
              console.log('offsetIndexStart', offsetIndexStart)
              console.log('startOffsetIndex', startOffsetIndex)
              if (offsetIndexStart < startOffsetIndex) {
                offsetIndexLength = offsetIndexLength - (startOffsetIndex - offsetIndexStart);
                offsetIndexStart = startOffsetIndex;
              }
              const offsetIndexBuffer = readFileRange(fd, offsetIndexStart, offsetIndexLength)

              const reader = {
                view: new DataView(offsetIndexBuffer),
                offset: 0
              }
              const offsetIndex = readOffsetIndex(reader)

              const compressionRatio = colMeta.total_compressed_size > 0n
                ? Number(colMeta.total_uncompressed_size) / Number(colMeta.total_compressed_size)
                : 1

              pages = offsetIndex.page_locations.map((loc, i) => ({
                pageNumber: i,
                offset: loc.offset,
                compressedSize: loc.compressed_page_size,
                uncompressedSize: Math.round(loc.compressed_page_size * compressionRatio),
                firstRowIndex: loc.first_row_index,
              }))
          }

          // If we still don't have pages, we can't determine page-level info
          // Don't create fake pages - just report that we don't have page details
          const numPages = pages.length || 'N/A'

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

          if (colMeta.encoding_stats && colMeta.encoding_stats.length > 0) {
            colMeta.encoding_stats.forEach((stat: any) => {
              aggregateStats.totalPages += stat.count
            })
          } else if (typeof numPages === 'number') {
            aggregateStats.totalPages += numPages
          }
          // If numPages is 'N/A', don't add to total (we don't have page info)

          const compressionRatio = colMeta.total_compressed_size > 0n
            ? Number(colMeta.total_uncompressed_size) / Number(colMeta.total_compressed_size)
            : 0

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

        // Allow garbage collection between batches for very large files
        if (global.gc && rowGroup.columns.length > 1000) {
          global.gc()
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
  } finally {
    closeSync(fd)
  }
}
