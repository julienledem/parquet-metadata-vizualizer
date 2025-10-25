import { describe, it, expect } from 'vitest'
import { readdirSync } from 'fs'
import { join } from 'path'
import { readParquetPagesFromFile } from '../src/lib/parquet-parsing-node.js'

describe('Parquet Page-Level Metadata', () => {
  // Find all .parquet files in the project root directory
  const projectRoot = process.cwd()
  const files = readdirSync(projectRoot)
  const parquetFiles = files.filter(file => file.endsWith('.parquet'))

  it('should find at least one parquet file in the project', () => {
    expect(parquetFiles.length).toBeGreaterThan(0)
  })

  // Create a test for each parquet file found
  parquetFiles.forEach(filename => {
    describe(`File: ${filename}`, () => {
      const filePath = join(projectRoot, filename)
      let pageMetadata: Awaited<ReturnType<typeof readParquetPagesFromFile>>

      it('should read metadata for page analysis', { timeout: 60000 }, async () => {
        pageMetadata = await readParquetPagesFromFile(filePath)

        expect(pageMetadata).toBeDefined()
        expect(pageMetadata.fileMetadata).toBeDefined()
        expect(pageMetadata.rowGroups).toBeDefined()
      })

      it('should have column chunks with metadata', () => {
        pageMetadata.rowGroups.forEach((rg, rgIndex) => {
          rg.columns.forEach((col, colIndex) => {
            expect(col.columnName).toBeDefined()
            expect(col.physicalType).toBeDefined()
            expect(col.compressionCodec).toBeDefined()
            expect(col.encodings).toBeDefined()
            expect(Array.isArray(col.encodings)).toBe(true)
          })
        })
      })

      it('should display comprehensive page metadata summary', () => {
        const { fileMetadata, rowGroups } = pageMetadata

        console.log(`\n${'='.repeat(80)}`)
        console.log(`PAGE-LEVEL METADATA SUMMARY FOR: ${filename}`)
        console.log('='.repeat(80))
        console.log(`\nFile Statistics:`)
        console.log(`  Total rows: ${fileMetadata.numRows}`)
        console.log(`  Total row groups: ${fileMetadata.numRowGroups}`)
        console.log(`  Parquet version: ${fileMetadata.version}`)
        if (fileMetadata.createdBy) {
          console.log(`  Created by: ${fileMetadata.createdBy}`)
        }

        let totalPages = 0
        const MAX_COLUMNS_TO_DISPLAY = 100 // Limit detailed output for large files
        const totalColumns = rowGroups.reduce((sum, rg) => sum + rg.numColumns, 0)
        const isLargeFile = totalColumns > MAX_COLUMNS_TO_DISPLAY

        if (isLargeFile) {
          console.log(`\n⚠️  Large file detected (${totalColumns} total columns). Showing summary mode.`)
        }

        // Iterate through each row group
        rowGroups.forEach((rowGroup) => {
          console.log(`\n${'-'.repeat(80)}`)
          console.log(`ROW GROUP ${rowGroup.rowGroupIndex}`)
          console.log('-'.repeat(80))
          console.log(`  Rows in group: ${rowGroup.numRows}`)
          console.log(`  Total byte size: ${rowGroup.totalByteSize.toLocaleString()} bytes`)
          console.log(`  Total compressed size: ${rowGroup.totalCompressedSize?.toLocaleString() || 'N/A'} bytes`)
          console.log(`  Number of columns: ${rowGroup.numColumns}`)

          // For large files, only show summary statistics
          if (isLargeFile) {
            // Process columns in streaming fashion without detailed output
            rowGroup.columns.forEach((column) => {
              // Just count pages without logging details
              if (column.encodingStats && column.encodingStats.length > 0) {
                column.encodingStats.forEach((stat) => {
                  totalPages += stat.count
                })
              } else if (typeof column.numPages === 'number') {
                totalPages += column.numPages
              }
            })

            console.log(`  (Column details omitted for large file - showing only first 10 columns)`)

            // Show only first 10 columns in summary
            rowGroup.columns.slice(0, 10).forEach((column) => {
              console.log(`\n  Column ${column.columnIndex}: ${column.columnName}`)
              console.log(`    Type: ${column.physicalType}, Pages: ${column.numPages}, Codec: ${column.compressionCodec}`)
            })

            if (rowGroup.numColumns > 10) {
              console.log(`\n  ... and ${rowGroup.numColumns - 10} more columns`)
            }
          } else {
            // Full detail mode for smaller files
            rowGroup.columns.forEach((column) => {
              console.log(`\n  Column ${column.columnIndex}: ${column.columnName}`)
              console.log(`  ${'~'.repeat(76)}`)

              // Basic column information
              console.log(`    Physical Type: ${column.physicalType}`)
              console.log(`    Compression Codec: ${column.compressionCodec}`)
              console.log(`    Total Values: ${column.totalValues.toLocaleString()}`)
              console.log(`    Total Compressed Size: ${column.totalCompressedSize.toLocaleString()} bytes`)
              console.log(`    Total Uncompressed Size: ${column.totalUncompressedSize.toLocaleString()} bytes`)
              console.log(`    Compression Ratio: ${column.compressionRatio}x`)

              // Encodings used
              console.log(`    Encodings Used: ${column.encodings.join(', ')}`)

              // Page offsets
              console.log(`\n    Page Offsets:`)
              if (column.dictionaryPageOffset !== undefined) {
                console.log(`      Dictionary Page Offset: ${column.dictionaryPageOffset}`)
              }
              console.log(`      Data Page Offset: ${column.dataPageOffset}`)
              if (column.indexPageOffset !== undefined) {
                console.log(`      Index Page Offset: ${column.indexPageOffset}`)
              }

              // Encoding statistics (if available)
              if (column.encodingStats && column.encodingStats.length > 0) {
                console.log(`\n    Encoding Statistics (by page type):`)
                column.encodingStats.forEach((stat) => {
                  console.log(`      ${stat.pageType}: ${stat.encoding} (${stat.count} pages)`)
                  totalPages += stat.count
                })
              }

              // Column statistics (if available)
              if (column.statistics) {
                console.log(`\n    Column Statistics:`)
                const stats = column.statistics

                if (stats.nullCount !== undefined) {
                  console.log(`      Null Count: ${stats.nullCount.toLocaleString()}`)
                }
                if (stats.distinctCount !== undefined) {
                  console.log(`      Distinct Count: ${stats.distinctCount.toLocaleString()}`)
                }
                if (stats.min !== undefined) {
                  const minStr = typeof stats.min === 'bigint' ? stats.min.toString() :
                                 stats.min instanceof Uint8Array ? `[${stats.min.length} bytes]` :
                                 String(stats.min)
                  console.log(`      Min Value: ${minStr.length > 50 ? minStr.substring(0, 50) + '...' : minStr}`)
                }
                if (stats.max !== undefined) {
                  const maxStr = typeof stats.max === 'bigint' ? stats.max.toString() :
                                 stats.max instanceof Uint8Array ? `[${stats.max.length} bytes]` :
                                 String(stats.max)
                  console.log(`      Max Value: ${maxStr.length > 50 ? maxStr.substring(0, 50) + '...' : maxStr}`)
                }
                if (stats.isMaxValueExact !== undefined) {
                  console.log(`      Max Value Exact: ${stats.isMaxValueExact}`)
                }
                if (stats.isMinValueExact !== undefined) {
                  console.log(`      Min Value Exact: ${stats.isMinValueExact}`)
                }
              }

              // Size statistics (if available)
              if (column.sizeStatistics) {
                console.log(`\n    Size Statistics:`)
                const sizeStats = column.sizeStatistics
                if (sizeStats.unencodedByteArrayDataBytes !== undefined) {
                  console.log(`      Unencoded Byte Array Data: ${sizeStats.unencodedByteArrayDataBytes.toLocaleString()} bytes`)
                }
                if (sizeStats.repetitionLevelHistogram) {
                  console.log(`      Repetition Level Histogram: [${sizeStats.repetitionLevelHistogram.length} entries]`)
                }
                if (sizeStats.definitionLevelHistogram) {
                  console.log(`      Definition Level Histogram: [${sizeStats.definitionLevelHistogram.length} entries]`)
                }
              }

              // Bloom filter (if available)
              if (column.bloomFilter) {
                console.log(`\n    Bloom Filter:`)
                console.log(`      Offset: ${column.bloomFilter.offset}`)
                console.log(`      Length: ${column.bloomFilter.length} bytes`)
              }

              // Key-value metadata (if available)
              if (column.keyValueMetadata && column.keyValueMetadata.length > 0) {
                console.log(`\n    Column Key-Value Metadata:`)
                column.keyValueMetadata.forEach((kv) => {
                  console.log(`      ${kv.key}: ${kv.value || '[no value]'}`)
                })
              }
            })
          }
        })

        console.log(`\n${'='.repeat(80)}`)
        console.log(`TOTAL PAGES IN FILE: ${totalPages}`)
        console.log(`END OF PAGE METADATA FOR: ${filename}`)
        console.log('='.repeat(80))

        expect(pageMetadata).toBeDefined()
      })

      it('should aggregate page statistics across all row groups', () => {
        const { aggregateStats } = pageMetadata

        console.log(`\nAggregate Statistics for ${filename}:`)
        console.log(`  Total Column Chunks: ${aggregateStats.totalColumnChunks}`)
        console.log(`  Total Pages: ${aggregateStats.totalPages}`)
        if (aggregateStats.totalPages === 0) {
          console.log(`  (Note: Page-level information not available - file has no offset_index or encoding_stats)`)
        }
        console.log(`  Average Pages per Column Chunk: ${aggregateStats.averagePagesPerColumnChunk}`)
        console.log(`  Total Compressed Size: ${aggregateStats.totalCompressedBytes.toLocaleString()} bytes`)
        console.log(`  Total Uncompressed Size: ${aggregateStats.totalUncompressedBytes.toLocaleString()} bytes`)
        console.log(`  Overall Compression Ratio: ${aggregateStats.overallCompressionRatio}x`)
        console.log(`  Encodings Used: ${aggregateStats.encodingsUsed.join(', ')}`)
        console.log(`  Compression Codecs: ${aggregateStats.codecsUsed.join(', ')}`)
        console.log(`  Column Chunks with Dictionary Pages: ${aggregateStats.columnsWithDictionary}`)
        console.log(`  Column Chunks with Bloom Filters: ${aggregateStats.columnsWithBloomFilter}`)
        console.log(`  Column Chunks with Statistics: ${aggregateStats.columnsWithStatistics}`)
        console.log(`  Column Chunks with Offset Index: ${aggregateStats.columnsWithOffsetIndex}`)

        expect(aggregateStats.totalColumnChunks).toBeGreaterThan(0)
        // Note: totalPages may be 0 for files without offset_index or encoding_stats
        expect(aggregateStats.totalPages).toBeGreaterThanOrEqual(0)
      })

    })
  })
})
