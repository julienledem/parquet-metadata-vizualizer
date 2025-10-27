/**
 * Debug script to specifically test DATA_PAGE_V2 null counts
 */

import { readParquetPagesFromFile, parseColumnChunkPageSizes, calculateMaxLevels, parseParquetPage } from './src/lib/parquet-parsing-node.js'
import { openSync, readSync, closeSync } from 'fs'
import { join } from 'path'

async function testV2NullCounts(filePath: string): Promise<void> {
  console.log('\n' + '='.repeat(80))
  console.log(`Testing V2 pages in: ${filePath}`)
  console.log('='.repeat(80))

  try {
    const metadata = await readParquetPagesFromFile(filePath)

    console.log(`\nFile info:`)
    console.log(`  Rows: ${metadata.fileMetadata.numRows}`)
    console.log(`  Columns: ${metadata.fileMetadata.numColumns}`)

    // Check a few columns for V2 pages
    const rowGroupIndex = 0
    const columnsToTest = Math.min(5, metadata.fileMetadata.numColumns)

    const fd = openSync(filePath, 'r')
    const readByteRange = async (offset: number, length: number): Promise<ArrayBuffer> => {
      const buffer = Buffer.allocUnsafe(length)
      readSync(fd, buffer, 0, length, offset)
      return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
    }

    try {
      for (let colIdx = 0; colIdx < columnsToTest; colIdx++) {
        const columnChunk = metadata.fileMetadata.rowGroups[rowGroupIndex].columns[colIdx]
        const columnPath = columnChunk.meta_data.path_in_schema

        console.log(`\n--- Column ${colIdx}: ${columnPath.join('.')} ---`)

        // Parse pages to check types
        const pages = await parseParquetPage(columnChunk, readByteRange)

        let hasV2 = false
        let hasV1 = false
        let v2WithNulls = 0
        let v2WithoutNulls = 0

        for (const page of pages) {
          if (page.dataPageHeaderV2) {
            hasV2 = true
            if (page.dataPageHeaderV2.num_nulls !== undefined && page.dataPageHeaderV2.num_nulls !== null) {
              v2WithNulls++
            } else {
              v2WithoutNulls++
            }
          } else if (page.dataPageHeader) {
            hasV1 = true
          }
        }

        console.log(`  Total pages: ${pages.length}`)
        console.log(`  Has DATA_PAGE_V2: ${hasV2}`)
        console.log(`  Has DATA_PAGE_V1: ${hasV1}`)

        if (hasV2) {
          console.log(`  V2 pages with num_nulls: ${v2WithNulls}`)
          console.log(`  V2 pages without num_nulls: ${v2WithoutNulls}`)

          // Try parsing page sizes
          const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
            metadata.fileMetadata.schema,
            columnPath
          )

          try {
            const pageSizes = await parseColumnChunkPageSizes(
              columnChunk,
              readByteRange,
              maxRepetitionLevel,
              maxDefinitionLevel
            )

            let successCount = 0
            let failCount = 0

            for (const pageSize of pageSizes) {
              if (pageSize.nullCount !== undefined) {
                successCount++
              } else {
                failCount++
              }
            }

            console.log(`  Null counts extracted: ${successCount}/${pageSizes.length}`)
          } catch (error) {
            console.error(`  ERROR parsing page sizes: ${error}`)
          }
        }
      }
    } finally {
      closeSync(fd)
    }

  } catch (error) {
    console.error(`ERROR: ${error}`)
  }
}

async function main() {
  const downloadsDir = join(process.env.HOME || '~', 'Downloads')

  // Test the V2 files
  const v2File = join(downloadsDir, '10kcol-v2-sparse1page-sparseLE10Variant-003.parquet')
  await testV2NullCounts(v2File)

  // Also test the baseline for comparison
  const baselineFile = join(downloadsDir, '10kcol-baseline-002.parquet')
  await testV2NullCounts(baselineFile)
}

main().catch(console.error)
