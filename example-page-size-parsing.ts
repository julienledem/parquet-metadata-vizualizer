/**
 * Example: Parsing Parquet Page Data Sizes
 *
 * This example demonstrates how to use the new page parsing functions
 * to extract the size breakdown of repetition levels, definition levels,
 * and values within Parquet pages.
 */

import {
  readParquetPagesFromFile,
  parseColumnChunkPageSizes,
  calculateMaxLevels,
  type PageSizeBreakdown
} from './src/lib/parquet-parsing-node.js'
import { openSync, closeSync } from 'fs'

/**
 * Parse a column chunk and display size breakdown for each page
 */
async function parseColumnChunkSizes(
  filePath: string,
  rowGroupIndex: number,
  columnIndex: number
): Promise<PageSizeBreakdown[]> {
  // 1. Read the file metadata
  const metadata = await readParquetPagesFromFile(filePath)

  // 2. Get the column chunk we want to analyze
  const rowGroup = metadata.rowGroups[rowGroupIndex]
  const columnChunk = metadata.fileMetadata.rowGroups[rowGroupIndex].columns[columnIndex]

  // 3. Calculate max repetition and definition levels from schema
  const columnPath = columnChunk.meta_data.path_in_schema
  const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
    metadata.fileMetadata.schema,
    columnPath
  )

  console.log(`\nAnalyzing column: ${columnPath.join('.')}`)
  console.log(`  Max Repetition Level: ${maxRepetitionLevel}`)
  console.log(`  Max Definition Level: ${maxDefinitionLevel}`)

  // 4. Create a byte range reader for the file
  const fd = openSync(filePath, 'r')
  const { readSync } = await import('fs')

  const readByteRange = async (offset: number, length: number): Promise<ArrayBuffer> => {
    const buffer = Buffer.allocUnsafe(length)
    readSync(fd, buffer, 0, length, offset)
    return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
  }

  try {
    // 5. Parse all pages in the column chunk
    const pageSizes = await parseColumnChunkPageSizes(
      columnChunk,
      readByteRange,
      maxRepetitionLevel,
      maxDefinitionLevel
    )

    return pageSizes
  } finally {
    closeSync(fd)
  }
}

/**
 * Display size breakdown for pages
 */
function displayPageSizes(pageSizes: PageSizeBreakdown[]): void {
  console.log('\n' + '='.repeat(80))
  console.log('PAGE SIZE BREAKDOWN')
  console.log('='.repeat(80))

  let totalHeaderSize = 0
  let totalRepetitionSize = 0
  let totalDefinitionSize = 0
  let totalValuesSize = 0

  for (const page of pageSizes) {
    console.log(`\nPage ${page.pageNumber} (${page.pageType}):`)
    console.log(`  Header Size:             ${page.headerSize.toLocaleString()} bytes`)
    console.log(`  Repetition Levels Size:  ${page.repetitionLevelsSize.toLocaleString()} bytes`)
    console.log(`  Definition Levels Size:  ${page.definitionLevelsSize.toLocaleString()} bytes`)
    console.log(`  Values Size:             ${page.valuesSize.toLocaleString()} bytes`)
    console.log(`  Total Data Size:         ${page.totalDataSize.toLocaleString()} bytes`)

    // Calculate percentages
    const total = page.headerSize + page.totalDataSize
    const repPercent = (page.repetitionLevelsSize / page.totalDataSize * 100).toFixed(1)
    const defPercent = (page.definitionLevelsSize / page.totalDataSize * 100).toFixed(1)
    const valPercent = (page.valuesSize / page.totalDataSize * 100).toFixed(1)

    console.log(`  Breakdown: Rep ${repPercent}% | Def ${defPercent}% | Values ${valPercent}%`)

    totalHeaderSize += page.headerSize
    totalRepetitionSize += page.repetitionLevelsSize
    totalDefinitionSize += page.definitionLevelsSize
    totalValuesSize += page.valuesSize
  }

  console.log('\n' + '-'.repeat(80))
  console.log('TOTALS:')
  console.log(`  Total Header Size:             ${totalHeaderSize.toLocaleString()} bytes`)
  console.log(`  Total Repetition Levels Size:  ${totalRepetitionSize.toLocaleString()} bytes`)
  console.log(`  Total Definition Levels Size:  ${totalDefinitionSize.toLocaleString()} bytes`)
  console.log(`  Total Values Size:             ${totalValuesSize.toLocaleString()} bytes`)

  const grandTotal = totalRepetitionSize + totalDefinitionSize + totalValuesSize
  if (grandTotal > 0) {
    const repPercent = (totalRepetitionSize / grandTotal * 100).toFixed(1)
    const defPercent = (totalDefinitionSize / grandTotal * 100).toFixed(1)
    const valPercent = (totalValuesSize / grandTotal * 100).toFixed(1)
    console.log(`  Overall Breakdown: Rep ${repPercent}% | Def ${defPercent}% | Values ${valPercent}%`)
  }

  console.log('='.repeat(80))
}

/**
 * Main example
 */
async function main() {
  // Example usage: parse the first column of the first row group
  const filePath = process.argv[2] || './parquet-testdata/alltypes_dictionary.parquet'
  const rowGroupIndex = parseInt(process.argv[3] || '0', 10)
  const columnIndex = parseInt(process.argv[4] || '0', 10)

  console.log(`Parsing Parquet file: ${filePath}`)
  console.log(`Row Group: ${rowGroupIndex}, Column: ${columnIndex}`)

  try {
    const pageSizes = await parseColumnChunkSizes(filePath, rowGroupIndex, columnIndex)
    displayPageSizes(pageSizes)
  } catch (error) {
    console.error('Error parsing page sizes:', error)
    process.exit(1)
  }
}

// Run the example if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
}
