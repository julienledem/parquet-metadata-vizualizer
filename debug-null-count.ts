/**
 * Debug script to test null count extraction from Parquet pages
 */

import { readParquetPagesFromFile, parseColumnChunkPageSizes, calculateMaxLevels } from './src/lib/parquet-parsing-node.js'
import { openSync, readSync, closeSync, readdirSync, statSync } from 'fs'
import { join } from 'path'

async function testNullCountInFile(filePath: string): Promise<void> {
  console.log('\n' + '='.repeat(80))
  console.log(`Testing file: ${filePath}`)
  console.log('='.repeat(80))

  try {
    // Read metadata
    const metadata = await readParquetPagesFromFile(filePath)

    console.log(`\nFile info:`)
    console.log(`  Rows: ${metadata.fileMetadata.numRows}`)
    console.log(`  Columns: ${metadata.fileMetadata.numColumns}`)
    console.log(`  Row Groups: ${metadata.fileMetadata.numRowGroups}`)

    // Test first row group, first few columns
    const rowGroupIndex = 0
    const maxColumnsToTest = Math.min(3, metadata.fileMetadata.numColumns)

    for (let columnIndex = 0; columnIndex < maxColumnsToTest; columnIndex++) {
      console.log(`\n--- Testing Column ${columnIndex} ---`)

      const columnChunk = metadata.fileMetadata.rowGroups[rowGroupIndex].columns[columnIndex]
      const columnPath = columnChunk.meta_data.path_in_schema
      const codec = columnChunk.meta_data.codec

      console.log(`  Column name: ${columnPath.join('.')}`)
      console.log(`  Compression: ${codec}`)
      console.log(`  Type: ${columnChunk.meta_data.type}`)

      // Calculate max levels
      const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
        metadata.fileMetadata.schema,
        columnPath
      )

      console.log(`  Max repetition level: ${maxRepetitionLevel}`)
      console.log(`  Max definition level: ${maxDefinitionLevel}`)

      // Open file for reading
      const fd = openSync(filePath, 'r')

      const readByteRange = async (offset: number, length: number): Promise<ArrayBuffer> => {
        const buffer = Buffer.allocUnsafe(length)
        readSync(fd, buffer, 0, length, offset)
        return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
      }

      try {
        // Parse page sizes
        const pageSizes = await parseColumnChunkPageSizes(
          columnChunk,
          readByteRange,
          maxRepetitionLevel,
          maxDefinitionLevel
        )

        console.log(`  Pages: ${pageSizes.length}`)

        // Display null count info for each page
        for (const page of pageSizes) {
          console.log(`\n  Page ${page.pageNumber} (${page.pageType}):`)
          console.log(`    Def levels size: ${page.definitionLevelsSize} bytes`)
          console.log(`    Null count: ${page.nullCount !== undefined ? page.nullCount : 'N/A'}`)

          if (page.nullCount === undefined) {
            if (maxDefinitionLevel === 0) {
              console.log(`    Reason: No definition levels (required column, no nulls possible)`)
            } else if (page.definitionLevelsSize === 0) {
              console.log(`    Reason: Definition levels size is 0`)
            } else if (page.definitionLevelsSize >= 1024) {
              console.log(`    Reason: Definition levels too large (${page.definitionLevelsSize} bytes >= 1kB threshold)`)
            } else if (page.pageType === 'DICTIONARY_PAGE') {
              console.log(`    Reason: Dictionary pages don't have nulls`)
            } else {
              console.log(`    Reason: Unknown - should have been decoded!`)
            }
          } else {
            console.log(`    ✓ Null count successfully extracted!`)
          }
        }
      } catch (error) {
        console.error(`  ERROR parsing column: ${error}`)
      } finally {
        closeSync(fd)
      }
    }
  } catch (error) {
    console.error(`ERROR reading file: ${error}`)
  }
}

async function main() {
  const downloadsDir = join(process.env.HOME || '~', 'Downloads')

  console.log(`Searching for Parquet files in: ${downloadsDir}`)

  try {
    const files = readdirSync(downloadsDir)
    const parquetFiles = files
      .filter(f => f.endsWith('.parquet'))
      .map(f => join(downloadsDir, f))
      .filter(f => {
        try {
          const stats = statSync(f)
          return stats.isFile()
        } catch {
          return false
        }
      })

    if (parquetFiles.length === 0) {
      console.log('No Parquet files found in Downloads folder')
      return
    }

    console.log(`Found ${parquetFiles.length} Parquet file(s):\n`)
    parquetFiles.forEach((f, i) => {
      const stats = statSync(f)
      console.log(`  ${i + 1}. ${f.split('/').pop()} (${(stats.size / 1024 / 1024).toFixed(2)} MB)`)
    })

    // Test first file (or up to 3 files)
    const filesToTest = parquetFiles.slice(0, 3)

    for (const file of filesToTest) {
      await testNullCountInFile(file)
    }

    console.log('\n' + '='.repeat(80))
    console.log('SUMMARY')
    console.log('='.repeat(80))
    console.log('If null counts are not showing up, check:')
    console.log('1. Are columns REQUIRED (no nulls possible)?')
    console.log('2. Are definition levels too large (>1kB)?')
    console.log('3. Is this a DATA_PAGE_V2 with num_nulls in header?')
    console.log('4. Are definition levels being decoded correctly?')

  } catch (error) {
    console.error(`ERROR: ${error}`)
  }
}

main().catch(console.error)
