/**
 * Debug ZSTD decompression issue
 */

import { parseParquetPage } from './src/lib/parquet-parsing-node.js'
import { readParquetPagesFromFile } from './src/lib/parquet-parsing-node.js'
import { decompress as zstdDecompress } from 'fzstd'
import { openSync, readSync, closeSync } from 'fs'
import { join } from 'path'

async function debugZstdPage(filePath: string): Promise<void> {
  console.log('Testing ZSTD decompression on:', filePath)

  try {
    const metadata = await readParquetPagesFromFile(filePath)
    const columnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

    console.log('Column:', columnChunk.meta_data.path_in_schema.join('.'))
    console.log('Codec:', columnChunk.meta_data.codec)

    const fd = openSync(filePath, 'r')
    const readByteRange = async (offset: number, length: number): Promise<ArrayBuffer> => {
      const buffer = Buffer.allocUnsafe(length)
      readSync(fd, buffer, 0, length, offset)
      return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
    }

    try {
      const pages = await parseParquetPage(columnChunk, readByteRange)

      console.log(`\nFound ${pages.length} pages`)

      // Try to decompress first data page
      for (let i = 0; i < Math.min(3, pages.length); i++) {
        const page = pages[i]

        console.log(`\nPage ${i}:`)
        console.log(`  Type: ${page.pageType}`)
        console.log(`  Compressed size: ${page.compressedSize}`)
        console.log(`  Uncompressed size: ${page.uncompressedSize}`)
        console.log(`  Header size: ${page.headerSize}`)

        if (page.dataPageHeaderV2) {
          console.log(`  V2 is_compressed: ${page.dataPageHeaderV2.is_compressed}`)
        }

        if (page.pageType !== 'DICTIONARY_PAGE') {
          try {
            const pageOffset = Number(page.offset)
            const headerSize = page.headerSize || 0
            const compressedSize = page.compressedSize || 0
            const uncompressedSize = page.uncompressedSize || compressedSize

            // Read compressed data
            const compressedData = await readByteRange(pageOffset + headerSize, compressedSize)
            const compressedBytes = new Uint8Array(compressedData)

            console.log(`  Read ${compressedBytes.length} compressed bytes`)
            console.log(`  First 20 bytes: ${Array.from(compressedBytes.slice(0, 20)).map(b => b.toString(16).padStart(2, '0')).join(' ')}`)

            // Try to decompress
            console.log('  Attempting ZSTD decompression...')
            const decompressed = zstdDecompress(compressedBytes)
            console.log(`  ✓ Successfully decompressed to ${decompressed.length} bytes`)

            if (decompressed.length !== uncompressedSize) {
              console.log(`  ⚠️  Size mismatch: expected ${uncompressedSize}, got ${decompressed.length}`)
            }
          } catch (error) {
            console.error(`  ✗ Decompression failed: ${error}`)
          }
        }
      }
    } finally {
      closeSync(fd)
    }
  } catch (error) {
    console.error('ERROR:', error)
  }
}

async function main() {
  const downloadsDir = join(process.env.HOME || '~', 'Downloads')

  // Test V2 file with ZSTD
  const v2File = join(downloadsDir, '10kcol-v2-sparse1page-sparseLE10Variant-003.parquet')
  await debugZstdPage(v2File)

  console.log('\n' + '='.repeat(80) + '\n')

  // Test baseline file with ZSTD
  const baselineFile = join(downloadsDir, '10kcol-baseline-002.parquet')
  await debugZstdPage(baselineFile)
}

main().catch(console.error)
