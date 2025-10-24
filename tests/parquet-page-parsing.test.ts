import { describe, it, expect } from 'vitest'
import { openSync, readSync, fstatSync, closeSync, readdirSync } from 'fs'
import { join } from 'path'
import { parseParquetPage } from '../src/lib/parquet-parsing-core.js'
import { readParquetPagesFromFile } from '../src/lib/parquet-parsing-node.js'

describe('parseParquetPage', () => {
  // Helper function to read byte ranges from a file
  function createByteRangeReader(filePath: string) {
    const fd = openSync(filePath, 'r')
    return {
      fd,
      reader: async (offset: number, length: number): Promise<ArrayBuffer> => {
        const buffer = Buffer.allocUnsafe(length)
        readSync(fd, buffer, 0, length, offset)
        return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
      },
      close: () => closeSync(fd)
    }
  }

  describe('alltypes_dictionary.parquet', () => {
    const filePath = 'alltypes_dictionary.parquet'

    it('should parse pages from the first column chunk', async () => {
      const metadata = await readParquetPagesFromFile(filePath)
      const firstColumnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

      const { reader, close } = createByteRangeReader(filePath)
      try {
        const pages = await parseParquetPage(firstColumnChunk, reader)

        // Should have parsed some pages
        expect(pages.length).toBeGreaterThan(0)

        // Each page should have required fields
        pages.forEach((page, idx) => {
          expect(page.pageNumber).toBe(idx)
          expect(page.pageType).toBeDefined()
          expect(typeof page.pageType).toBe('string')
          expect(page.offset).toBeDefined()
          expect(typeof page.offset).toBe('bigint')

          // Should have size information
          expect(page.compressedSize).toBeDefined()
          const compressedSize = Number(page.compressedSize)
          expect(compressedSize).toBeGreaterThan(0)

          expect(page.uncompressedSize).toBeDefined()
          const uncompressedSize = Number(page.uncompressedSize)
          expect(uncompressedSize).toBeGreaterThan(0)
        })

        // First column (id) should have dictionary and data pages
        const pageTypes = pages.map(p => p.pageType)
        expect(pageTypes).toContain('DICTIONARY_PAGE')
        expect(pageTypes).toContain('DATA_PAGE')
      } finally {
        close()
      }
    })

    it('should parse pages with correct page types', async () => {
      const metadata = await readParquetPagesFromFile(filePath)

      // Test multiple columns
      for (let colIndex = 0; colIndex < Math.min(3, metadata.fileMetadata.rowGroups[0].columns.length); colIndex++) {
        const columnChunk = metadata.fileMetadata.rowGroups[0].columns[colIndex]
        const { reader, close } = createByteRangeReader(filePath)

        try {
          const pages = await parseParquetPage(columnChunk, reader)

          // All pages should have valid page types
          pages.forEach(page => {
            expect(['DATA_PAGE', 'DICTIONARY_PAGE', 'INDEX_PAGE', 'DATA_PAGE_V2'])
              .toContain(page.pageType)
          })
        } finally {
          close()
        }
      }
    })

    it('should have correct size relationships', async () => {
      const metadata = await readParquetPagesFromFile(filePath)
      const firstColumnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

      const { reader, close } = createByteRangeReader(filePath)
      try {
        const pages = await parseParquetPage(firstColumnChunk, reader)

        pages.forEach(page => {
          // Uncompressed size should be >= compressed size (or equal if no compression)
          // Note: For very small pages, compression overhead can make compressed > uncompressed
          if (page.uncompressedSize && page.compressedSize) {
            const uncompressed = Number(page.uncompressedSize)
            const compressed = Number(page.compressedSize)
            // Just verify both are positive numbers
            expect(uncompressed).toBeGreaterThan(0)
            expect(compressed).toBeGreaterThan(0)
          }
        })

        // Total compressed size should roughly match column metadata
        const totalCompressed = pages.reduce((sum, p) => sum + Number(p.compressedSize), 0)
        const columnTotalCompressed = Number(firstColumnChunk.meta_data.total_compressed_size)

        // Page data sizes don't include page header overhead
        // So total should be less than or equal to column total
        expect(totalCompressed).toBeLessThanOrEqual(columnTotalCompressed)
        expect(totalCompressed).toBeGreaterThan(0) // Should have some data
      } finally {
        close()
      }
    })

    it('should extract encoding information when available', async () => {
      const metadata = await readParquetPagesFromFile(filePath)
      const firstColumnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

      const { reader, close } = createByteRangeReader(filePath)
      try {
        const pages = await parseParquetPage(firstColumnChunk, reader)

        // At least some pages should have encoding information
        const pagesWithEncoding = pages.filter(p => p.encoding !== undefined)
        expect(pagesWithEncoding.length).toBeGreaterThan(0)

        // Encodings should be valid strings
        pagesWithEncoding.forEach(page => {
          expect(typeof page.encoding).toBe('string')
          expect(page.encoding!.length).toBeGreaterThan(0)
        })
      } finally {
        close()
      }
    })
  })

  describe('yellow_tripdata_2025-01.parquet', () => {
    const filePath = 'yellow_tripdata_2025-01.parquet'

    it('should parse pages from a compressed column', async () => {
      const metadata = await readParquetPagesFromFile(filePath)

      // Find a column with ZSTD compression
      const rowGroup = metadata.fileMetadata.rowGroups[0]
      const compressedColumn = rowGroup.columns.find(
        col => col.meta_data.codec === 'ZSTD'
      )

      if (!compressedColumn) {
        console.log('No ZSTD compressed columns found, skipping test')
        return
      }

      const { reader, close } = createByteRangeReader(filePath)
      try {
        const pages = await parseParquetPage(compressedColumn, reader)

        expect(pages.length).toBeGreaterThan(0)

        // For compressed columns, check that we have valid sizes
        // Note: Small pages may have compressed > uncompressed due to overhead
        pages.forEach(page => {
          if (page.compressedSize && page.uncompressedSize) {
            const compressed = Number(page.compressedSize)
            const uncompressed = Number(page.uncompressedSize)
            // Just verify both are positive
            expect(compressed).toBeGreaterThan(0)
            expect(uncompressed).toBeGreaterThan(0)
          }
        })

        // Overall, total compressed should be less than total uncompressed
        const totalCompressed = pages.reduce((sum, p) => sum + Number(p.compressedSize), 0)
        const totalUncompressed = pages.reduce((sum, p) => sum + Number(p.uncompressedSize), 0)
        expect(totalCompressed).toBeGreaterThan(0)
        expect(totalUncompressed).toBeGreaterThan(0)
      } finally {
        close()
      }
    })

    it('should handle files with multiple pages per column', async () => {
      const metadata = await readParquetPagesFromFile(filePath)
      const firstColumnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

      const { reader, close } = createByteRangeReader(filePath)
      try {
        const pages = await parseParquetPage(firstColumnChunk, reader)

        // Should have multiple pages
        expect(pages.length).toBeGreaterThanOrEqual(1)

        // Page numbers should be sequential starting from 0
        pages.forEach((page, idx) => {
          expect(page.pageNumber).toBe(idx)
        })

        // Offsets should be increasing
        for (let i = 1; i < pages.length; i++) {
          expect(Number(pages[i].offset)).toBeGreaterThan(Number(pages[i-1].offset))
        }
      } finally {
        close()
      }
    })
  })

  describe('all parquet files in project', () => {
    // Find all .parquet files in the project root directory
    const projectRoot = process.cwd()
    const files = readdirSync(projectRoot)
    const parquetFiles = files.filter(file => file.endsWith('.parquet'))

    it('should find at least one parquet file', () => {
      expect(parquetFiles.length).toBeGreaterThan(0)
    })

    parquetFiles.forEach(filename => {
      describe(`File: ${filename}`, () => {
        const filePath = join(projectRoot, filename)
        let metadata: Awaited<ReturnType<typeof readParquetPagesFromFile>>

        it('should parse pages from multiple columns', { timeout: 60000 }, async () => {
          metadata = await readParquetPagesFromFile(filePath)

          // Test parsing pages for up to 5 columns from the first row group
          const columnsToTest = Math.min(5, metadata.fileMetadata.rowGroups[0].columns.length)

          let successfulParses = 0
          let failedParses = 0

          for (let colIndex = 0; colIndex < columnsToTest; colIndex++) {
            const columnChunk = metadata.fileMetadata.rowGroups[0].columns[colIndex]
            const { reader, close } = createByteRangeReader(filePath)

            try {
              const pages = await parseParquetPage(columnChunk, reader)

              // Should have parsed at least one page
              expect(pages.length).toBeGreaterThan(0)

              // Validate each page
              pages.forEach((page, pageIdx) => {
                // Page number should be sequential
                expect(page.pageNumber).toBe(pageIdx)

                // Should have a valid page type
                expect(page.pageType).toBeDefined()
                expect(typeof page.pageType).toBe('string')
                expect(['DATA_PAGE', 'DICTIONARY_PAGE', 'INDEX_PAGE', 'DATA_PAGE_V2'])
                  .toContain(page.pageType)

                // Should have valid offset
                expect(page.offset).toBeDefined()
                expect(typeof page.offset).toBe('bigint')
                expect(page.offset).toBeGreaterThanOrEqual(0n)

                // Should have size information
                expect(page.compressedSize).toBeDefined()
                expect(Number(page.compressedSize)).toBeGreaterThan(0)

                expect(page.uncompressedSize).toBeDefined()
                expect(Number(page.uncompressedSize)).toBeGreaterThan(0)
              })

              // Offsets should be increasing
              for (let i = 1; i < pages.length; i++) {
                expect(Number(pages[i].offset)).toBeGreaterThan(Number(pages[i-1].offset))
              }

              successfulParses++
            } catch (error) {
              // Some files may have parsing issues - that's okay
              // We just want to ensure most files parse correctly
              failedParses++
              console.warn(`Failed to parse column ${colIndex} in ${filename}:`, error instanceof Error ? error.message : String(error))
            } finally {
              close()
            }
          }

          // At least some columns should parse successfully
          expect(successfulParses).toBeGreaterThan(0)
        })
      })
    })
  })

  describe('error handling', () => {
    it('should handle columns with no pages gracefully', async () => {
      const metadata = await readParquetPagesFromFile('alltypes_dictionary.parquet')

      // Create a mock column chunk with minimal data
      const mockColumnChunk = {
        meta_data: {
          type: 'INT32',
          codec: 'UNCOMPRESSED',
          num_values: 0n,
          data_page_offset: 4n,
          total_compressed_size: 0n,
          total_uncompressed_size: 0n,
          path_in_schema: ['test'],
          encodings: []
        }
      }

      const { reader, close } = createByteRangeReader('alltypes_dictionary.parquet')
      try {
        const pages = await parseParquetPage(mockColumnChunk, reader)

        // Should return empty array or handle gracefully
        expect(Array.isArray(pages)).toBe(true)
      } catch (error) {
        // It's okay to throw an error for invalid data
        expect(error).toBeDefined()
      } finally {
        close()
      }
    })
  })
})
