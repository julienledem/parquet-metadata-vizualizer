import { describe, it, expect } from 'vitest'
import { readdirSync } from 'fs'
import { join } from 'path'
import { readParquetFooterFromFile } from '../src/lib/parquet-parsing-node.js'

describe('Parquet Footer Reading', () => {
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
      let metadata: ReturnType<typeof readParquetFooterFromFile>

      it('should read and parse the footer metadata', () => {
        metadata = readParquetFooterFromFile(filePath)

        expect(metadata).toBeDefined()
      })

      it('should have valid file metadata', () => {
        metadata = readParquetFooterFromFile(filePath)

        expect(metadata.version).toBeDefined()
        expect(metadata.schema).toBeDefined()
        expect(metadata.rowGroups).toBeDefined()
        expect(metadata.numRows).toBeDefined()
      })

      it('should have row groups', () => {
        metadata = readParquetFooterFromFile(filePath)

        expect(Array.isArray(metadata.rowGroups)).toBe(true)
        expect(metadata.rowGroups.length).toBeGreaterThan(0)
        expect(metadata.numRowGroups).toBe(metadata.rowGroups.length)
      })

      it('should have schema information', () => {
        metadata = readParquetFooterFromFile(filePath)

        expect(Array.isArray(metadata.schema)).toBe(true)
        expect(metadata.schema.length).toBeGreaterThan(0)
        expect(metadata.numColumns).toBeGreaterThan(0)
      })

      it('should have column information in row groups', () => {
        metadata = readParquetFooterFromFile(filePath)

        metadata.rowGroups.forEach((rowGroup: any, index: number) => {
          expect(rowGroup).toHaveProperty('columns')
          expect(Array.isArray(rowGroup.columns)).toBe(true)
          expect(rowGroup.columns.length).toBeGreaterThan(0)
        })
      })

      it('should display footer metadata summary', () => {
        metadata = readParquetFooterFromFile(filePath)

        console.log(`\n=== Footer Metadata for ${filename} ===`)
        console.log(`Version: ${metadata.version}`)
        console.log(`Number of rows: ${metadata.numRows}`)
        console.log(`Number of row groups: ${metadata.numRowGroups}`)
        console.log(`Number of columns: ${metadata.numColumns}`)

        if (metadata.createdBy) {
          console.log(`Created by: ${metadata.createdBy}`)
        }

        console.log('\nRow Groups:')
        metadata.rowGroups.forEach((rg: any, i: number) => {
          console.log(`  Row Group ${i}:`)
          console.log(`    Total byte size: ${rg.total_byte_size}`)
          console.log(`    Number of rows: ${rg.num_rows}`)
          console.log(`    Number of columns: ${rg.columns.length}`)
        })

        console.log('\nSchema:')
        metadata.schema.forEach((col: any, i: number) => {
          if (i === 0) return // Skip root element
          console.log(`  Column ${i}: ${col.name}`)
          if (col.type) console.log(`    Type: ${col.type}`)
          if (col.logicalType) console.log(`    Logical Type: ${JSON.stringify(col.logicalType)}`)
          if (col.repetition_type !== undefined) console.log(`    Repetition: ${col.repetition_type}`)
        })

        expect(metadata).toBeDefined()
      })
    })
  })
})
