# Parquet Page Data Size Parsing API

This document describes the new API for parsing Parquet page data and extracting size breakdowns of repetition levels, definition levels, and values.

## Overview

Parquet pages contain three main components:
1. **Repetition Levels** - Used for nested/repeated fields
2. **Definition Levels** - Used for optional/nullable fields
3. **Values** - The actual data values

This API allows you to parse page data and determine the exact byte size of each component.

## API Reference

### Types

#### `PageSizeBreakdown`

Contains the size breakdown for a single page:

```typescript
interface PageSizeBreakdown {
  pageNumber: number           // Page number within the column chunk
  pageType: string             // Page type (DATA_PAGE, DATA_PAGE_V2, DICTIONARY_PAGE)
  headerSize: number           // Size of the page header in bytes
  repetitionLevelsSize: number // Size of repetition levels in bytes
  definitionLevelsSize: number // Size of definition levels in bytes
  valuesSize: number           // Size of values data in bytes
  totalDataSize: number        // Total size of page data (excluding header)
  nullCount?: number           // Number of null values in the page (when available)
}
```

### Functions

#### `parsePageDataSizes()`

Parse a single page's data and extract size information.

```typescript
function parsePageDataSizes(
  pageInfo: PageInfo,
  uncompressedPageData: ArrayBuffer,
  maxRepetitionLevel: number,
  maxDefinitionLevel: number
): PageSizeBreakdown
```

**Parameters:**
- `pageInfo` - Page metadata from `parseParquetPage()`
- `uncompressedPageData` - Uncompressed page data (excluding the page header)
- `maxRepetitionLevel` - Maximum repetition level for the column (0 for non-nested columns)
- `maxDefinitionLevel` - Maximum definition level for the column

**Returns:** `PageSizeBreakdown` object with size information

**Notes:**
- For Data Page V2, sizes are read directly from the page header
- For Data Page V1, sizes are calculated by parsing the RLE/bit-packed level data
- Dictionary pages have no repetition or definition levels

---

#### `parseColumnChunkPageSizes()`

Parse all pages in a column chunk and extract size breakdowns.

```typescript
async function parseColumnChunkPageSizes(
  columnChunkMetadata: any,
  readByteRange: (offset: number, length: number) => Promise<ArrayBuffer>,
  maxRepetitionLevel: number,
  maxDefinitionLevel: number
): Promise<PageSizeBreakdown[]>
```

**Parameters:**
- `columnChunkMetadata` - Column chunk metadata from the Parquet footer
- `readByteRange` - Function to read byte ranges from the file
- `maxRepetitionLevel` - Maximum repetition level for the column
- `maxDefinitionLevel` - Maximum definition level for the column

**Returns:** Array of `PageSizeBreakdown` objects, one for each page

**Important:** Currently assumes uncompressed data. Compression support is planned.

---

#### `calculateMaxLevels()`

Calculate the maximum repetition and definition levels for a column from the schema.

```typescript
function calculateMaxLevels(
  schema: any[],
  columnPath: string[]
): { maxRepetitionLevel: number; maxDefinitionLevel: number }
```

**Parameters:**
- `schema` - Complete Parquet schema array from file metadata
- `columnPath` - Path to the column (e.g., `['user', 'address', 'city']`)

**Returns:** Object with `maxRepetitionLevel` and `maxDefinitionLevel`

**Example:**
```typescript
const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
  metadata.fileMetadata.schema,
  ['users', 'emails']  // path_in_schema from column metadata
)
```

## Usage Examples

### Example 1: Parse a Single Column

```typescript
import {
  readParquetPagesFromFile,
  parseColumnChunkPageSizes,
  calculateMaxLevels
} from './src/lib/parquet-parsing-node.js'
import { openSync, readSync, closeSync } from 'fs'

async function analyzeColumn(filePath: string, rowGroupIndex: number, columnIndex: number) {
  // Read file metadata
  const metadata = await readParquetPagesFromFile(filePath)

  // Get column chunk
  const columnChunk = metadata.fileMetadata.rowGroups[rowGroupIndex].columns[columnIndex]

  // Calculate levels from schema
  const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
    metadata.fileMetadata.schema,
    columnChunk.meta_data.path_in_schema
  )

  // Create byte range reader
  const fd = openSync(filePath, 'r')
  const readByteRange = async (offset: number, length: number) => {
    const buffer = Buffer.allocUnsafe(length)
    readSync(fd, buffer, 0, length, offset)
    return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
  }

  try {
    // Parse all pages
    const pageSizes = await parseColumnChunkPageSizes(
      columnChunk,
      readByteRange,
      maxRepetitionLevel,
      maxDefinitionLevel
    )

    // Display results
    for (const page of pageSizes) {
      console.log(`Page ${page.pageNumber}:`)
      console.log(`  Rep Levels: ${page.repetitionLevelsSize} bytes`)
      console.log(`  Def Levels: ${page.definitionLevelsSize} bytes`)
      console.log(`  Values: ${page.valuesSize} bytes`)
    }
  } finally {
    closeSync(fd)
  }
}
```

### Example 2: Analyze Size Distribution

```typescript
function analyzePageOverhead(pageSizes: PageSizeBreakdown[]): void {
  let totalRepSize = 0
  let totalDefSize = 0
  let totalValueSize = 0

  for (const page of pageSizes) {
    totalRepSize += page.repetitionLevelsSize
    totalDefSize += page.definitionLevelsSize
    totalValueSize += page.valuesSize
  }

  const total = totalRepSize + totalDefSize + totalValueSize

  console.log('Size Distribution:')
  console.log(`  Repetition Levels: ${(totalRepSize / total * 100).toFixed(1)}%`)
  console.log(`  Definition Levels: ${(totalDefSize / total * 100).toFixed(1)}%`)
  console.log(`  Values: ${(totalValueSize / total * 100).toFixed(1)}%`)
}
```

### Example 3: Browser Usage

```typescript
import {
  readParquetPagesFromFile,
  parsePageDataSizes,
  parseParquetPage,
  calculateMaxLevels
} from './src/lib/parquet-parsing.js'

async function analyzeBrowserFile(file: File) {
  // Read metadata
  const metadata = await readParquetPagesFromFile(file)

  // Get first column
  const columnChunk = metadata.fileMetadata.rowGroups[0].columns[0]

  // Calculate levels
  const { maxRepetitionLevel, maxDefinitionLevel } = calculateMaxLevels(
    metadata.fileMetadata.schema,
    columnChunk.meta_data.path_in_schema
  )

  // Create byte range reader
  const readByteRange = async (offset: number, length: number) => {
    const slice = file.slice(offset, offset + length)
    return await slice.arrayBuffer()
  }

  // Get pages
  const pages = await parseParquetPage(columnChunk, readByteRange)

  // Analyze each page
  for (const page of pages) {
    const pageOffset = Number(page.offset) + (page.headerSize || 0)
    const pageData = await readByteRange(pageOffset, page.compressedSize || 0)

    const sizeBreakdown = parsePageDataSizes(
      page,
      pageData,
      maxRepetitionLevel,
      maxDefinitionLevel
    )

    console.log(`Page ${sizeBreakdown.pageNumber}:`, sizeBreakdown)
  }
}
```

## Running the Example

A complete example is provided in `example-page-size-parsing.ts`:

```bash
# Analyze the first column of the first row group
npx tsx example-page-size-parsing.ts ./path/to/file.parquet 0 0

# Analyze a different column
npx tsx example-page-size-parsing.ts ./path/to/file.parquet 0 5
```

## Understanding the Results

### Page Types

- **DATA_PAGE** (v1): Contains repetition levels, definition levels, and values. Levels are RLE/bit-packed encoded with a 4-byte length prefix.
- **DATA_PAGE_V2**: Same as v1, but level sizes are stored directly in the page header.
- **DICTIONARY_PAGE**: Contains only dictionary values, no levels.

### Levels

- **Repetition Levels**: Only present when `maxRepetitionLevel > 0` (nested/repeated fields)
- **Definition Levels**: Only present when `maxDefinitionLevel > 0` (optional/nullable fields)

### Null Count

The optional `nullCount` field shows the number of null/missing values in a page:

#### For DATA_PAGE_V2:
- Read directly from `num_nulls` field in page header (when writer includes it)
- Always accurate and fast

#### For DATA_PAGE_V1:
- Computed by decoding definition levels when they're small (<1kB)
- Values with `definitionLevel < maxDefinitionLevel` are counted as nulls
- Only available for pages with small definition level data (performance optimization)

#### When Null Count is Unavailable:
- Large definition level data (≥1kB) in DATA_PAGE_V1
- Writer didn't include statistics in page header
- Dictionary pages (don't contain nulls)

### Size Calculation

For Data Page V1:
1. First 4 bytes of repetition level data = length prefix (little-endian uint32)
2. Next N bytes = actual repetition level data
3. Same format for definition levels
4. Remaining bytes = values

For Data Page V2:
- Sizes are directly in the page header (`repetition_levels_byte_length`, `definition_levels_byte_length`)

## Compression Support

The library now supports decompression of page data:

- **UNCOMPRESSED**: No decompression needed
- **SNAPPY**: Fully supported using hyparquet's built-in decompressor
- **ZSTD**: Fully supported using the fzstd library
- **GZIP, BROTLI, LZ4**: Not yet supported (will throw an error)

The `decompressPageData()` function handles decompression automatically based on the column's compression codec.

```typescript
import { decompressPageData } from './src/lib/parquet-parsing.js'

const compressedData = await readByteRange(pageOffset + headerSize, compressedSize)
const uncompressedBytes = decompressPageData(
  compressedData,
  uncompressedSize,
  'ZSTD' // or 'SNAPPY' or 'UNCOMPRESSED'
)
```

### DATA_PAGE_V2 Compression Flag

DATA_PAGE_V2 includes an `is_compressed` flag in the page header that overrides the column's codec:
- When `true` or undefined: page data is compressed using the column's codec
- When `false`: page data is **not compressed**, even if the column codec is ZSTD/SNAPPY/etc.

The library automatically checks this flag before attempting decompression:

```typescript
// Automatic handling of DATA_PAGE_V2 is_compressed flag
if (page.dataPageHeaderV2 && page.dataPageHeaderV2.is_compressed === false) {
  // Skip decompression - data is already uncompressed
  uncompressedBytes = new Uint8Array(compressedData)
} else {
  // Normal decompression based on codec
  uncompressedBytes = decompressPageData(compressedData, uncompressedSize, codec)
}
```

This allows writers to selectively compress pages for better performance on small or incompressible data.

## Limitations

1. **Compression**: Currently supports UNCOMPRESSED, SNAPPY, and ZSTD. Other codecs (GZIP, BROTLI, LZ4) are not yet supported.
2. **Level Parsing**: For Data Page V1, the current implementation reads the length prefix but doesn't fully decode RLE/bit-packed data. This works for determining sizes but not for reading individual level values.

## Future Enhancements

- [x] Add decompression support for SNAPPY codec ✅
- [x] Add decompression support for ZSTD codec ✅
- [ ] Add decompression support for GZIP, BROTLI, LZ4 codecs
- [ ] Add full RLE/bit-packed level decoding
- [ ] Add support for reading actual level values
- [ ] Add support for reading actual data values
- [ ] Add validation of parsed sizes against page header

## References

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Parquet Encoding Specification](https://github.com/apache/parquet-format/blob/master/Encodings.md)
- [Dremel Paper](https://research.google/pubs/pub36632/) - Original paper describing repetition and definition levels
