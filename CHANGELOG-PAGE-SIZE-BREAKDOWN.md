# Page Size Breakdown Feature - Changelog

## Summary

Added functionality to parse Parquet page data and display the size breakdown of repetition levels, definition levels, and values in the page histogram tooltip.

## Changes Made

### 1. Core Parsing Functions (src/lib/parquet-parsing-core.ts)

Added three new functions for parsing page data:

- **`parsePageDataSizes()`** - Parses a single page and returns size breakdown
  - Handles Data Page V1, V2, and Dictionary pages
  - For V2 pages, reads sizes directly from header
  - For V1 pages, calculates sizes by parsing RLE/bit-packed level data

- **`parseColumnChunkPageSizes()`** - Parses all pages in a column chunk
  - Reads page data using byte range reader
  - Returns size breakdown for each page
  - Note: Currently assumes uncompressed data

- **`calculateMaxLevels()`** - Calculates max repetition/definition levels from schema
  - Traverses schema to determine level values
  - Required input for page parsing functions

Added new type:

- **`PageSizeBreakdown`** - Interface containing size breakdown for page components
  ```typescript
  interface PageSizeBreakdown {
    pageNumber: number
    pageType: string
    headerSize: number
    repetitionLevelsSize: number
    definitionLevelsSize: number
    valuesSize: number
    totalDataSize: number
  }
  ```

### 2. Export Updates

Updated both browser and Node.js modules to export new functions:
- `src/lib/parquet-parsing.ts` (browser)
- `src/lib/parquet-parsing-node.ts` (Node.js)

### 3. UI Integration (webapp/src/components/PagesView.tsx)

Enhanced the Pages View component to:

1. **Import new parsing functions**
   - Added `PageSizeBreakdown` type
   - Imported `parsePageDataSizes` and `calculateMaxLevels`

2. **Load page size breakdowns**
   - Added `pageSizeBreakdowns` state
   - Calculate max levels from schema when loading pages
   - Parse size breakdown for each page
   - Handle errors gracefully with warnings

3. **Enhanced tooltip display**
   - Added "Page Data Breakdown" section to tooltip
   - Shows repetition levels, definition levels, and values sizes
   - Displays percentage distribution (Rep % | Def % | Val %)
   - Better organized with section headers

### 4. Example and Documentation

Created comprehensive documentation:

- **`example-page-size-parsing.ts`** - Working example showing how to use the API
  - Parse column chunks and display size breakdowns
  - Calculate and display percentage distributions
  - Can be run with: `npx tsx example-page-size-parsing.ts <file> <rg> <col>`

- **`PAGE-PARSING-API.md`** - Complete API documentation
  - Detailed function descriptions
  - Usage examples for both Node.js and browser
  - Notes on limitations and future enhancements
  - References to Parquet format specifications

## Tooltip Example

When hovering over a page in the histogram, users now see:

```
Page 0: DATA_PAGE
Encoding: PLAIN_DICTIONARY
Header: 35 bytes
Compressed: 8,192 bytes
Uncompressed: 16,384 bytes
Total (Header + Compressed): 8,227 bytes

--- Page Data Breakdown ---
Repetition Levels: 0 bytes
Definition Levels: 127 bytes
Values: 16,257 bytes
Distribution: Rep 0.0% | Def 0.8% | Val 99.2%

Values Count: 1,024

--- Statistics ---
Nulls: 15
Distinct: 987
Min: 100
Max: 9999
```

## Testing

- All existing tests pass (54 tests in 3 test files)
- Build succeeds without TypeScript errors
- Webapp builds successfully with new features

## Compression Support

The implementation now includes decompression for:
- **UNCOMPRESSED**: No decompression needed
- **SNAPPY**: Fully supported using hyparquet's built-in decompressor
- **ZSTD**: Fully supported using the fzstd library

This means the page size breakdown now works for all pages in UNCOMPRESSED, SNAPPY, and ZSTD compressed columns!

### Dependencies Added
- `fzstd`: Pure JavaScript ZSTD decompression library (works in Node.js and browsers)

## Limitations

1. **Compression**: Currently supports UNCOMPRESSED, SNAPPY, and ZSTD codecs
   - TODO: Add support for GZIP, BROTLI, LZ4

2. **Level Parsing**: For Data Page V1, reads length prefix but doesn't fully decode RLE/bit-packed data
   - Works for determining sizes but not for reading individual level values

3. **Schema Traversal**: Basic implementation of max level calculation
   - May need refinement for complex nested schemas

## Future Enhancements

- [x] Add decompression support for SNAPPY codec ✅
- [x] Add decompression support for ZSTD codec ✅
- [ ] Add decompression support for GZIP, BROTLI, LZ4 codecs
- [ ] Add full RLE/bit-packed level decoding
- [ ] Add support for reading actual level values
- [ ] Add support for reading actual data values
- [ ] Add validation of parsed sizes against page header
- [ ] Improve schema traversal for complex nested structures
- [ ] Add visual representation of size breakdown in histogram (stacked bars)

## Files Modified

1. `src/lib/parquet-parsing-core.ts` - Core parsing logic
2. `src/lib/parquet-parsing.ts` - Browser exports
3. `src/lib/parquet-parsing-node.ts` - Node.js exports
4. `webapp/src/components/PagesView.tsx` - UI integration
5. `tests/parquet-page-parsing.test.ts` - Fixed TypeScript warning

## Files Added

1. `example-page-size-parsing.ts` - Usage example
2. `PAGE-PARSING-API.md` - API documentation
3. `CHANGELOG-PAGE-SIZE-BREAKDOWN.md` - This file

## Commit Message

```
Add page size breakdown parsing and display in histogram tooltip

- Implement parsePageDataSizes() to extract rep/def/values sizes
- Add calculateMaxLevels() to compute max levels from schema
- Display size breakdown in page histogram tooltips
- Include percentage distribution of page components
- Add comprehensive API documentation and examples
- Support both Data Page V1 and V2 formats
```
