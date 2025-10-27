# DATA_PAGE_V2 Compression Flag Fix

## Issue

DATA_PAGE_V2 pages were failing with "Error: invalid zstd data" even though the column metadata specified ZSTD compression.

## Root Cause

DATA_PAGE_V2 has an `is_compressed` flag in the page header that indicates whether the page data is actually compressed, independent of the column's compression codec setting. When `is_compressed: false`, the page data is already uncompressed and should not be decompressed.

Our code was always attempting to decompress based on the column's codec, without checking this flag.

## Example

From debug output:
```
Page 1:
  Type: DATA_PAGE_V2
  Compressed size: 8
  Uncompressed size: 8
  V2 is_compressed: false  <-- Data is NOT compressed
  Read 8 compressed bytes
  First 20 bytes: 80 c0 02 01 00 80 c0 02
  Attempting ZSTD decompression...
  ✗ Decompression failed: Error: invalid zstd data
```

The page data was only 8 bytes and not ZSTD-compressed, but we were trying to decompress it anyway.

## Solution

Added a check for the `is_compressed` flag before attempting decompression:

```typescript
// For DATA_PAGE_V2, check the is_compressed flag
let uncompressedBytes: Uint8Array
if (page.dataPageHeaderV2 && page.dataPageHeaderV2.is_compressed === false) {
  // Data is already uncompressed
  uncompressedBytes = new Uint8Array(compressedData)
} else {
  // Normal decompression based on codec
  uncompressedBytes = decompressPageData(
    compressedData,
    uncompressedSize,
    codec
  )
}
```

## Files Modified

1. **`src/lib/parquet-parsing-core.ts`** (line 996-1009)
   - Updated `parseColumnChunkPageSizes()` to check `is_compressed` flag

2. **`webapp/src/components/PagesView.tsx`** (line 71-84)
   - Updated page breakdown parsing to check `is_compressed` flag

## Testing

### Before Fix
- V2 pages with `is_compressed: false` → ✗ Failed with "invalid zstd data"
- V1 pages with ZSTD compression → ✓ Working

### After Fix
- V2 pages with `is_compressed: false` → ✓ Working (null counts extracted: 83/84 pages)
- V2 pages with `is_compressed: true` → ✓ Working (normal decompression)
- V1 pages with ZSTD compression → ✓ Working
- All 54 tests pass ✓
- Webapp builds successfully ✓

## Background

According to the Parquet specification, DATA_PAGE_V2 includes an `is_compressed` flag:
- When `true` or undefined: page data is compressed using the column's codec
- When `false`: page data is uncompressed, even if the column codec is not UNCOMPRESSED

This allows writers to selectively compress pages based on effectiveness. For example:
- Very small pages may not benefit from compression
- Dictionary pages may be left uncompressed
- Pages with low entropy data may compress poorly

## Related Files

- `NULL-COUNT-FEATURE.md` - Documents the null count feature that led to discovering this bug
- `PAGE-PARSING-API.md` - API documentation (updated to note this behavior)
- `debug-zstd.ts` - Debug script that helped identify the issue
- `debug-null-count-v2.ts` - Test script showing V2 null count extraction now works

## Impact

This fix enables:
1. ✅ Correct parsing of DATA_PAGE_V2 pages with `is_compressed: false`
2. ✅ Null count extraction from V2 pages (previously failing)
3. ✅ Page size breakdown for V2 pages (previously failing)
4. ✅ Support for mixed compression within a column chunk
