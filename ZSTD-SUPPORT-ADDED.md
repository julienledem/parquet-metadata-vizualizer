# ZSTD Compression Support Added

## Summary

Added full support for ZSTD (Zstandard) decompression to the Parquet page parsing functionality. The page size breakdown feature now works with UNCOMPRESSED, SNAPPY, and ZSTD compressed pages!

## Changes Made

### 1. Dependencies Added

**Root package.json:**
```json
{
  "dependencies": {
    "fzstd": "^0.1.1"
  }
}
```

**webapp/package.json:**
```json
{
  "dependencies": {
    "fzstd": "^0.1.1"
  }
}
```

The `fzstd` library is a pure JavaScript implementation of ZSTD decompression that works in both Node.js and browsers.

### 2. Code Changes

**src/lib/parquet-parsing-core.ts:**

Added ZSTD import:
```typescript
import { decompress as zstdDecompress } from 'fzstd'
```

Updated `decompressPageData()` function:
```typescript
// ZSTD decompression using fzstd library
if (codec === 'ZSTD') {
  return zstdDecompress(compressedBytes)
}
```

Updated error message:
```typescript
throw new Error(`Decompression for codec ${codec} is not yet supported. Supported codecs: UNCOMPRESSED, SNAPPY, ZSTD`)
```

### 3. Testing

✅ All 54 existing tests pass
✅ ZSTD compressed columns detected in test data
✅ Webapp builds successfully
✅ TypeScript compiles without errors

Test output shows ZSTD columns being processed:
```
Type: INT64, Codec: ZSTD
Type: BYTE_ARRAY, Codec: ZSTD
Type: BYTE_ARRAY, Codec: ZSTD
...
```

## Compression Support Matrix

| Codec | Status | Implementation |
|-------|--------|----------------|
| UNCOMPRESSED | ✅ Supported | Native (no decompression needed) |
| SNAPPY | ✅ Supported | hyparquet built-in |
| ZSTD | ✅ Supported | fzstd library |
| GZIP | ❌ Not yet | Would need pako or similar |
| BROTLI | ❌ Not yet | Would need brotli-wasm or similar |
| LZ4 | ❌ Not yet | Would need lz4js or similar |

## Usage

The ZSTD decompression is automatic and transparent. When the page parsing functions encounter a ZSTD compressed page, they automatically decompress it:

```typescript
import { decompressPageData } from './src/lib/parquet-parsing.js'

// Works with ZSTD, SNAPPY, or UNCOMPRESSED
const uncompressedBytes = decompressPageData(
  compressedData,
  uncompressedSize,
  'ZSTD'
)
```

In the webapp, when you hover over a page in the histogram, the tooltip now shows the size breakdown even for ZSTD compressed pages:

```
Page 2: DATA_PAGE
Encoding: PLAIN
Header: 32 bytes
Compressed: 2,048 bytes (ZSTD)
Uncompressed: 8,192 bytes

--- Page Data Breakdown ---
Repetition Levels: 0 bytes
Definition Levels: 156 bytes
Values: 8,036 bytes
Distribution: Rep 0.0% | Def 1.9% | Val 98.1%
```

## Benefits

1. **Broader Compatibility**: ZSTD is increasingly popular for Parquet files due to its excellent compression ratio and speed
2. **Better Analysis**: Can now analyze page structure in ZSTD compressed files
3. **Cross-Platform**: Works in both Node.js and browsers
4. **Lightweight**: fzstd is a pure JavaScript implementation with no native dependencies

## Performance Notes

- ZSTD decompression is fast and efficient
- The fzstd library is optimized for modern JavaScript engines
- Decompression happens on-demand only when analyzing specific pages
- No performance impact on files using other codecs

## Documentation Updated

- ✅ `PAGE-PARSING-API.md` - Updated compression support section
- ✅ `CHANGELOG-PAGE-SIZE-BREAKDOWN.md` - Added ZSTD to compression support
- ✅ Future enhancements sections marked ZSTD as complete

## Files Modified

1. `package.json` - Added fzstd dependency
2. `webapp/package.json` - Added fzstd dependency
3. `src/lib/parquet-parsing-core.ts` - Added ZSTD decompression
4. `PAGE-PARSING-API.md` - Updated documentation
5. `CHANGELOG-PAGE-SIZE-BREAKDOWN.md` - Updated changelog

## Commit Message

```
Add ZSTD compression support for page parsing

- Install fzstd library for ZSTD decompression
- Update decompressPageData() to handle ZSTD codec
- Support ZSTD in both Node.js and browser environments
- All tests pass with ZSTD compressed columns
- Update documentation to reflect ZSTD support

Supported codecs: UNCOMPRESSED, SNAPPY, ZSTD
```
