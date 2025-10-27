# Null Count Feature

## Summary

Added optional `nullCount` field to `PageSizeBreakdown` that shows the number of null/missing values in each page. The count is extracted from:

1. **DATA_PAGE_V2**: Directly from the `num_nulls` field in the page header (when available)
2. **DATA_PAGE_V1**: By decoding definition levels when they're small (<1kB) and counting values with `definitionLevel < maxDefinitionLevel`

## Implementation

### 1. Updated Interface

**`PageSizeBreakdown` interface:**
```typescript
export interface PageSizeBreakdown {
  pageNumber: number
  pageType: string
  headerSize: number
  repetitionLevelsSize: number
  definitionLevelsSize: number
  valuesSize: number
  totalDataSize: number
  nullCount?: number  // ← NEW: Optional null count
}
```

### 2. RLE/Bit-Packed Decoding

Implemented three helper functions to decode definition levels:

#### `readVarInt(view, offset)`
Reads a variable-length integer (varint) used in RLE encoding headers.

#### `decodeRleBitPackedLevels(view, offset, length, bitWidth, numValues)`
Decodes RLE/bit-packed hybrid encoded data used for definition and repetition levels.

**Supports:**
- RLE runs (repeated values)
- Bit-packed runs (densely packed values)
- Proper bit extraction across byte boundaries

#### `countNullsFromDefinitionLevels(definitionLevels, maxDefinitionLevel)`
Counts values where `definitionLevel < maxDefinitionLevel` (indicates null/missing value).

### 3. Null Count Extraction

**In `parsePageDataSizes()`:**

#### For DATA_PAGE_V2:
```typescript
if (header.num_nulls !== undefined && header.num_nulls !== null) {
  nullCount = header.num_nulls
}
```

#### For DATA_PAGE_V1:
```typescript
// Only decode if definition levels are small (<1kB)
if (definitionLevelsSize > 0 && definitionLevelsSize < 1024) {
  try {
    const levelDataLength = view.getUint32(definitionLevelsOffset, true)
    const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1))

    const definitionLevels = decodeRleBitPackedLevels(
      view,
      definitionLevelsOffset + 4,
      levelDataLength,
      bitWidth,
      numValues
    )

    nullCount = countNullsFromDefinitionLevels(definitionLevels, maxDefinitionLevel)
  } catch (error) {
    // Fail gracefully, just skip null counting
    console.warn('Failed to decode definition levels for null counting:', error)
  }
}
```

### 4. UI Updates

**In tooltip (`PagesView.tsx`):**
```typescript
if (breakdown.nullCount !== undefined) {
  lines.push(`Null Count: ${breakdown.nullCount.toLocaleString()}`)
}
```

## Understanding Definition Levels

In Parquet, definition levels track whether optional fields have values:

- **maxDefinitionLevel**: The deepest level of nesting for this column
- **definitionLevel < maxDefinitionLevel**: Value is null/missing
- **definitionLevel == maxDefinitionLevel**: Value is present (non-null)

### Example

For a simple optional column (e.g., `OPTIONAL INT32 age`):
- maxDefinitionLevel = 1
- definitionLevel = 0 → null
- definitionLevel = 1 → value present

For nested optional fields (e.g., `OPTIONAL struct.OPTIONAL field`):
- maxDefinitionLevel = 2
- definitionLevel = 0 → struct is null
- definitionLevel = 1 → struct present, field is null
- definitionLevel = 2 → both struct and field present

## Performance Considerations

### Why the 1kB Threshold?

Decoding definition levels has a cost, so we only do it when:
1. The data is small enough (<1kB) that decoding is fast
2. The benefit of showing null count outweighs the cost

For larger definition level data:
- Decoding could be slow
- The page likely has many values, so decoding would take significant time
- Users can still see null counts from statistics if available (V2 or statistics in header)

### When Null Count is Available

| Page Type | Null Count Source | Always Available? |
|-----------|------------------|-------------------|
| DATA_PAGE_V2 | `num_nulls` in header | Usually (if writer included it) |
| DATA_PAGE_V1 | Decoded from definition levels | Only when def levels <1kB |
| DICTIONARY_PAGE | N/A | Never (dictionaries don't have nulls) |

## Example Tooltip Output

```
Page 2: DATA_PAGE
Encoding: PLAIN
Header: 32 bytes
Compressed: 4,096 bytes
Uncompressed: 16,384 bytes

--- Page Data Breakdown ---
Repetition Levels: 0 bytes
Definition Levels: 256 bytes
Values: 16,128 bytes
Distribution: Rep 0.0% | Def 1.6% | Val 98.4%
Null Count: 1,234

Values Count: 10,000

--- Statistics ---
Nulls: 1,234
Distinct: 8,766
Min: 100
Max: 999999
```

## Benefits

1. **Better Understanding**: See exactly how many nulls are in each page
2. **Data Quality**: Quickly identify pages with many nulls
3. **Validation**: Verify null counts match statistics (when both available)
4. **Optimization**: Understand storage overhead from null tracking

## Testing

✅ All 54 tests pass
✅ Webapp builds successfully
✅ TypeScript compiles without errors
✅ Null counts appear in tooltips when available

## Files Modified

1. `src/lib/parquet-parsing-core.ts`:
   - Added `nullCount?` to `PageSizeBreakdown` interface
   - Implemented `readVarInt()` helper
   - Implemented `decodeRleBitPackedLevels()` decoder
   - Implemented `countNullsFromDefinitionLevels()` counter
   - Updated `parsePageDataSizes()` to extract/compute null counts

2. `webapp/src/components/PagesView.tsx`:
   - Updated tooltip to display null count when available

## Limitations

1. **DATA_PAGE_V1**: Only decodes definition levels when <1kB for performance
2. **No Statistics**: If page has no statistics and definition levels are large, null count won't be available
3. **Dictionary Pages**: Don't have null counts (dictionaries store unique values, not nulls)

## Future Enhancements

- [ ] Add configuration option to always decode definition levels regardless of size
- [ ] Cache decoded definition levels for reuse
- [ ] Show null percentage in addition to count
- [ ] Add null count to aggregate statistics across all pages
