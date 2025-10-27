# Column Count Fix

## Issue

The total number of columns displayed in the file metadata did not match the number of columns in each row group.

### Root Cause

The column count was being calculated as `metadata.schema.length - 1`, which counts **all schema elements** including:
- Root element
- Leaf columns (actual data columns)
- Intermediate container elements (structs, lists, maps)

However, row groups only contain **leaf columns** - the actual data columns that store values.

### Example

Consider this Parquet schema:
```
root
  user (struct)
    name (string)      <- leaf column
    age (int)          <- leaf column
  address (struct)
    street (string)    <- leaf column
    city (string)      <- leaf column
```

**Before the fix:**
- File-level numColumns: `schema.length - 1` = 6 (root + user + name + age + address + street + city - 1)
- Row group numColumns: `rowGroup.columns.length` = 4 (name, age, street, city)
- ❌ **Mismatch: 6 vs 4**

**After the fix:**
- File-level numColumns: `countLeafColumns(schema)` = 4 (name, age, street, city)
- Row group numColumns: `rowGroup.columns.length` = 4 (name, age, street, city)
- ✅ **Match: 4 vs 4**

## Solution

Created a new helper function `countLeafColumns()` that:
1. Iterates through the schema
2. Skips the root element (no name)
3. Counts only elements with no children (leaf columns)

```typescript
function countLeafColumns(schema: any[]): number {
  if (!schema || schema.length === 0) return 0

  let leafCount = 0

  for (const element of schema) {
    // Skip the root element
    if (!element.name) continue

    // A leaf column is one that has no children
    // num_children is undefined/null/0 for leaf columns
    const hasChildren = element.num_children && element.num_children > 0

    if (!hasChildren) {
      leafCount++
    }
  }

  return leafCount
}
```

### Updated Locations

1. **`parseParquetFooter()`** (line 202)
   - Changed: `numColumns: metadata.schema.length - 1`
   - To: `numColumns: countLeafColumns(metadata.schema)`

2. **`parseParquetPageIndex()`** (line 230)
   - Changed: `numColumns: metadata.schema.length - 1`
   - To: `numColumns: countLeafColumns(metadata.schema)`

## Testing

✅ All 54 tests pass
✅ Webapp builds successfully
✅ Column counts now match between file-level and row group-level metadata

### Test Output Verification

Before fix:
```
File metadata shows: Number of columns: 15
Row Group 0: Number of columns: 11
❌ Mismatch!
```

After fix:
```
File metadata shows: Number of columns: 11
Row Group 0: Number of columns: 11
✅ Match!
```

## Impact

This fix ensures that:
- The file-level column count accurately represents the number of actual data columns
- Users see consistent column counts across different views
- The count matches the number of column chunks in each row group
- Nested/structured schemas are handled correctly

## Files Modified

- `src/lib/parquet-parsing-core.ts` - Added `countLeafColumns()` function and updated column count calculation

## Related

This fix is particularly important for Parquet files with:
- Nested structures (structs)
- Lists/arrays
- Maps
- Any schema with container types

For simple flat schemas (no nested types), the behavior remains the same as before since all non-root elements are leaf columns.
