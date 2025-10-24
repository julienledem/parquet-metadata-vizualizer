# Claude Parquet Visualizer - Project Specification

## Overview

A client-side browser application for visualizing and exploring Apache Parquet files. Users can upload Parquet files directly in their browser and view insights into file structure, schema, metadata, and data content. All processing happens locally in the browser - no data is sent to any server.

## Goals

- Enable users to quickly understand the structure and contents of Parquet files through a web interface
- Provide a privacy-focused tool that processes files entirely in the browser
- Support reasonably large files efficiently without loading entire datasets into memory
- Display schema information, statistics, and metadata in a human-readable, interactive format
- Ensure no data leaves the user's machine

## Features

### Core Features

#### 1. File Structure Visualization
- Visual representation of physical file layout showing:
  - Header (magic number) size and position
  - Each column chunk size and position within row groups
  - Footer size and position
  - Overall file size breakdown
- Display as a proportional diagram or ASCII visualization
- Show byte offsets and sizes for each component

#### 2. File Information Display
- File size and path
- Parquet version
- Number of row groups
- Total row count
- Compression codec information
- Created by metadata

#### 3. Column Chunk and Page Analysis
- For each column in each row group, display:
  - Column chunk size and offset
  - Number of pages in the column chunk
  - For each page within the column:
    - Page type (data page, dictionary page, index page)
    - Encoding type (PLAIN, RLE, DELTA_BINARY_PACKED, etc.)
    - Compression codec used
    - Compressed size vs uncompressed size
    - Compression ratio (%)
    - Number of values in the page
    - Number of null values
    - Page-level statistics (min, max, null count, distinct count if available)
    - All available page metadata
- Summary statistics per column:
  - Total number of pages
  - Average compression ratio
  - Total compressed vs uncompressed size
  - Encoding distribution across pages

#### 4. Schema Visualization
- Column names and types
- Nested structure representation (for complex types)
- Logical types (timestamps, decimals, etc.)
- Repetition levels (required, optional, repeated)
- Column statistics (min, max, null count, distinct count where available)

#### 5. Data Preview
- Display first N rows of data
- Support for pagination or row range selection
- Formatted output for different data types
- Handling of nested and complex types (structs, lists, maps)

#### 6. Metadata Inspection
- Display all metadata found in the file footer, including:
  - All key-value metadata pairs (both standard and custom/unknown metadata)
  - Schema-level metadata
  - Column-level metadata
  - Row group metadata
  - File-level metadata
- Show unknown or custom metadata keys without filtering
- Display metadata in both human-readable format and raw form when applicable
- Preserve and display metadata that doesn't match known Parquet specifications

### Optional/Future Features

- Column filtering (show specific columns only)
- Search/filter data by conditions
- Export to CSV or JSON
- Performance metrics (read time, decompression time)
- Visual graphs for data distribution
- Comparison between multiple Parquet files
- Dark mode support
- Drag-and-drop file upload
- Save/load visualization settings
- Shareable links with file structure (without data)

## User Interface

### Web Interface

The application will provide a single-page web interface with:

#### File Upload Section
- File upload button with drag-and-drop support
- Display uploaded file name and size
- Clear/remove file option
- Privacy notice: "All processing happens in your browser"

#### Navigation/Tabs
- **Structure** - Visual file layout diagram
- **Info** - File information and statistics
- **Schema** - Schema tree and column information
- **Pages** - Detailed page-level analysis
- **Metadata** - File and column metadata
- **Data Preview** - Tabular view of data rows

#### Structure View
- Interactive visual diagram showing:
  - Header section (4 bytes) with "PAR1" magic number
  - Row groups with column chunks (proportionally sized boxes/bars)
  - Footer section with size
- Hover to see detailed byte offsets and sizes
- Color-coded sections for easy identification
- Expandable sections to see column chunk details

#### Info View
- File size and name
- Parquet version
- Number of row groups
- Total row count
- Compression codec information
- Created by metadata

#### Schema View
- Expandable/collapsible tree view for nested structures
- Column details panel showing:
  - Physical type
  - Logical type
  - Repetition level (required, optional, repeated)
  - Column-level statistics (when available)

#### Pages View
- Per-column, per-row-group breakdown showing:
  - Number of pages per column chunk
  - Page types (data, dictionary, index)
  - Encoding types used
  - Compressed vs uncompressed sizes
  - Compression ratios
  - Value counts and null counts
  - Page-level statistics
- Filterable by column or row group
- Summary statistics

#### Data Preview
- Paginated table view
- Column headers with type information
- Row number indicators
- Configurable page size (10, 50, 100, 500 rows)
- Proper formatting for different data types
- Handling of nested structures

#### Metadata View
- Key-value pairs in a clean table format
- Separate sections for file-level and column-level metadata
- Display of unknown/custom metadata

### Design Considerations
- Clean, modern UI with good typography
- Responsive layout that works on different screen sizes
- Loading indicators for file processing
- Error messages for invalid or corrupted files
- Accessible (keyboard navigation, ARIA labels)
- Performance considerations for large files

## Technical Details

### Parquet File Format Structure

Understanding the Parquet file format is crucial for implementing the visualizer:

#### Physical Layout
```
[4-byte Magic Number: "PAR1"]
[Row Group 1]
  [Column Chunk 1 Data]
  [Column Chunk 2 Data]
  ...
[Row Group 2]
  [Column Chunk 1 Data]
  [Column Chunk 2 Data]
  ...
[File Metadata]
[4-byte Footer Length]
[4-byte Magic Number: "PAR1"]
```

#### Key Components to Parse

1. **Header (Magic Number)**
   - First 4 bytes: "PAR1"
   - Used to verify file is valid Parquet format

2. **Row Groups**
   - Horizontal partitions of the data
   - Each row group contains column chunks for all columns
   - Row groups allow for parallel processing and efficient filtering

3. **Column Chunks**
   - Vertical partitions within a row group
   - Contains all data for a single column within that row group
   - Includes:
     - Column metadata (encoding, compression, statistics)
     - Data pages (actual column data)
     - Dictionary pages (optional, for dictionary encoding)

4. **Footer**
   - Located at end of file
   - Contains:
     - File schema
     - Row group metadata (location, size, column statistics)
     - Key-value metadata
     - Version information
   - Last 8 bytes are: [4-byte footer length][4-byte "PAR1" magic]

### Implementation Approach

#### Phase 1: Footer and Metadata Reading
1. Seek to end of file
2. Read last 8 bytes to get footer length
3. Verify magic number
4. Seek back by footer length and read footer metadata
5. Parse schema and row group metadata

#### Phase 2: File Structure Analysis
1. Calculate header size (4 bytes)
2. Extract row group byte offsets and sizes from metadata
3. For each row group, extract column chunk offsets and sizes
4. Calculate footer size from metadata
5. Build a map of file structure with all components

#### Phase 3: Data Reading
1. Use row group metadata to locate specific data
2. Read only required column chunks
3. Decompress data (handle different compression codecs)
4. Decode pages based on encoding type
5. Reconstruct columnar data into rows for display

### Technology Stack

#### Recommended Stack
- **Frontend Framework**: React or Vue.js (for component-based UI)
- **Language**: TypeScript (for type safety)
- **Parquet Library**: hyparquet (pure JavaScript implementation)
- **File Reading**: File API (native browser support)
- **UI Components**:
  - Component library (e.g., shadcn/ui, Material-UI, or Ant Design)
  - Data tables: react-table or similar
- **Visualization**:
  - D3.js or SVG for file structure diagram
  - CSS for responsive layout
- **Build Tool**: Vite (fast development and optimized builds)
- **Testing**: Vitest + React Testing Library

#### Architecture
- **src/components/** - React components for UI
- **src/lib/parquet-parsing.ts** - All Parquet file parsing logic using hyparquet, isolated in one file
- **src/types/** - TypeScript type definitions
- **src/utils/** - Utility functions for formatting, etc.

### Browser-Based File Reading Strategy

For file structure visualization in the browser:

1. **File Upload**
   - Use HTML5 File API to handle uploaded files
   - Access file as Blob/ArrayBuffer
   - No server upload required - all processing client-side

2. **Metadata Parsing (in parquet-parsing.ts)**
   - Use hyparquet to read and parse Parquet metadata
   - Extract schema, row group info, column chunk offsets
   - All parquet format parsing isolated in dedicated file
   - hyparquet handles the Thrift binary parsing internally

4. **Size Calculations**
   ```typescript
   // Pseudocode for structure analysis
   - header_size = 4 bytes
   - footer_length = read last 4 bytes before magic number
   - footer_size = footer_length + 8
   - file_size = file.size (from File API)

   for each row_group in metadata.row_groups:
       for each column_chunk in row_group.columns:
           - offset = column_chunk.file_offset
           - size = column_chunk.total_compressed_size
   ```

5. **Streaming Large Files**
   - Use file.slice() to read specific byte ranges
   - Avoid loading entire file into memory
   - Read only required sections on demand

### Performance Considerations

1. **Lazy Loading**
   - Don't read actual data until requested
   - Load only metadata initially (typically < 1MB even for large files)
   - Parse metadata once and cache results

2. **Streaming for Large Files**
   - Process row groups one at a time
   - Use iterators for data preview
   - Read only required byte ranges using file.slice()

3. **Caching**
   - Cache parsed metadata in memory after initial load
   - Avoid re-parsing footer for multiple operations
   - Cache decoded Thrift structures

4. **Memory Limits**
   - Set maximum rows to read at once (default: 100)
   - Warn user for very wide tables (many columns)
   - Use virtual scrolling for large data previews

5. **Code Organization for Maintainability**
   - All Parquet file parsing in parquet-parsing.ts using hyparquet (one source of truth)
   - Reuse encoding/compression enum definitions across views
   - Avoid duplicating parsing logic for encodings or compression algorithms
   - Share type definitions between parser and UI components

### File Support
- Support for local files via browser File API
- Files processed entirely client-side (no server upload)
- Future: Support for URLs to files (with CORS support)
- Future: Handle both single files and partitioned datasets

### Error Handling

- Invalid magic number: Clear error that file is not Parquet format
- Corrupted footer: Attempt partial recovery, show what's readable
- Unsupported compression: List supported codecs, suggest alternatives
- Large files: Warn and provide options to limit data read

## Success Criteria

- Successfully parse and display information from standard Parquet files in the browser
- Handle files up to several hundred MB efficiently (browser memory constraints)
- Provide clear, accurate schema representation
- Complete core features with intuitive web interface
- Fast initial load time (< 2 seconds for metadata parsing and schema display)
- All processing happens client-side with no server required
- Works on modern browsers (Chrome, Firefox, Safari, Edge)

## Non-Goals

- Modifying or writing Parquet files (read-only tool)
- Complex data analysis or aggregations
- Built-in data visualization charts (initial version)
- Full dataset scanning for statistics

## Timeline

1. Phase 1: Project setup and basic file upload
2. Phase 2: Parquet metadata parsing using hyparquet (parquet-parsing.ts)
3. Phase 3: File structure visualization
4. Phase 4: Schema and info display
5. Phase 5: Page-level analysis view
6. Phase 6: Data preview functionality
7. Phase 7: Metadata inspection
8. Phase 8: UI polish and responsive design
9. Phase 9: Optional features based on feedback

## Development Notes

### Key Principles
- **Privacy First**: All processing must happen in the browser
- **Single Source of Truth**: Parquet parsing using hyparquet in one file (parquet-parsing.ts)
- **No Duplication**: Reuse enum definitions and parsing logic across components
- **Performance**: Lazy load data, cache parsed metadata
- **Maintainability**: Clear separation between parsing logic and UI components
