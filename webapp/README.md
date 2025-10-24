# Parquet File Visualizer - Web Application

A modern web application for visualizing Apache Parquet file metadata and structure. Built with React, TypeScript, and Vite, using the hyparquet library for Parquet file parsing.

## Features

- **File Upload**: Drag-and-drop or click to upload Parquet files
- **Structure View**: Visual representation of the file layout showing headers, row groups, column chunks, and footer
- **Info View**: Comprehensive file statistics including compression ratios, encoding information, and aggregate statistics
- **Schema View**: Interactive schema browser with expandable column details and statistics
- **Pages View**: Page-level analysis with filters for exploring individual pages within column chunks
- **Metadata View**: Display of file-level and column-level key-value metadata

## Getting Started

### Prerequisites

- Node.js (v16 or higher)
- npm or yarn

### Installation

```bash
cd webapp
npm install
```

### Development

Start the development server:

```bash
npm run dev
```

The application will be available at `http://localhost:5173/`

### Production Build

Build the application for production:

```bash
npm run build
```

The built files will be in the `dist/` directory.

Preview the production build:

```bash
npm run preview
```

## Usage

1. **Upload a Parquet File**:
   - Drag and drop a `.parquet` file onto the upload area
   - Or click "Choose File" to select a file from your system

2. **Explore Views**:
   - **Structure**: See the physical layout of your Parquet file
   - **Info**: View file statistics, compression details, and aggregate metrics
   - **Schema**: Browse the schema and column details with expandable cards
   - **Pages**: Dive into page-level metadata for each column chunk
   - **Metadata**: View custom key-value metadata if present in the file

3. **Load Different File**: Click the "Load Different File" button to analyze another Parquet file

## Technology Stack

- **React 18**: UI framework
- **TypeScript**: Type-safe development
- **Vite**: Fast build tool and dev server
- **hyparquet**: Pure JavaScript Parquet parser
- **CSS3**: Modern styling with flexbox and grid layouts

## Project Structure

```
webapp/
├── src/
│   ├── components/          # React components
│   │   ├── FileUpload.tsx   # File upload with drag-and-drop
│   │   ├── StructureView.tsx # File layout visualization
│   │   ├── InfoView.tsx     # File information display
│   │   ├── SchemaView.tsx   # Schema browser
│   │   ├── PagesView.tsx    # Page-level analysis
│   │   └── MetadataView.tsx # Metadata display
│   ├── lib/
│   │   └── parquet-parsing.ts # Parquet parsing library
│   ├── App.tsx              # Main application component
│   ├── App.css              # Application styles
│   └── main.tsx             # Application entry point
├── public/                  # Static assets
├── package.json
├── tsconfig.json            # TypeScript configuration
└── vite.config.ts           # Vite configuration
```

## Browser Support

The application works in all modern browsers that support ES2020+ features:
- Chrome/Edge 90+
- Firefox 88+
- Safari 14+

## License

See the root project LICENSE file.
