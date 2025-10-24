import { useState } from 'react'
import './App.css'
import type { ParquetPageMetadata } from '../../src/lib/parquet-parsing'
import FileUpload from './components/FileUpload'
import InfoView from './components/InfoView'
import SchemaView from './components/SchemaView'
import PagesView from './components/PagesView'
import MetadataView from './components/MetadataView'
import StructureView from './components/StructureView'

type TabType = 'structure' | 'info' | 'schema' | 'pages' | 'metadata'

function App() {
  const [metadata, setMetadata] = useState<ParquetPageMetadata | null>(null)
  const [fileName, setFileName] = useState<string>('')
  const [file, setFile] = useState<File | null>(null)
  const [activeTab, setActiveTab] = useState<TabType>('structure')

  const handleFileLoaded = (name: string, meta: ParquetPageMetadata, uploadedFile: File) => {
    setFileName(name)
    setMetadata(meta)
    setFile(uploadedFile)
    setActiveTab('structure')
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Parquet File Visualizer</h1>
        {fileName && <p className="file-name">File: {fileName}</p>}
      </header>

      <main className="app-main">
        {!metadata ? (
          <FileUpload onFileLoaded={handleFileLoaded} />
        ) : (
          <>
            <nav className="tabs">
              <button
                className={activeTab === 'structure' ? 'active' : ''}
                onClick={() => setActiveTab('structure')}
              >
                Structure
              </button>
              <button
                className={activeTab === 'info' ? 'active' : ''}
                onClick={() => setActiveTab('info')}
              >
                Info
              </button>
              <button
                className={activeTab === 'schema' ? 'active' : ''}
                onClick={() => setActiveTab('schema')}
              >
                Schema
              </button>
              <button
                className={activeTab === 'pages' ? 'active' : ''}
                onClick={() => setActiveTab('pages')}
              >
                Pages
              </button>
              <button
                className={activeTab === 'metadata' ? 'active' : ''}
                onClick={() => setActiveTab('metadata')}
              >
                Metadata
              </button>
              <button
                className="reset-button"
                onClick={() => {
                  setMetadata(null)
                  setFileName('')
                  setFile(null)
                }}
              >
                Load Different File
              </button>
            </nav>

            <div className="tab-content">
              {activeTab === 'structure' && <StructureView metadata={metadata} />}
              {activeTab === 'info' && <InfoView metadata={metadata} />}
              {activeTab === 'schema' && <SchemaView metadata={metadata} />}
              {activeTab === 'pages' && file && <PagesView metadata={metadata} file={file} />}
              {activeTab === 'metadata' && <MetadataView metadata={metadata} />}
            </div>
          </>
        )}
      </main>
    </div>
  )
}

export default App
