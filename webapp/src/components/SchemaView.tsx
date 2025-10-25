import type { ParquetPageMetadata } from '../../../src/lib/parquet-parsing'
import './SchemaView.css'

interface SchemaViewProps {
  metadata: ParquetPageMetadata
}

function SchemaView({ metadata }: SchemaViewProps) {
  const { fileMetadata } = metadata

  // Convert schema to text representation
  const schemaToText = (schema: any[]): string => {
    const lines: string[] = []
    let currentIndex = 0

    const processElement = (depth: number): void => {
      if (currentIndex >= schema.length) return

      const element = schema[currentIndex]
      const indent = '  '.repeat(depth)

      // Handle root element
      if (depth === 0 && element.name === 'schema') {
        lines.push('message schema {')
        currentIndex++

        // Process all children
        const numChildren = element.num_children || 0
        for (let i = 0; i < numChildren; i++) {
          processElement(depth + 1)
        }

        lines.push('}')
        return
      }

      // Get repetition type
      let repetition = ''
      if (element.repetition_type === 0) repetition = 'required'
      else if (element.repetition_type === 1) repetition = 'optional'
      else if (element.repetition_type === 2) repetition = 'repeated'

      // Check if this is a group (has children)
      const numChildren = element.num_children || 0
      const isGroup = numChildren > 0

      if (isGroup) {
        // Group element
        let groupLine = `${indent}${repetition} group ${element.name}`

        // Add logical type if present
        if (element.logicalType) {
          const logicalTypeStr = typeof element.logicalType === 'string'
            ? element.logicalType
            : JSON.stringify(element.logicalType)
          groupLine += ` (${logicalTypeStr})`
        }

        groupLine += ' {'
        lines.push(groupLine)
        currentIndex++

        // Process children
        for (let i = 0; i < numChildren; i++) {
          processElement(depth + 1)
        }

        lines.push(`${indent}}`)
      } else {
        // Leaf element (primitive type)
        let typeName = element.type || 'UNKNOWN'

        // Add logical type annotation if present
        let typeAnnotation = ''
        if (element.logicalType) {
          if (typeof element.logicalType === 'string') {
            typeAnnotation = ` (${element.logicalType})`
          } else if (element.logicalType.TIMESTAMP) {
            const ts = element.logicalType.TIMESTAMP
            typeAnnotation = ` (TIMESTAMP(${ts.unit},${ts.isAdjustedToUTC}))`
          } else if (element.logicalType.INTEGER) {
            const int = element.logicalType.INTEGER
            typeAnnotation = ` (INTEGER(${int.bitWidth},${int.isSigned}))`
          } else if (element.logicalType.DECIMAL) {
            const dec = element.logicalType.DECIMAL
            typeAnnotation = ` (DECIMAL(${dec.precision},${dec.scale}))`
          } else {
            typeAnnotation = ` (${JSON.stringify(element.logicalType)})`
          }
        }

        lines.push(`${indent}${repetition} ${typeName.toLowerCase()} ${element.name}${typeAnnotation};`)
        currentIndex++
      }
    }

    processElement(0)
    return lines.join('\n')
  }

  const schemaText = schemaToText(fileMetadata.schema)

  return (
    <div className="schema-view">
      <section className="schema-section">
        <h2>Schema Definition</h2>
        <p className="schema-description">
          Parquet schema showing the structure and types of columns in the file.
        </p>
        <pre className="schema-text">
          <code>{schemaText}</code>
        </pre>
      </section>
    </div>
  )
}

export default SchemaView
