#!/usr/bin/env node
/**
 * Generate a single self-contained HTML file for the Parquet Visualizer
 *
 * This script:
 * 1. Builds the webapp using Vite
 * 2. Reads the generated CSS and JS files
 * 3. Combines them into a single HTML file with inlined styles and scripts
 * 4. Outputs to parquet-visualizer.html
 *
 * Usage:
 *   npm run generate-html
 *   or
 *   npx tsx generate-html.ts
 */

import { execSync } from 'child_process'
import { readFileSync, writeFileSync, readdirSync, statSync } from 'fs'
import { join } from 'path'

const WEBAPP_DIR = join(process.cwd(), 'webapp')
const DIST_DIR = join(WEBAPP_DIR, 'dist')
const ASSETS_DIR = join(DIST_DIR, 'assets')
const OUTPUT_FILE = join(process.cwd(), 'parquet-visualizer.html')

function main() {
  console.log('🔨 Building webapp...')

  try {
    // Build the webapp
    execSync('npm run build', {
      cwd: WEBAPP_DIR,
      stdio: 'inherit'
    })
  } catch (error) {
    console.error('❌ Build failed:', error)
    process.exit(1)
  }

  console.log('✅ Build completed')
  console.log('📦 Reading generated files...')

  // Find the generated CSS and JS files
  const files = readdirSync(ASSETS_DIR)
  const cssFile = files.find(f => f.startsWith('index-') && f.endsWith('.css'))
  const jsFile = files.find(f => f.startsWith('index-') && f.endsWith('.js'))

  if (!cssFile || !jsFile) {
    console.error('❌ Could not find generated CSS or JS files in:', ASSETS_DIR)
    console.error('   Found files:', files)
    process.exit(1)
  }

  console.log(`   CSS: ${cssFile}`)
  console.log(`   JS:  ${jsFile}`)

  // Read the files
  const cssContent = readFileSync(join(ASSETS_DIR, cssFile), 'utf-8')
  const jsContent = readFileSync(join(ASSETS_DIR, jsFile), 'utf-8')

  console.log('✍️  Generating single HTML file...')

  // Create the combined HTML
  const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Parquet Visualizer</title>
    <meta name="description" content="Visualize Parquet file structure, metadata, schema, and pages" />
    <style>
${cssContent}
    </style>
  </head>
  <body>
    <div id="root"></div>
    <script type="module">
${jsContent}
    </script>
  </body>
</html>
`

  // Write the output file
  writeFileSync(OUTPUT_FILE, html, 'utf-8')

  // Get file size
  const stats = statSync(OUTPUT_FILE)
  const sizeKB = (stats.size / 1024).toFixed(1)

  console.log('✅ Generated parquet-visualizer.html')
  console.log(`   Size: ${sizeKB} KB`)
  console.log(`   Location: ${OUTPUT_FILE}`)
  console.log('')
  console.log('🎉 Done! You can now open parquet-visualizer.html in your browser.')
}

main()
