# Generating a Single HTML File

The `generate-html.ts` script creates a self-contained, single-file HTML version of the Parquet Visualizer that can be shared and used without any build process.

## Quick Start

```bash
npm run generate-html
```

This will:
1. Build the webapp using Vite
2. Read the generated CSS and JavaScript files
3. Inline them into a single HTML file
4. Output to `parquet-visualizer.html` (264.9 KB)

## Usage

### Option 1: Using npm script (recommended)
```bash
npm run generate-html
```

### Option 2: Direct execution
```bash
npx tsx generate-html.ts
```

### Option 3: Make it executable
```bash
chmod +x generate-html.ts
./generate-html.ts
```

## Output

The script generates `parquet-visualizer.html` in the project root:
- **Size**: ~265 KB
- **Dependencies**: None (completely self-contained)
- **Browser support**: All modern browsers

## What's Included

The generated HTML file contains:
- ✅ Complete React application
- ✅ All CSS styles (inlined in `<style>` tag)
- ✅ All JavaScript code (inlined in `<script type="module">` tag)
- ✅ All Parquet parsing logic
- ✅ All visualization components

## Features

The standalone HTML file includes all features:
- File upload via drag & drop
- Structure view with clickable columns
- File info and statistics
- Schema visualization
- Page-level analysis with size breakdown
- Null count extraction
- Metadata viewer
- Support for DATA_PAGE and DATA_PAGE_V2
- ZSTD and SNAPPY decompression

## How to Use the Generated File

1. **Open locally**: Double-click `parquet-visualizer.html` to open in your browser
2. **Drag & drop**: Drag a Parquet file onto the page
3. **Explore**: Navigate through the different tabs to visualize your data

## Sharing

The generated HTML file is completely portable:
- ✅ Email it
- ✅ Upload to a web server
- ✅ Share via file sharing services
- ✅ Use offline
- ✅ No installation required

## Technical Details

### Build Process

The script performs these steps:

1. **Build webapp**
   ```bash
   cd webapp && npm run build
   ```

2. **Find generated assets**
   - Locates `index-*.css` in `webapp/dist/assets/`
   - Locates `index-*.js` in `webapp/dist/assets/`

3. **Combine files**
   - Reads CSS content
   - Reads JavaScript content
   - Inlines both into HTML template

4. **Write output**
   - Creates `parquet-visualizer.html`
   - Reports file size

### File Structure

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Parquet Visualizer</title>
    <style>
      /* All CSS inlined here (~20KB) */
    </style>
  </head>
  <body>
    <div id="root"></div>
    <script type="module">
      /* All JavaScript inlined here (~250KB) */
    </script>
  </body>
</html>
```

## Troubleshooting

### Build fails

If the build fails, check:
- You're in the project root directory
- `webapp/` directory exists
- Dependencies are installed: `cd webapp && npm install`

### Missing CSS or JS files

If the script can't find generated files:
- Check `webapp/dist/assets/` directory exists
- Ensure Vite build completed successfully
- Try cleaning and rebuilding: `rm -rf webapp/dist && npm run generate-html`

### File too large

The generated file is ~265 KB, which is reasonable for a single-page app. If you need a smaller file:
- Most of the size is the minified JavaScript
- Consider using the regular webapp deployment instead
- The file is already production-optimized by Vite

## Automation

You can automate HTML generation in your workflow:

### GitHub Actions
```yaml
- name: Generate standalone HTML
  run: npm run generate-html

- name: Upload artifact
  uses: actions/upload-artifact@v3
  with:
    name: parquet-visualizer
    path: parquet-visualizer.html
```

### Pre-commit Hook
```bash
#!/bin/sh
npm run generate-html
git add parquet-visualizer.html
```

## Development

When modifying the webapp, regenerate the HTML file:

1. Make changes to webapp source code
2. Run `npm run generate-html`
3. Test the generated HTML file
4. Commit both changes and new HTML file

## Related Files

- `generate-html.ts` - The generation script
- `webapp/` - Source code for the visualizer
- `parquet-visualizer.html` - Generated output (git tracked)
- `package.json` - Contains the `generate-html` script

## Version

The HTML file is regenerated with each build and contains the latest features from the webapp. Always regenerate after making changes to ensure the standalone file is up to date.
