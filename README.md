# Zeppelin to Databricks Notebook Converter

A Python tool to convert Apache Zeppelin notebooks (`.zpln` and `.json` files) to Databricks notebook format, enabling seamless migration of your analytics workflows.

## Features

- **Batch Conversion**: Convert individual files or entire directories
- **Language Support**: Supports Python, Scala, SQL, R, Shell, and Markdown
- **Interpreter Mapping**: Automatically maps Zeppelin interpreters to Databricks equivalents
- **Smart Filtering**: Skip empty notebooks and optionally filter by age
- **FUSE-Compatible**: Optimized for network filesystems and cloud storage
- **Progress Tracking**: Detailed conversion reports with success/failure statistics
- **Filename Sanitization**: Handles special characters and ensures cross-platform compatibility

## Supported Languages & Interpreters

| Zeppelin Interpreter | Databricks Equivalent | File Extension |
|---------------------|----------------------|----------------|
| `%pyspark`, `%python` | `%python` | `.py` |
| `%spark`, `%scala` | `%scala` | `.scala` |
| `%sql`, `%spark.sql` | `%sql` | `.sql` |
| `%r`, `%spark.r` | `%r` | `.r` |
| `%md` | `%md` | `.md` |
| `%sh`, `%file` | `%sh` | `.sh` |

## Installation

### Requirements
- Python 3.6+
- No external dependencies (uses only standard library)

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd zeppelin-to-databricks

# Make the script executable
chmod +x zeppelin_converter.py
```

## Usage

### Basic Examples

Convert a single notebook:
```bash
python zeppelin_converter.py --file notebook.zpln
```

Convert all notebooks in a directory:
```bash
python zeppelin_converter.py --directory /path/to/zeppelin/notebooks
```

Convert with custom output directory:
```bash
python zeppelin_converter.py --directory ./notebooks --out_dir ./converted
```

### Advanced Options

```bash
# Convert with specific default language
python zeppelin_converter.py --directory ./notebooks --language python

# Skip notebooks older than 30 days
python zeppelin_converter.py --directory ./notebooks --skip_old_days 30

# Ignore specific directories and show interpreter statistics
python zeppelin_converter.py --directory ./notebooks \
  --ignore_dirs temp backup \
  --show-interpreters

# Full example with all options
python zeppelin_converter.py \
  --directory ./zeppelin-notebooks \
  --out_dir ./databricks-notebooks \
  --language scala \
  --skip_old_days 90 \
  --ignore_dirs .git temp backup \
  --show-interpreters
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--file` | Single notebook file to convert | - |
| `--directory` | Directory containing notebooks | - |
| `--language` | Default language (`spark`, `python`, `pyspark`, `sql`, `r`, `scala`) | `spark` |
| `--out_dir` | Output directory | Same as input |
| `--skip_old_days` | Skip notebooks older than N days | - |
| `--ignore_dirs` | Directory names to ignore | `[]` |
| `--show-interpreters` | Display interpreter usage statistics | `false` |

## Output Format

The converter generates Databricks-compatible notebooks with:

- **Magic Commands**: Proper `%python`, `%scala`, `%sql`, etc. cell headers
- **Cell Separators**: `COMMAND ----------` between cells
- **Titles**: Cell titles converted to `DBTITLE` format
- **Comments**: Conversion notes for unsupported interpreters
- **Metadata**: Original Zeppelin structure preserved in comments

### Example Output
```python
# Databricks notebook source
# DBTITLE 1, Data Loading
# MAGIC %python
# MAGIC import pandas as pd
# MAGIC df = pd.read_csv("data.csv")
# COMMAND ----------

# DBTITLE 1, Analysis
# MAGIC %sql
# MAGIC SELECT * FROM table LIMIT 10
# COMMAND ----------
```

## Sample Notebooks

The repository includes sample notebooks demonstrating various Zeppelin features:

- **Flink Tutorial**: Stream processing examples
- **Spark Tutorial**: Scala, Python, SQL, and R examples
- **Python Tutorial**: IPython, visualization, and machine learning
- **R Tutorial**: Basic R, Shiny apps, and conda environments

## Migration Notes

### Manual Adjustments Needed

1. **Inline Interpreters**: Databricks doesn't support inline interpreter switching within cells
2. **Custom Libraries**: Verify library compatibility between platforms
3. **File Paths**: Update file paths to match Databricks filesystem structure
4. **Credentials**: Replace hardcoded credentials with Databricks secrets

### Known Limitations

- Some Zeppelin-specific features may not translate directly
- Custom Zeppelin interpreters require manual conversion
- Paragraph execution order is preserved but may need review

## Troubleshooting

### Common Issues

**File not found errors:**
- Ensure file paths are correct and files exist
- Check file permissions

**Empty output:**
- Verify input files contain valid Zeppelin notebook structure
- Check if notebooks have actual content (not just metadata)

**Encoding issues:**
- The tool handles UTF-8 with BOM automatically
- For other encodings, convert files first

### Getting Help

Check the conversion summary for detailed error messages:
```bash
python zeppelin_converter.py --directory ./notebooks --show-interpreters
```

Example output:
```
Found 15 notebook file(s) to process
Default language: spark -> %scala

  ✓ notebook1.zpln: Successfully converted to ./converted/notebook1.scala
  ⊘ old_notebook.zpln: Skipped (older than 30 days)
  ✗ broken_notebook.zpln: Missing 'paragraphs' field

SUMMARY: 12/15 successful, 1 failed, 2 skipped

INTERPRETER USAGE:
  %python: 45 cells (60.0%)
  %sql: 20 cells (26.7%)
  %scala: 10 cells (13.3%)
  Total cells: 75
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Databricks Integration

### Copying Files from Volumes to Workspace

After converting your Zeppelin notebooks, you'll often need to copy them from Databricks Volumes to your Workspace directory for execution.

#### Method 1: Using Databricks CLI

```bash
# Install Databricks CLI (if not already installed)
pip install databricks-cli

# Configure authentication
databricks configure --token

# Copy from Volume to Workspace
databricks fs cp -r /Volumes/catalog/schema/volume/converted-notebooks /Workspace/Users/your-email@company.com/notebooks --overwrite
```

#### Method 2: Using Databricks File System Commands (in notebook)

```python
# In a Databricks notebook cell
%fs cp -r /Volumes/catalog/schema/volume/converted-notebooks /Workspace/Users/your-email@company.com/notebooks
```

#### Method 3: Using Python/SQL in Databricks

```python
# Python approach using dbutils
dbutils.fs.cp("/Volumes/catalog/schema/volume/converted-notebooks", 
              "/Workspace/Users/your-email@company.com/notebooks", 
              recurse=True)

# List files to verify
dbutils.fs.ls("/Workspace/Users/your-email@company.com/notebooks")
```

#### Method 4: Workspace Import via UI

1. Open Databricks Workspace
2. Navigate to your user folder
3. Right-click → Import
4. Select "Import from Volume"
5. Browse to your converted notebooks in the Volume
6. Select files/folders to import

### Best Practices for File Organization

```
/Workspace/Users/your-email@company.com/
├── notebooks/
│   ├── converted-from-zeppelin/
│   │   ├── flink-tutorials/
│   │   ├── spark-tutorials/
│   │   └── python-tutorials/
│   └── original-databricks/
└── shared/
```

### Volume Path Examples

```bash
# Unity Catalog Volume structure
/Volumes/{catalog}/{schema}/{volume_name}/path/to/files

# Examples:
/Volumes/main/default/zeppelin_converted/flink-tutorials/
/Volumes/analytics/notebooks/migrated_notebooks/
/Volumes/dev/workspace/converted_files/
```

## Directory Copy Utility

The repository includes `copy_dir.py`, a utility script that helps copy directories while handling filename conflicts (when a file has the same name as its parent folder).

### Usage

```bash
# Basic usage
python copy_dir.py <source_folder> <destination_folder>

# Copy with error handling (continue on errors)
python copy_dir.py ./converted_notebooks ./backup_notebooks

# Stop on first error
python copy_dir.py ./source ./dest --no-skip
```

### Features

- **Conflict Resolution**: Automatically renames files that match their parent folder name
- **Structure Preservation**: Maintains directory hierarchy during copy
- **Error Handling**: Optional skip-on-error mode for robust batch operations
- **Progress Tracking**: Shows each file copied with full paths

### Example Output

```bash
python copy_dir.py ./converted_notebooks ./workspace_backup

Copied: ./converted_notebooks/Python Tutorial/basics.py -> ./workspace_backup/Python Tutorial/basics.py
Copied: ./converted_notebooks/Python Tutorial/Python Tutorial.scala -> ./workspace_backup/Python Tutorial/Python Tutorial_file.scala
Copied: ./converted_notebooks/Flink Tutorial/streaming.sql -> ./workspace_backup/Flink Tutorial/streaming.sql
```

### Integration with Converter

Use both tools together for a complete migration workflow:

```bash
# Step 1: Convert Zeppelin notebooks
python zeppelin_converter.py --directory ./zeppelin_notebooks --out_dir ./converted_notebooks

# Step 2: Copy to another location with conflict resolution
python copy_dir.py ./converted_notebooks ./final_destination
```

## Security Notice

⚠️ **Important**: Before using in production, review converted notebooks for hardcoded credentials or sensitive information. The sample notebooks in this repository contain example credentials that should be replaced with proper secret management.


