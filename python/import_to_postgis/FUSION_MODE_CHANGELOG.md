# Fusion Mode Feature - Changelog

## New Features Added

### 1. Fusion Mode Parameter
- Added `try_to_fusion` parameter to `GeoDataImporter` class
- When `True`, merges files with matching schemas into single tables
- When `False` (default), each file creates its own table

### 2. Source Filename Tracking
- **All tables now include a `source_filename` column**
- Tracks which file each row originated from
- Essential for identifying data sources in fused tables
- Works in both fusion and normal modes

### 3. Intelligent Schema Grouping
- **CSV Files**: Groups by matching column names
- **Shapefiles**: Groups by matching field names AND geometry type
- **GeoJSON Files**: Groups by matching property names AND geometry type

### 4. Enhanced Import Methods
All import methods now support:
- `table_name` parameter: Specify custom table name
- `mode` parameter: Control import behavior ('replace', 'append', 'fail')
- Automatic addition of `source_filename` column

## Usage Examples

### Basic Configuration

```python
config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'geodatabase',
    'user': 'postgres',
    'password': 'password',
    'schema': 'imported_data',
    'source_folder': '/data',
    'try_to_fusion': True  # NEW: Enable fusion mode
}
```

### Example Scenario

**Input Files:**
```
/data/
├── region1_points.geojson  (name, population)
├── region2_points.geojson  (name, population)  # Same schema as region1
├── region3_areas.geojson   (name, area)        # Different schema
```

**Result with try_to_fusion = True:**
- `imported_data.geojson_group_1` (region1 + region2 combined)
  - Contains `source_filename` column showing which file each row came from
- `imported_data.geojson_group_2` (region3 only)
  - Also contains `source_filename` column

**Result with try_to_fusion = False:**
- `imported_data.region1_points` (with `source_filename`)
- `imported_data.region2_points` (with `source_filename`)
- `imported_data.region3_areas` (with `source_filename`)

## SQL Queries for Fused Data

```sql
-- Count records per source file
SELECT source_filename, COUNT(*) as records
FROM imported_data.geojson_group_1
GROUP BY source_filename;

-- Filter by specific source
SELECT * 
FROM imported_data.geojson_group_1
WHERE source_filename = 'region1_points.geojson';

-- Analyze data distribution
SELECT 
    source_filename,
    MIN(population) as min_pop,
    MAX(population) as max_pop,
    AVG(population) as avg_pop
FROM imported_data.geojson_group_1
GROUP BY source_filename;
```

## Benefits of Fusion Mode

1. **Reduced Table Count**: Consolidates similar data into fewer tables
2. **Easier Querying**: Query all related data from a single table
3. **Better Organization**: Logical grouping of similar datasets
4. **Source Tracking**: Never lose track of data origins with `source_filename`
5. **Flexible**: Can still separate data using WHERE clauses

## When to Use Fusion Mode

### Use Fusion Mode When:
- You have multiple files with identical structures (e.g., monthly exports)
- You want to query all data together without JOINs
- You're consolidating regional datasets
- You need simplified data management

### Use Normal Mode When:
- Each file represents distinct data
- You need complete isolation between datasets
- Table names are important for your workflow
- Files have varying schemas

## Implementation Details

### New Helper Methods
- `get_csv_schema()`: Extracts column names from CSV
- `get_shapefile_schema()`: Extracts fields and geometry type
- `get_geojson_schema()`: Extracts properties and geometry type
- `group_files_by_schema()`: Groups files with matching schemas
- `generate_fusion_table_name()`: Creates table names for groups

### Modified Methods
- `import_csv()`: Now accepts `table_name` and `mode` parameters
- `import_shapefile()`: Now accepts `table_name` and `mode` parameters
- `import_geojson()`: Now accepts `table_name` and `mode` parameters
- `import_all()`: Completely rewritten to support both modes

## Statistics Output

The import summary now shows:
- Files imported (success/failed)
- **Tables created** (important in fusion mode)

Example:
```
CSV Files:
  ✓ Success: 5 files
  ✗ Failed:  0 files
  → Tables:  2 tables

Total:
  ✓ Files Imported: 9
  → Total Tables:   4  (instead of 9 without fusion)
```

## Backward Compatibility

- Default behavior unchanged (`try_to_fusion=False`)
- All existing code continues to work
- `source_filename` column added to all imports (minimal impact)
- Table structure unchanged except for new column

## Notes

- Fusion is type-specific: CSV files only merge with CSV files, shapefiles with shapefiles, etc.
- Geometry type matters for spatial data: Point features won't merge with Polygon features
- Column order doesn't matter for matching (sorted internally)
- First file in group creates table (replace), subsequent files append
