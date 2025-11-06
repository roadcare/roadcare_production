# CSV, Shapefile, and GeoJSON to PostgreSQL/PostGIS Importer

A Python script that recursively searches through folders to find CSV, ESRI shapefiles, and GeoJSON files, then imports them into a PostgreSQL/PostGIS database.

## Features

- **Recursive folder scanning**: Searches through all subfolders automatically
- **CSV support**: Imports CSV files into regular PostgreSQL tables
- **Shapefile support**: Imports ESRI shapefiles with geometry into PostGIS tables
- **GeoJSON support**: Imports GeoJSON files with geometry into PostGIS tables
- **Fusion mode**: Optionally merge files with matching schemas into single tables
- **Source tracking**: Automatically adds `source_filename` column to track data origin
- **Schema management**: Creates tables in a specified schema
- **Automatic table naming**: Sanitizes filenames to create valid table names
- **Error handling**: Continues processing even if individual files fail
- **Detailed logging**: Provides progress updates and error messages
- **PostGIS integration**: Preserves coordinate reference systems (CRS)

## Prerequisites

- Python 3.7 or higher
- PostgreSQL with PostGIS extension
- Database with appropriate permissions

## Installation

1. Install required Python packages:

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install pandas geopandas sqlalchemy psycopg2-binary
```

If you encounter permission issues, use:

```bash
pip install pandas geopandas sqlalchemy psycopg2-binary --break-system-packages
```

## Configuration

Edit the configuration section in `import_to_postgis.py`:

```python
config = {
    'host': 'localhost',           # Database host
    'port': 5432,                  # Database port
    'database': 'your_database',   # Database name
    'user': 'your_username',       # Database user
    'password': 'your_password',   # Database password
    'schema': 'imported_data',     # Target schema
    'source_folder': '/path/to/your/data/folder',  # Source folder to scan
    'try_to_fusion': False         # Fusion mode (see below)
}
```

## Fusion Mode

The `try_to_fusion` parameter enables intelligent merging of files with matching schemas:

### When `try_to_fusion = False` (Default):
- Each file creates its own table
- `cities1.csv` → `imported_data.cities1`
- `cities2.csv` → `imported_data.cities2`

### When `try_to_fusion = True`:
- Files with identical schemas are merged into a single table
- `cities1.csv` + `cities2.csv` → `imported_data.csv_group_1`
- All tables include a `source_filename` column to track data origin

### How It Works:

1. **Schema Detection**: Analyzes each file to identify columns/fields
2. **Grouping**: Groups files by matching schemas
   - **CSV**: Matches by column names
   - **Shapefiles**: Matches by field names AND geometry type
   - **GeoJSON**: Matches by property names AND geometry type
3. **Merging**: Imports matching files into a single table
4. **Tracking**: Adds `source_filename` column to identify source file

### Example:

**Files:**
```
/data/
├── region1_points.geojson  (fields: name, population)
├── region2_points.geojson  (fields: name, population)
└── region3_points.geojson  (fields: name, population, area)  # Different schema
```

**Result with `try_to_fusion = True`:**
- `imported_data.geojson_group_1` (contains region1 + region2 data with `source_filename` column)
- `imported_data.geojson_group_2` (contains region3 data with `source_filename` column)

**Result with `try_to_fusion = False`:**
- `imported_data.region1_points`
- `imported_data.region2_points`
- `imported_data.region3_points`

### Security Best Practice

For better security, use environment variables:

```python
import os

config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'your_database'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD'),
    'schema': 'imported_data',
    'source_folder': '/path/to/your/data/folder'
}
```

Then set environment variables:

```bash
export DB_PASSWORD="your_password"
export DB_HOST="localhost"
export DB_NAME="your_database"
export DB_USER="your_username"
```

## Usage

Run the script:

```bash
python import_to_postgis.py
```

Or make it executable:

```bash
chmod +x import_to_postgis.py
./import_to_postgis.py
```

## Database Setup

Before running the script, ensure your PostgreSQL database is ready:

```sql
-- Create database (if needed)
CREATE DATABASE your_database;

-- Connect to the database
\c your_database

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create a user (if needed)
CREATE USER your_username WITH PASSWORD 'your_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE your_database TO your_username;
GRANT ALL ON SCHEMA public TO your_username;
```

## How It Works

1. **Connection**: Establishes connection to PostgreSQL/PostGIS
2. **Schema Creation**: Creates the target schema if it doesn't exist
3. **File Discovery**: Recursively scans the source folder for:
   - `.csv` files
   - `.shp` files (ESRI shapefiles)
   - `.geojson` and `.json` files (GeoJSON)
4. **Import Process**:
   - **CSV files**: Imported as regular tables
   - **Shapefiles**: Imported as PostGIS tables with geometry columns
   - **GeoJSON files**: Imported as PostGIS tables with geometry columns
5. **Table Naming**: Filenames are sanitized to create valid table names:
   - Special characters replaced with underscores
   - Converted to lowercase
   - Prefixed with "table_" if starting with a number

## Table Behavior

### Source Tracking

All imported tables automatically include a `source_filename` column that stores the original filename of each record. This is especially useful in fusion mode to identify which file each row came from.

**Example query:**
```sql
SELECT source_filename, COUNT(*) 
FROM imported_data.csv_group_1 
GROUP BY source_filename;
```

### Import Modes

By default, tables are **replaced** if they already exist (`if_exists='replace'`). 

In fusion mode, the first file uses 'replace' mode and subsequent files use 'append' mode.

To change this behavior, modify the import methods:

- `if_exists='fail'` - Raise error if table exists
- `if_exists='append'` - Add data to existing table
- `if_exists='replace'` - Drop and recreate table (default)

## Example Directory Structure

```
/data/
├── cities.csv
├── roads.shp
├── roads.shx
├── roads.dbf
├── parks.geojson
├── subfolder/
│   ├── buildings.shp
│   ├── buildings.shx
│   ├── buildings.dbf
│   ├── population.csv
│   └── districts.geojson
└── another_folder/
    ├── boundaries.shp
    └── points_of_interest.json
```

This will create tables:
- `imported_data.cities` (from cities.csv)
- `imported_data.roads` (from roads.shp)
- `imported_data.parks` (from parks.geojson)
- `imported_data.buildings` (from subfolder/buildings.shp)
- `imported_data.population` (from subfolder/population.csv)
- `imported_data.districts` (from subfolder/districts.geojson)
- `imported_data.boundaries` (from another_folder/boundaries.shp)
- `imported_data.points_of_interest` (from another_folder/points_of_interest.json)

## Output Example

### Normal Mode (try_to_fusion = False)

```
==============================================================
CSV, Shapefile, and GeoJSON to PostgreSQL/PostGIS Importer
==============================================================

Configuration:
  Host: localhost:5432
  Database: geodatabase
  Schema: imported_data
  Source Folder: /data
  Fusion Mode: False

2024-01-15 10:30:00 - INFO - Successfully connected to database
2024-01-15 10:30:00 - INFO - Using schema: imported_data
2024-01-15 10:30:00 - INFO - Found 2 CSV files, 3 shapefiles, and 2 GeoJSON files

==============================================================
Importing CSV files...
==============================================================
2024-01-15 10:30:01 - INFO - Importing CSV: cities.csv -> imported_data.cities (mode: replace)
2024-01-15 10:30:01 - INFO - ✓ Successfully imported 150 rows to imported_data.cities

==============================================================
IMPORT SUMMARY
==============================================================
CSV Files:
  ✓ Success: 2
  ✗ Failed:  0
  → Tables:  2

Shapefiles:
  ✓ Success: 3
  ✗ Failed:  0
  → Tables:  3

GeoJSON Files:
  ✓ Success: 2
  ✗ Failed:  0
  → Tables:  2

Total:
  ✓ Files Imported: 7
  ✗ Files Failed:   0
  → Total Tables:   7
==============================================================
```

### Fusion Mode (try_to_fusion = True)

```
==============================================================
CSV, Shapefile, and GeoJSON to PostgreSQL/PostGIS Importer
==============================================================

Configuration:
  Host: localhost:5432
  Database: geodatabase
  Schema: imported_data
  Source Folder: /data
  Fusion Mode: True

2024-01-15 10:30:00 - INFO - Successfully connected to database
2024-01-15 10:30:00 - INFO - Using schema: imported_data
2024-01-15 10:30:00 - INFO - Found 4 CSV files, 3 shapefiles, and 2 GeoJSON files

==============================================================
FUSION MODE ENABLED
Grouping files by matching schemas...
==============================================================

==============================================================
Processing CSV files...
==============================================================
2024-01-15 10:30:00 - INFO - Found 2 unique CSV schema(s)

Group 1: 3 file(s) with matching schema -> imported_data.csv_group_1
  Fields: city, population, latitude, longitude, country
2024-01-15 10:30:01 - INFO - Importing CSV: region1_cities.csv -> imported_data.csv_group_1 (mode: replace)
2024-01-15 10:30:01 - INFO - ✓ Successfully imported 50 rows to imported_data.csv_group_1
2024-01-15 10:30:02 - INFO - Importing CSV: region2_cities.csv -> imported_data.csv_group_1 (mode: append)
2024-01-15 10:30:02 - INFO - ✓ Successfully imported 45 rows to imported_data.csv_group_1
2024-01-15 10:30:03 - INFO - Importing CSV: region3_cities.csv -> imported_data.csv_group_1 (mode: append)
2024-01-15 10:30:03 - INFO - ✓ Successfully imported 55 rows to imported_data.csv_group_1

Group 2: 1 file(s) with matching schema -> imported_data.countries
  Fields: country, area, gdp
2024-01-15 10:30:04 - INFO - Importing CSV: countries.csv -> imported_data.countries (mode: replace)
2024-01-15 10:30:04 - INFO - ✓ Successfully imported 25 rows to imported_data.countries

==============================================================
IMPORT SUMMARY
==============================================================
CSV Files:
  ✓ Success: 4
  ✗ Failed:  0
  → Tables:  2

Shapefiles:
  ✓ Success: 3
  ✗ Failed:  0
  → Tables:  1

GeoJSON Files:
  ✓ Success: 2
  ✗ Failed:  0
  → Tables:  1

Total:
  ✓ Files Imported: 9
  ✗ Files Failed:   0
  → Total Tables:   4
==============================================================
```

## Troubleshooting

### Connection Issues

```
Database connection failed: could not connect to server
```

**Solution**: Check that PostgreSQL is running and connection parameters are correct.

### PostGIS Extension Missing

```
ERROR: type "geometry" does not exist
```

**Solution**: Enable PostGIS extension:
```sql
CREATE EXTENSION postgis;
```

### Permission Denied

```
ERROR: permission denied for schema imported_data
```

**Solution**: Grant necessary permissions:
```sql
GRANT ALL ON SCHEMA imported_data TO your_username;
```

### File Encoding Issues

If CSV files have encoding issues, modify the import_csv method:

```python
df = pd.read_csv(csv_path, encoding='utf-8')  # or 'latin-1', 'cp1252', etc.
```

## Advanced Usage

### Using as a Module

```python
from import_to_postgis import GeoDataImporter

# Create importer without fusion
importer = GeoDataImporter(
    host='localhost',
    port=5432,
    database='mydb',
    user='myuser',
    password='mypass',
    schema='myschema',
    try_to_fusion=False
)

# Connect
if importer.connect():
    # Import all files
    stats = importer.import_all('/path/to/data')
    print(f"Imported {stats['csv_success'] + stats['shp_success']} files into {stats['csv_tables'] + stats['shp_tables']} tables")
    
    # Close connection
    importer.close()
```

### Using Fusion Mode

```python
# Create importer with fusion enabled
importer = GeoDataImporter(
    host='localhost',
    port=5432,
    database='mydb',
    user='myuser',
    password='mypass',
    schema='myschema',
    try_to_fusion=True  # Enable fusion
)

if importer.connect():
    stats = importer.import_all('/path/to/data')
    print(f"Created {stats['csv_tables'] + stats['shp_tables'] + stats['geojson_tables']} tables from {stats['csv_success'] + stats['shp_success'] + stats['geojson_success']} files")
    importer.close()
```

### Importing Individual Files

```python
from pathlib import Path

# Import single CSV with custom table name
importer.import_csv(Path('/data/myfile.csv'), table_name='custom_name', mode='replace')

# Import and append to existing table
importer.import_csv(Path('/data/more_data.csv'), table_name='custom_name', mode='append')

# Import single shapefile
importer.import_shapefile(Path('/data/myshape.shp'))

# Import single GeoJSON
importer.import_geojson(Path('/data/mydata.geojson'))
```

### Querying Fused Data

After importing with fusion mode:

```sql
-- See which files contributed to the table
SELECT DISTINCT source_filename 
FROM imported_data.csv_group_1;

-- Count records per source file
SELECT source_filename, COUNT(*) as record_count
FROM imported_data.csv_group_1
GROUP BY source_filename
ORDER BY record_count DESC;

-- Filter by specific source file
SELECT * 
FROM imported_data.csv_group_1 
WHERE source_filename = 'region1_cities.csv';
```

## License

This script is provided as-is for educational and commercial use.

## Support

For issues related to:
- **pandas**: https://pandas.pydata.org/docs/
- **geopandas**: https://geopandas.org/
- **PostgreSQL**: https://www.postgresql.org/docs/
- **PostGIS**: https://postgis.net/documentation/
