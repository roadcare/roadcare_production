# CSV and Shapefile to PostgreSQL/PostGIS Importer

A Python script that recursively searches through folders to find CSV and ESRI shapefiles, then imports them into a PostgreSQL/PostGIS database.

## Features

- **Recursive folder scanning**: Searches through all subfolders automatically
- **CSV support**: Imports CSV files into regular PostgreSQL tables
- **Shapefile support**: Imports ESRI shapefiles with geometry into PostGIS tables
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
    'source_folder': '/path/to/your/data/folder'  # Source folder to scan
}
```

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
4. **Import Process**:
   - **CSV files**: Imported as regular tables
   - **Shapefiles**: Imported as PostGIS tables with geometry columns
5. **Table Naming**: Filenames are sanitized to create valid table names:
   - Special characters replaced with underscores
   - Converted to lowercase
   - Prefixed with "table_" if starting with a number

## Table Behavior

By default, tables are **replaced** if they already exist (`if_exists='replace'`). 

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
├── subfolder/
│   ├── buildings.shp
│   ├── buildings.shx
│   ├── buildings.dbf
│   └── population.csv
└── another_folder/
    └── boundaries.shp
```

This will create tables:
- `imported_data.cities` (from cities.csv)
- `imported_data.roads` (from roads.shp)
- `imported_data.buildings` (from subfolder/buildings.shp)
- `imported_data.population` (from subfolder/population.csv)
- `imported_data.boundaries` (from another_folder/boundaries.shp)

## Output Example

```
==============================================================
CSV and Shapefile to PostgreSQL/PostGIS Importer
==============================================================

Configuration:
  Host: localhost:5432
  Database: geodatabase
  Schema: imported_data
  Source Folder: /data

2024-01-15 10:30:00 - INFO - Successfully connected to database
2024-01-15 10:30:00 - INFO - Using schema: imported_data
2024-01-15 10:30:00 - INFO - Found 2 CSV files and 3 shapefiles

==============================================================
Importing CSV files...
==============================================================
2024-01-15 10:30:01 - INFO - Importing CSV: cities.csv -> imported_data.cities
2024-01-15 10:30:01 - INFO - ✓ Successfully imported 150 rows to imported_data.cities

==============================================================
Importing Shapefiles...
==============================================================
2024-01-15 10:30:02 - INFO - Importing Shapefile: roads.shp -> imported_data.roads
2024-01-15 10:30:02 - INFO -   CRS: EPSG:4326, Geometry Type: ['LineString']
2024-01-15 10:30:03 - INFO - ✓ Successfully imported 1250 features to imported_data.roads

==============================================================
IMPORT SUMMARY
==============================================================
CSV Files:
  ✓ Success: 2
  ✗ Failed:  0

Shapefiles:
  ✓ Success: 3
  ✗ Failed:  0

Total:
  ✓ Success: 5
  ✗ Failed:  0
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

# Create importer
importer = GeoDataImporter(
    host='localhost',
    port=5432,
    database='mydb',
    user='myuser',
    password='mypass',
    schema='myschema'
)

# Connect
if importer.connect():
    # Import all files
    stats = importer.import_all('/path/to/data')
    print(f"Imported {stats['csv_success'] + stats['shp_success']} files")
    
    # Close connection
    importer.close()
```

### Importing Individual Files

```python
from pathlib import Path

# Import single CSV
importer.import_csv(Path('/data/myfile.csv'))

# Import single shapefile
importer.import_shapefile(Path('/data/myshape.shp'))
```

## License

This script is provided as-is for educational and commercial use.

## Support

For issues related to:
- **pandas**: https://pandas.pydata.org/docs/
- **geopandas**: https://geopandas.org/
- **PostgreSQL**: https://www.postgresql.org/docs/
- **PostGIS**: https://postgis.net/documentation/
