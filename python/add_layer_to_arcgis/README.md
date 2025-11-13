# PostgreSQL to ArcGIS Online Layer Transfer

This script transfers data from a PostgreSQL/PostGIS table to an ArcGIS Online Feature Service as a new layer.

## Prerequisites

1. Python 3.7 or higher
2. PostgreSQL database with PostGIS extension
3. ArcGIS Online account with permissions to edit Feature Services

## Installation

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Configuration

Edit the `main()` function in `add_layer_to_arcgis.py` to configure:

### 1. PostgreSQL Connection

```python
PG_CONFIG = {
    'host': 'localhost',
    'database': 'cd12_demo',
    'user': 'diagway',
    'password': 'diagway',
    'port': 5433
}
```

### 2. ArcGIS Online Credentials

```python
ARCGIS_USERNAME = "roadcare"
ARCGIS_PASSWORD = "Antonin&TienSy2021"
FEATURE_SERVICE_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer"
```

### 3. Layer Parameters

```python
SCHEMA_NAME = "client"           # PostgreSQL schema name
TABLE_NAME = "route_client"      # PostgreSQL table name
LAYER_NAME = "Route_Client_Layer"  # Name for the new layer in ArcGIS Online
LIMIT = 1000                     # Max records to transfer (None for all)
WHERE_CLAUSE = None              # Optional SQL WHERE clause for filtering
```

## Usage

### Basic Usage

Run the script:

```bash
python add_layer_to_arcgis.py
```

### Examples

#### Example 1: Transfer client.route_client table

```python
SCHEMA_NAME = "client"
TABLE_NAME = "route_client"
LAYER_NAME = "Routes_Client"
LIMIT = None  # Transfer all records
WHERE_CLAUSE = None
```

#### Example 2: Transfer client.troncon_client with filtering

```python
SCHEMA_NAME = "client"
TABLE_NAME = "troncon_client"
LAYER_NAME = "Troncons_Client"
LIMIT = 5000
WHERE_CLAUSE = "cumuld > 0 AND cumulf < 10000"  # Only segments within range
```

#### Example 3: Transfer public.image table

```python
SCHEMA_NAME = "public"
TABLE_NAME = "image"
LAYER_NAME = "Images_Acquisition"
LIMIT = 10000
WHERE_CLAUSE = "is_obsolete = false"  # Only non-obsolete images
```

#### Example 4: Transfer public.session table

```python
SCHEMA_NAME = "public"
TABLE_NAME = "session"
LAYER_NAME = "Acquisition_Sessions"
LIMIT = None
WHERE_CLAUSE = "state = 'analyzed'"  # Only analyzed sessions
```

## Features

### Supported Data Types

The script automatically converts PostgreSQL data types to ArcGIS field types:

- **Numeric**: integer, bigint, numeric, double precision → Integer/Double
- **Text**: text, varchar, char → String
- **Date/Time**: timestamp, date → Date
- **UUID**: uuid → GUID
- **Boolean**: boolean → String

### Supported Geometry Types

The script handles all PostGIS geometry types:

- **Point/PointM/PointZ** → esriGeometryPoint
- **LineString/LineStringM/LineStringZ** → esriGeometryPolyline
- **Polygon/PolygonM/PolygonZ** → esriGeometryPolygon
- **MultiPoint/MultiLineString/MultiPolygon** → Corresponding multi-geometries

### M and Z Values

The script supports M (measure) and Z (elevation) values in geometries:
- LineStringM, PointM → Preserves M values
- LineStringZ, PointZ → Preserves Z values

## How It Works

1. **Connect to PostgreSQL**: Establishes connection to the database
2. **Connect to ArcGIS Online**: Authenticates with ArcGIS Online
3. **Analyze Table Schema**: Reads column definitions and geometry type
4. **Create Layer Definition**: Builds ArcGIS layer schema from PostgreSQL schema
5. **Fetch Data**: Retrieves records from PostgreSQL with geometries as GeoJSON
6. **Convert Format**: Transforms data to ArcGIS feature format
7. **Add Layer**: Creates new layer in Feature Service
8. **Upload Features**: Adds features in batches (100 features per batch)

## Limitations

- **Batch Size**: Features are uploaded in batches of 100 to avoid timeouts
- **Field Names**: Field names are preserved as-is (case-sensitive)
- **SRID**: Default SRID is 2154 (Lambert 93) if not detected
- **ObjectID**: An OBJECTID field is automatically added (required by ArcGIS)

## Troubleshooting

### "Failed to add layer definition"

Check that:
- You have edit permissions on the Feature Service
- The Feature Service URL is correct
- The layer name doesn't already exist

### "No records found"

Check that:
- The schema and table names are correct
- The WHERE clause is valid
- The table contains data

### "Error connecting to PostgreSQL"

Verify:
- PostgreSQL is running
- Connection parameters are correct
- The database and schema exist
- The user has SELECT permissions

### "Error connecting to ArcGIS Online"

Verify:
- Username and password are correct
- You have an active ArcGIS Online subscription
- You have permissions to edit the Feature Service

## Advanced Usage

### Using the Class Programmatically

```python
from add_layer_to_arcgis import PostgresToArcGIS

# Create instance
pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, USERNAME, PASSWORD)

# Connect
pg_to_arcgis.connect_postgres()
pg_to_arcgis.connect_arcgis()

# Add layer
pg_to_arcgis.add_layer_to_feature_service(
    feature_service_url="https://...",
    layer_name="My Layer",
    schema_name="client",
    table_name="route_client",
    limit=1000,
    where_clause="longueur > 500"
)

# Clean up
pg_to_arcgis.close_connections()
```

### Fetching Data Only

```python
# Just fetch data without uploading
records = pg_to_arcgis.fetch_table_data(
    schema_name="client",
    table_name="route_client",
    limit=100
)

# Process records
for record in records:
    print(record)
```

## Project Context

This script is part of the `roadare_sig_prod` project for processing road network acquisition data. It supports the following schemas:

- **client**: Linear referencing data (route_client, troncon_client)
- **public**: Scanned road data (image, session)
- **rendu**: Final results

For more information about linear referencing, see:
https://pro.arcgis.com/en/pro-app/latest/help/data/linear-referencing/introduction-to-linear-referencing.htm

## License

Internal use for roadare_sig_prod project.
