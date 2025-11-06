# PostgreSQL to ArcGIS Online Sync - Enhanced Version

## Overview
This script synchronizes data from a PostgreSQL/PostGIS table to an ArcGIS Online Feature Service layer by matching records using an ID field.

## Key Features

### 1. Flexible ID Field Matching
- **Parameter**: `id_field` (default: `'id'`)
- Specify any field name to use for matching records between PostgreSQL and ArcGIS Online
- Both systems must have this field with matching values

### 2. Flexible Field Updates
- **Parameter**: `fields_to_update` (default: `'*'`)
- Three modes of operation:

#### Mode 1: Update ALL fields (default)
```python
fields_to_update='*'
```
- Updates every non-geometry field from the PostgreSQL table
- Automatically excludes geometry fields and the ID field
- Perfect for full synchronization

#### Mode 2: Update SINGLE field
```python
fields_to_update='note_classe'
```
- Updates only the specified field
- Useful for targeted updates

#### Mode 3: Update MULTIPLE specific fields
```python
fields_to_update=['note_classe', 'largeur', 'other_field']
```
- Updates only the listed fields
- Provides fine-grained control

## Usage Examples

### Example 1: Update All Fields
```python
sync_postgres_to_agol(
    pg_config=PG_CONFIG,
    agol_config=AGOL_CONFIG,
    table_name='zh_u02_l200',
    schema='rendu',
    id_field='id',              # Matching field
    fields_to_update='*',        # Update ALL fields
    batch_size=1000
)
```

### Example 2: Update Single Field
```python
sync_postgres_to_agol(
    pg_config=PG_CONFIG,
    agol_config=AGOL_CONFIG,
    table_name='zh_u02_l200',
    schema='rendu',
    id_field='id',              
    fields_to_update='note_classe',  # Update ONLY note_classe
    batch_size=1000
)
```

### Example 3: Update Multiple Specific Fields
```python
sync_postgres_to_agol(
    pg_config=PG_CONFIG,
    agol_config=AGOL_CONFIG,
    table_name='zh_u02_l200',
    schema='rendu',
    id_field='id',              
    fields_to_update=['note_classe', 'largeur', 'width'],  # Update these 3 fields
    batch_size=1000
)
```

### Example 4: Use Different ID Field
```python
sync_postgres_to_agol(
    pg_config=PG_CONFIG,
    agol_config=AGOL_CONFIG,
    table_name='another_table',
    schema='rendu',
    id_field='custom_id',       # Use custom_id for matching
    fields_to_update='*',
    batch_size=1000
)
```

## Configuration

### PostgreSQL Configuration
```python
PG_CONFIG = {
    'host': 'localhost',
    'database': 'cd12_demo',
    'user': 'diagway',
    'password': 'diagway',
    'port': 5433
}
```

### ArcGIS Online Configuration
```python
AGOL_CONFIG = {
    'feature_service_url': 'https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer/2',
    'username': "roadcare",
    'password': "Antonin&TienSy2021",
    'portal_url': 'https://www.arcgis.com'
}
```

## Function Parameters

### `sync_postgres_to_agol()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pg_config` | dict | required | PostgreSQL connection parameters |
| `agol_config` | dict | required | ArcGIS Online connection parameters |
| `table_name` | str | required | PostgreSQL table name |
| `schema` | str | `'rendu'` | PostgreSQL schema name |
| `id_field` | str | `'id'` | Field name for matching records |
| `fields_to_update` | str or list | `'*'` | Fields to update: `'*'`, `'field_name'`, or `['field1', 'field2']` |
| `batch_size` | int | `1000` | Number of features per update batch |

## How It Works

1. **Read from PostgreSQL**:
   - Connects to PostgreSQL database
   - Queries the specified table and fields
   - Builds a dictionary: `{id: {field1: value1, field2: value2, ...}}`

2. **Update ArcGIS Online**:
   - Connects to ArcGIS Online
   - Queries the feature layer
   - Matches features by ID field
   - Updates matched features with values from PostgreSQL
   - Processes updates in batches to avoid rate limits

3. **Reports Results**:
   - Number of matched features
   - Number of successful updates
   - Number of failed updates
   - Detailed error information if needed

## Important Notes

- **ID Field**: Must exist in both PostgreSQL table and ArcGIS Online layer with matching values
- **Geometry Fields**: Automatically excluded when using `fields_to_update='*'`
- **NULL Values**: Fields with NULL values in PostgreSQL are skipped
- **Batch Processing**: Updates are processed in batches (default 1000) to avoid rate limits
- **Data Types**: The script handles automatic type conversion where possible

## Error Handling

The script includes comprehensive error handling:
- Connection errors (PostgreSQL and ArcGIS Online)
- Query errors
- Update errors (per batch and per feature)
- Detailed error messages and stack traces

## Requirements

```
psycopg2
arcgis
urllib3
```

## SSL Certificate Warnings

The script automatically disables SSL certificate verification warnings (InsecureRequestWarning) that may appear when connecting to ArcGIS Online services. This is done at the start of the script:

```python
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
```

If you need to re-enable these warnings for security auditing, you can comment out or remove this line.

## Backward Compatibility

The enhanced version maintains backward compatibility with the original single-field update approach. Existing code will continue to work without modifications.
