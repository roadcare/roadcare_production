# Fixing Polygon Geometry Errors - Quick Guide

## The Problem

When uploading polygon geometries to ArcGIS Online, you may encounter this error:
```
The specified geometry is not in the correct format. 
The given key was not present in the dictionary. (Error Code: 400)
```

This happens because:
1. **Format mismatch**: ESRI uses a "rings" format for polygons, not standard GeoJSON
2. **Invalid geometries**: PostGIS may have invalid geometries that need to be fixed
3. **Null/Empty geometries**: Some records may have NULL or empty geometries

## Solution - Updated Script

The main script has been updated with proper polygon conversion. It now includes:

### `convert_geojson_to_esri_geometry()` function

This converts PostGIS geometries to ESRI format:

- **Point** → `{"x": x, "y": y}`
- **LineString** → `{"paths": [[[x1, y1], [x2, y2], ...]]}`
- **Polygon** → `{"rings": [[[x1, y1], [x2, y2], ..., [x1, y1]]]}`
- **MultiPolygon** → `{"rings": [ring1, ring2, ...]}`

### Error handling

The script now catches and reports geometry conversion errors without stopping the entire upload.

## Step-by-Step Fix

### Step 1: Check Your Geometries

Run the geometry troubleshooting script:

```bash
python geometry_troubleshooting.py
```

Select option **1** to check geometry validity:
- Enter your schema name (e.g., `public`, `client`)
- Enter your table name

This will show:
- ✓ Valid geometries count
- ⚠ NULL geometries count  
- ✗ Invalid geometries count
- Details about invalid geometries

### Step 2: Fix Invalid Geometries (if any)

If you have invalid geometries, you have two options:

#### Option A: Fix in Database (Recommended)

1. Run geometry troubleshooting script
2. Select option **4** (DRY RUN) to preview changes
3. Select option **5** (ACTUAL FIX) to fix them
4. Type `yes` to confirm

This uses PostGIS `ST_MakeValid()` to repair invalid geometries.

#### Option B: Skip Invalid Geometries

Add a WHERE clause when uploading:

```python
pg_to_arcgis.add_layer_to_feature_service(
    feature_service_url=FEATURE_SERVICE_URL,
    layer_name="My_Polygons",
    schema_name="public",
    table_name="my_polygon_table",
    limit=None,
    where_clause="ST_IsValid(geom) = true"  # Only valid geometries
)
```

### Step 3: Test Geometry Conversion

Before uploading, test the conversion:

```bash
python geometry_troubleshooting.py
```

Select option **3** to test geometry conversion:
- Shows 5 sample geometries
- Displays their ESRI format conversion
- Verifies the conversion works correctly

### Step 4: Upload Your Data

Now use the main script:

```bash
python add_layer_to_arcgis.py
```

Or use the examples:

```python
from add_layer_to_arcgis import PostgresToArcGIS

PG_CONFIG = {
    'host': 'localhost',
    'database': 'cd12_demo',
    'user': 'diagway',
    'password': 'diagway',
    'port': 5433
}

pg_to_arcgis = PostgresToArcGIS(
    PG_CONFIG, 
    "roadcare", 
    "Antonin&TienSy2021"
)

pg_to_arcgis.connect_postgres()
pg_to_arcgis.connect_arcgis()

# Upload polygons
pg_to_arcgis.add_layer_to_feature_service(
    feature_service_url="https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer",
    layer_name="My_Polygon_Layer",
    schema_name="public",
    table_name="my_polygon_table",
    limit=None,  # or specify a number
    where_clause=None  # or add filtering
)

pg_to_arcgis.close_connections()
```

## Common Polygon Issues

### Issue 1: Self-Intersecting Polygons

**Symptom**: Polygon edges cross each other

**Fix**: Use `ST_MakeValid()` (option 5 in troubleshooting menu)

### Issue 2: Duplicate Points

**Symptom**: Same point appears multiple times in sequence

**Fix**: PostGIS usually handles this, but you can clean with:
```sql
UPDATE your_table 
SET geom = ST_RemoveRepeatedPoints(geom, 0.001);
```

### Issue 3: Ring Not Closed

**Symptom**: First and last points of polygon ring are different

**Fix**: Use `ST_MakeValid()` or manually close:
```sql
UPDATE your_table 
SET geom = ST_MakePolygon(ST_AddPoint(ST_ExteriorRing(geom), ST_StartPoint(ST_ExteriorRing(geom))))
WHERE ST_StartPoint(ST_ExteriorRing(geom)) != ST_EndPoint(ST_ExteriorRing(geom));
```

### Issue 4: Wrong Ring Orientation

**Symptom**: Exterior ring is clockwise instead of counter-clockwise

**Fix**: Use `ST_ForceRHR()` (Right-Hand Rule):
```sql
UPDATE your_table 
SET geom = ST_ForceRHR(geom);
```

## Quick SQL Checks

### Check for NULL geometries:
```sql
SELECT COUNT(*) 
FROM your_schema.your_table 
WHERE geom IS NULL;
```

### Check for invalid geometries:
```sql
SELECT id, ST_IsValidReason(geom) as reason
FROM your_schema.your_table 
WHERE NOT ST_IsValid(geom);
```

### Check for empty geometries:
```sql
SELECT COUNT(*) 
FROM your_schema.your_table 
WHERE ST_IsEmpty(geom);
```

### Fix all issues at once:
```sql
-- Backup your data first!
CREATE TABLE your_table_backup AS SELECT * FROM your_schema.your_table;

-- Fix geometries
UPDATE your_schema.your_table 
SET geom = ST_MakeValid(geom)
WHERE geom IS NOT NULL AND NOT ST_IsValid(geom);
```

## Troubleshooting Checklist

- [ ] Run geometry validity check
- [ ] Fix any invalid geometries
- [ ] Test geometry conversion with samples
- [ ] Upload with updated script
- [ ] Check ArcGIS Online to verify upload

## Still Having Issues?

If you still encounter errors:

1. **Check the specific error message** - Look for details about which features failed
2. **Reduce batch size** - Try uploading fewer records at a time
3. **Check SRID** - Ensure your SRID (2154) matches what ArcGIS expects
4. **Test with subset** - Use `limit=10` to test with a small dataset first
5. **Check geometry column name** - Ensure it's spelled correctly (case-sensitive)

## Example: Complete Workflow

```python
# 1. Check and fix geometries first
# Run: python geometry_troubleshooting.py
# Select: 1 (Check validity)
# Select: 5 (Fix if needed)

# 2. Then upload
from add_layer_to_arcgis import PostgresToArcGIS

PG_CONFIG = {
    'host': 'localhost',
    'database': 'cd12_demo',
    'user': 'diagway',
    'password': 'diagway',
    'port': 5433
}

pg_to_arcgis = PostgresToArcGIS(
    PG_CONFIG,
    "roadcare",
    "Antonin&TienSy2021"
)

try:
    pg_to_arcgis.connect_postgres()
    pg_to_arcgis.connect_arcgis()
    
    success = pg_to_arcgis.add_layer_to_feature_service(
        feature_service_url="https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer",
        layer_name="Polygon_Layer",
        schema_name="public",
        table_name="polygon_table",
        limit=None,
        where_clause=None
    )
    
    if success:
        print("Upload successful!")
    else:
        print("Upload failed - check errors above")
        
finally:
    pg_to_arcgis.close_connections()
```

## Notes

- The script now handles MultiPolygons by flattening them into a single rings array
- All geometry conversions include error handling to prevent crashes
- Invalid geometries are reported but don't stop the entire upload process
- Always backup your data before running fixes on the database

## Support Files

- **add_layer_to_arcgis.py** - Main upload script with fixed polygon conversion
- **geometry_troubleshooting.py** - Diagnostic and repair tool
- **utilities.py** - Connection testing and database exploration
- **examples.py** - Ready-to-use examples for different tables
