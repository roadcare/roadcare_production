# ReplaceSingleLayerFromPostGIS.py
from arcgis.gis import GIS
from arcgis.features import Feature
import geopandas as gpd
from shapely.geometry import mapping
from sqlalchemy import create_engine
import math

# ---------- CONFIG ----------
PORTAL_URL = "https://www.arcgis.com"         # or your portal URL
USERNAME = "your_username"
PASSWORD = "your_password"

FEATURE_SERVICE_ITEM_ID = "abcdef1234567890"  # item id of the hosted feature service (the item, not individual layer item)
TARGET_LAYER_NAME = "my_layer_name"           # name of the sublayer to replace (or use index)
POSTGIS_CONN = "postgresql+psycopg2://user:pw@host:5432/dbname"
POSTGIS_TABLE = "schema.table_name"           # table to read
BATCH_SIZE = 500                              # features per upload batch
TARGET_SRID = 4326                            # expected spatial ref of hosted layer (use actual if known)
# ----------------------------

# Connect to portal
gis = GIS(PORTAL_URL, USERNAME, PASSWORD)
item = gis.content.get(FEATURE_SERVICE_ITEM_ID)
if item is None:
    raise RuntimeError(f"Feature service item {FEATURE_SERVICE_ITEM_ID} not found")

# Get the FeatureLayer object by name (or index)
# item.layers is a list of FeatureLayer objects
layers = item.layers
target_layer = None
for lyr in layers:
    # lyr.properties.name available
    if hasattr(lyr.properties, "name") and lyr.properties.name == TARGET_LAYER_NAME:
        target_layer = lyr
        break

if target_layer is None:
    # fallback: try matching by layer id or index if name didn't match
    # e.g. use index 0: target_layer = item.layers[0]
    raise RuntimeError(f"Layer named '{TARGET_LAYER_NAME}' not found in item {FEATURE_SERVICE_ITEM_ID}")

print(f"Found target layer: {target_layer.properties.name} (id: {target_layer.properties.id})")

# Read PostGIS table with geopandas
engine = create_engine(POSTGIS_CONN)
sql = f"SELECT * FROM {POSTGIS_TABLE};"
gdf = gpd.read_postgis(sql, con=engine, geom_col='geom')  # adjust geom column name if different

# Ensure geometry column exists
if gdf.geometry.isnull().all():
    raise RuntimeError("No geometries read from PostGIS table - check geom column name and SQL.")

# Reproject if needed to target SRID
if gdf.crs is None:
    # assume input is EPSG:4326 if unknown — change as appropriate
    gdf.set_crs(epsg=4326, inplace=True)

if gdf.crs.to_epsg() != TARGET_SRID:
    gdf = gdf.to_crs(epsg=TARGET_SRID)

# Optional: match schema: remove fields not in target layer or rename fields accordingly
# Get target fields list
target_fields = [f['name'] for f in target_layer.properties.fields]
# Remove OID/geometry/system fields from target_fields for attribute mapping
sys_fields = {'OBJECTID', 'OBJECTID_1', 'Shape', 'SHAPE', 'globalid', 'GlobalID'}
attrib_fields = [f for f in target_fields if f.upper() not in sys_fields]

# Keep only attributes that exist in both gdf and layer (case-insensitive)
gdf_columns_lower = {c.lower(): c for c in gdf.columns}
mapped_columns = {}
for tgt in attrib_fields:
    lc = tgt.lower()
    if lc in gdf_columns_lower:
        mapped_columns[tgt] = gdf_columns_lower[lc]

# Build list of features (dictionary with geometry and attributes)
geojson = gdf.to_json()  # geojson string
import json
gj = json.loads(geojson)

features = []
for feat in gj['features']:
    # attributes: keep only mapped columns
    attrs = {}
    for tgt, src in mapped_columns.items():
        # src is original gdf column name
        attrs[tgt] = feat['properties'].get(src, None)
    # geometry
    geom = feat.get('geometry', None)
    features.append(Feature({"attributes": attrs, "geometry": geom}))

print(f"Prepared {len(features)} features to upload.")

# --------------- Replace data ---------------
# 1) Delete existing features in target layer
print("Deleting existing features in target layer...")
# Many layers allow SQL where delete "1=1" to delete all; if not permitted use a different strategy
delete_result = target_layer.delete_features(where="1=1")
print("Delete RPC result:", delete_result)

# 2) Add features in batches
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

print("Uploading in batches...")
for i, batch in enumerate(chunk_list(features, BATCH_SIZE), start=1):
    # edit_features expects list of features or FeatureSet; use 'adds' param
    result = target_layer.edit_features(adds=batch)
    print(f"Batch {i}: uploaded {len(batch)} features. Result:", result)

print("Done. Replacement finished.")
