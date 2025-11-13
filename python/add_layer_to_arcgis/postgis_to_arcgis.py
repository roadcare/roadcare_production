"""
Script to clear and reload ArcGIS Online Feature Service layer from PostgreSQL/PostGIS
"""

import os
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import psycopg2
from psycopg2.extras import RealDictCursor
from shapely import wkb
from shapely.geometry import mapping
import json

# ===========================
# CONFIGURATION
# ===========================

# ArcGIS Online credentials
ARCGIS_URL = "https://www.arcgis.com"
ARCGIS_USERNAME = "roadcare"
ARCGIS_PASSWORD = "Antonin&TienSy2021"
# Or use API key: ARCGIS_API_KEY = "your_api_key"

# Feature Service URL (the REST endpoint of your layer)
FEATURE_LAYER_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo_Degradation/FeatureServer/7"

# PostgreSQL/PostGIS connection parameters
PG_HOST = "localhost"
PG_PORT = 5433
PG_DATABASE = "cd12_demo"
PG_USER = "diagway"
PG_PASSWORD = "diagway"

# SQL Query to fetch data from PostGIS
# IMPORTANT: Select geometry as WKB and include all attribute fields you need
# Transform from SRID 2154 (Lambert 93) to 4326 (WGS84) for ArcGIS Online
# Geometry type: MultiPolygon,2154
SQL_QUERY = """  
	select id,session_id,session_name,sous_classe
,surface,extension,filename,ST_AsEWKB(ST_Transform(geom, 4326)) as geom
from rendu.degradation
"""

# Geometry field name in your PostGIS table
GEOM_FIELD = "geom"

# Batch size for uploading features (ArcGIS has limits)
BATCH_SIZE = 1000


def connect_to_arcgis():
    """Connect to ArcGIS Online"""
    print("Connecting to ArcGIS Online...")
    try:
        # Option 1: Username/Password
        gis = GIS(ARCGIS_URL, ARCGIS_USERNAME, ARCGIS_PASSWORD)
        
        # Option 2: API Key (uncomment if using API key)
        # gis = GIS(ARCGIS_URL, api_key=ARCGIS_API_KEY)
        
        print(f"Successfully connected as: {gis.properties.user.username}")
        return gis
    except Exception as e:
        print(f"Error connecting to ArcGIS Online: {e}")
        raise


def connect_to_postgresql():
    """Connect to PostgreSQL/PostGIS database"""
    print("Connecting to PostgreSQL/PostGIS...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise


def clear_feature_layer(feature_layer):
    """Delete all features from the ArcGIS Online layer"""
    print("Clearing existing features from layer...")
    try:
        # Query all object IDs
        object_ids = feature_layer.query(where="1=1", return_ids_only=True)
        
        if object_ids['objectIds']:
            print(f"Found {len(object_ids['objectIds'])} features to delete")
            
            # Delete in batches if there are many features
            oids = object_ids['objectIds']
            for i in range(0, len(oids), BATCH_SIZE):
                batch_oids = oids[i:i + BATCH_SIZE]
                result = feature_layer.edit_features(deletes=batch_oids)
                print(f"Deleted batch {i//BATCH_SIZE + 1}: {len(batch_oids)} features")
            
            print("All existing features deleted successfully")
        else:
            print("No existing features to delete")
            
    except Exception as e:
        print(f"Error clearing features: {e}")
        raise


def convert_postgis_to_esri_geometry(wkb_geom, spatial_reference=4326):
    """
    Convert PostGIS WKB geometry to ArcGIS JSON geometry format
    
    Args:
        wkb_geom: Well-Known Binary geometry from PostGIS
        spatial_reference: WKID of the spatial reference (default: 4326 for WGS84)
    
    Returns:
        Dictionary in ArcGIS geometry format
    """
    try:
        # Convert WKB to Shapely geometry
        geom = wkb.loads(bytes(wkb_geom))
        
        # Convert to GeoJSON-like format
        geom_json = mapping(geom)
        
        # Convert to ArcGIS format
        geom_type = geom.geom_type
        
        if geom_type == "Point":
            esri_geom = {
                "x": geom_json["coordinates"][0],
                "y": geom_json["coordinates"][1],
                "spatialReference": {"wkid": spatial_reference}
            }
        elif geom_type == "LineString":
            esri_geom = {
                "paths": [geom_json["coordinates"]],
                "spatialReference": {"wkid": spatial_reference}
            }
        elif geom_type == "Polygon":
            esri_geom = {
                "rings": geom_json["coordinates"],
                "spatialReference": {"wkid": spatial_reference}
            }
        elif geom_type == "MultiPoint":
            esri_geom = {
                "points": geom_json["coordinates"],
                "spatialReference": {"wkid": spatial_reference}
            }
        elif geom_type == "MultiLineString":
            esri_geom = {
                "paths": geom_json["coordinates"],
                "spatialReference": {"wkid": spatial_reference}
            }
        elif geom_type == "MultiPolygon":
            # Flatten multipolygon rings
            rings = []
            for polygon in geom_json["coordinates"]:
                rings.extend(polygon)
            esri_geom = {
                "rings": rings,
                "spatialReference": {"wkid": spatial_reference}
            }
        else:
            raise ValueError(f"Unsupported geometry type: {geom_type}")
        
        return esri_geom
        
    except Exception as e:
        print(f"Error converting geometry: {e}")
        raise


def fetch_postgis_data(conn):
    """Fetch data from PostgreSQL/PostGIS"""
    print("Fetching data from PostGIS...")
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} records from PostGIS")
        cursor.close()
        return rows
    except Exception as e:
        print(f"Error fetching data from PostGIS: {e}")
        raise


def upload_features_to_arcgis(feature_layer, postgis_data):
    """Upload features from PostGIS to ArcGIS Online"""
    print("Preparing features for upload...")
    
    features = []
    
    for row in postgis_data:
        # Convert row to dictionary and handle geometry
        attributes = {}
        geometry = None
        
        for key, value in row.items():
            if key == GEOM_FIELD:
                # Convert PostGIS geometry to ArcGIS format
                if value is not None:
                    geometry = convert_postgis_to_esri_geometry(value)
            else:
                # Handle attribute values
                # Convert to JSON-serializable types
                if isinstance(value, (list, dict)):
                    attributes[key] = json.dumps(value)
                else:
                    attributes[key] = value
        
        # Create feature
        feature = {
            "attributes": attributes,
            "geometry": geometry
        }
        features.append(feature)
    
    # Upload in batches
    print(f"Uploading {len(features)} features in batches of {BATCH_SIZE}...")
    total_added = 0
    
    for i in range(0, len(features), BATCH_SIZE):
        batch = features[i:i + BATCH_SIZE]
        
        try:
            result = feature_layer.edit_features(adds=batch)
            
            # Check results
            if 'addResults' in result:
                success_count = sum(1 for r in result['addResults'] if r['success'])
                total_added += success_count
                print(f"Batch {i//BATCH_SIZE + 1}: Added {success_count}/{len(batch)} features")
                
                # Print any errors
                for idx, r in enumerate(result['addResults']):
                    if not r['success']:
                        print(f"  Error on feature {i + idx}: {r.get('error', 'Unknown error')}")
            
        except Exception as e:
            print(f"Error uploading batch {i//BATCH_SIZE + 1}: {e}")
            continue
    
    print(f"Total features successfully added: {total_added}/{len(features)}")
    return total_added


def main():
    """Main execution function"""
    print("=" * 60)
    print("PostGIS to ArcGIS Online Feature Service Sync")
    print("=" * 60)
    
    pg_conn = None
    
    try:
        # Connect to ArcGIS Online
        gis = connect_to_arcgis()
        feature_layer = FeatureLayer(FEATURE_LAYER_URL, gis)
        
        # Connect to PostgreSQL
        pg_conn = connect_to_postgresql()
        
        # Clear existing features
        clear_feature_layer(feature_layer)
        
        # Fetch data from PostGIS
        postgis_data = fetch_postgis_data(pg_conn)
        
        if not postgis_data:
            print("No data to upload")
            return
        
        # Upload features to ArcGIS Online
        upload_features_to_arcgis(feature_layer, postgis_data)
        
        print("=" * 60)
        print("Sync completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Script failed with error: {e}")
        raise
        
    finally:
        # Clean up connections
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed")


if __name__ == "__main__":
    main()
