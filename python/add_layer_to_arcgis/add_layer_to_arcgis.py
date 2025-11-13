"""
Script to add a new layer to ArcGIS Online Feature Service from PostgreSQL/PostGIS table
Project: roadare_sig_prod
"""

import psycopg2
import psycopg2.extras
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import json
from datetime import datetime
import traceback


class PostgresToArcGIS:
    """Class to handle data transfer from PostgreSQL to ArcGIS Online"""
    
    def __init__(self, pg_config, arcgis_username, arcgis_password):
        """
        Initialize connections
        
        Args:
            pg_config (dict): PostgreSQL connection parameters
            arcgis_username (str): ArcGIS Online username
            arcgis_password (str): ArcGIS Online password
        """
        self.pg_config = pg_config
        self.arcgis_username = arcgis_username
        self.arcgis_password = arcgis_password
        self.gis = None
        self.pg_conn = None
        
    def connect_arcgis(self):
        """Connect to ArcGIS Online"""
        try:
            print("Connecting to ArcGIS Online...")
            self.gis = GIS("https://www.arcgis.com", self.arcgis_username, self.arcgis_password)
            print(f"Successfully connected as: {self.gis.properties.user.username}")
            return True
        except Exception as e:
            print(f"Error connecting to ArcGIS Online: {str(e)}")
            return False
    
    def connect_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            print("Connecting to PostgreSQL...")
            self.pg_conn = psycopg2.connect(**self.pg_config)
            print("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {str(e)}")
            return False
    
    def get_table_schema(self, schema_name, table_name):
        """
        Get the schema information of a PostgreSQL table
        
        Args:
            schema_name (str): Schema name
            table_name (str): Table name
            
        Returns:
            list: List of column information dictionaries
        """
        cursor = self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """
            SELECT 
                column_name, 
                data_type, 
                udt_name,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        
        cursor.execute(query, (schema_name, table_name))
        columns = cursor.fetchall()
        cursor.close()
        
        return [dict(col) for col in columns]
    
    def convert_pg_type_to_esri_type(self, pg_type, udt_name):
        """
        Convert PostgreSQL data type to ESRI field type
        
        Args:
            pg_type (str): PostgreSQL data type
            udt_name (str): PostgreSQL UDT name
            
        Returns:
            str: ESRI field type
        """
        type_mapping = {
            'integer': 'esriFieldTypeInteger',
            'bigint': 'esriFieldTypeInteger',
            'smallint': 'esriFieldTypeSmallInteger',
            'numeric': 'esriFieldTypeDouble',
            'double precision': 'esriFieldTypeDouble',
            'real': 'esriFieldTypeSingle',
            'text': 'esriFieldTypeString',
            'character varying': 'esriFieldTypeString',
            'character': 'esriFieldTypeString',
            'boolean': 'esriFieldTypeString',
            'timestamp without time zone': 'esriFieldTypeDate',
            'timestamp with time zone': 'esriFieldTypeDate',
            'date': 'esriFieldTypeDate',
            'uuid': 'esriFieldTypeGUID',
            'USER-DEFINED': 'esriFieldTypeString'
        }
        
        return type_mapping.get(pg_type, 'esriFieldTypeString')
    
    def get_geometry_type(self, schema_name, table_name):
        """
        Get the geometry type of a PostGIS table
        
        Args:
            schema_name (str): Schema name
            table_name (str): Table name
            
        Returns:
            tuple: (geometry_column_name, geometry_type, srid)
        """
        cursor = self.pg_conn.cursor()
        
        query = """
            SELECT f_geometry_column, type, srid
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """
        
        cursor.execute(query, (schema_name, table_name))
        result = cursor.fetchone()
        cursor.close()
        
        return result if result else (None, None, None)
    
    def convert_geom_type_to_esri(self, postgis_type):
        """
        Convert PostGIS geometry type to ESRI geometry type
        
        Args:
            postgis_type (str): PostGIS geometry type
            
        Returns:
            str: ESRI geometry type
        """
        if not postgis_type:
            return None
            
        postgis_type = postgis_type.upper()
        
        type_mapping = {
            'POINT': 'esriGeometryPoint',
            'POINTM': 'esriGeometryPoint',
            'POINTZ': 'esriGeometryPoint',
            'LINESTRING': 'esriGeometryPolyline',
            'LINESTRINGM': 'esriGeometryPolyline',
            'LINESTRINGZ': 'esriGeometryPolyline',
            'POLYGON': 'esriGeometryPolygon',
            'POLYGONM': 'esriGeometryPolygon',
            'POLYGONZ': 'esriGeometryPolygon',
            'MULTIPOINT': 'esriGeometryMultipoint',
            'MULTIPOINTM': 'esriGeometryMultipoint',
            'MULTIPOINTZ': 'esriGeometryMultipoint',
            'MULTILINESTRING': 'esriGeometryPolyline',
            'MULTILINESTRINGM': 'esriGeometryPolyline',
            'MULTILINESTRINGZ': 'esriGeometryPolyline',
            'MULTIPOLYGON': 'esriGeometryPolygon',
            'MULTIPOLYGONM': 'esriGeometryPolygon',
            'MULTIPOLYGONZ': 'esriGeometryPolygon'
        }
        
        return type_mapping.get(postgis_type, 'esriGeometryPoint')
    
    def fetch_table_data(self, schema_name, table_name, limit=None, where_clause=None):
        """
        Fetch data from PostgreSQL table
        
        Args:
            schema_name (str): Schema name
            table_name (str): Table name
            limit (int): Maximum number of records to fetch
            where_clause (str): Optional WHERE clause
            
        Returns:
            list: List of records with geometry as GeoJSON
        """
        cursor = self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get geometry column
        geom_col, geom_type, srid = self.get_geometry_type(schema_name, table_name)
        
        # Get all columns
        columns_info = self.get_table_schema(schema_name, table_name)
        columns = [col['column_name'] for col in columns_info if col['column_name'] != geom_col]
        
        # Build query
        if geom_col:
            # Convert geometry to GeoJSON
            select_cols = ', '.join([f'"{col}"' for col in columns])
            geom_select = f'ST_AsGeoJSON("{geom_col}") as geom_json'
            query = f'SELECT {select_cols}, {geom_select} FROM "{schema_name}"."{table_name}"'
        else:
            select_cols = ', '.join([f'"{col}"' for col in columns])
            query = f'SELECT {select_cols} FROM "{schema_name}"."{table_name}"'
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        print(f"Executing query: {query}")
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        
        return [dict(record) for record in records]
    
    def create_layer_definition(self, layer_name, schema_name, table_name, geometry_type, srid):
        """
        Create layer definition for ArcGIS Online
        
        Args:
            layer_name (str): Name for the new layer
            schema_name (str): Schema name
            table_name (str): Table name
            geometry_type (str): ESRI geometry type
            srid (int): Spatial reference ID
            
        Returns:
            dict: Layer definition
        """
        columns_info = self.get_table_schema(schema_name, table_name)
        geom_col, _, _ = self.get_geometry_type(schema_name, table_name)
        
        # Create fields definition
        fields = []
        
        # Add ObjectID field (required by ArcGIS)
        fields.append({
            "name": "OBJECTID",
            "type": "esriFieldTypeOID",
            "alias": "OBJECTID",
            "sqlType": "sqlTypeOther",
            "nullable": False,
            "editable": False
        })
        
        # Add other fields
        for col in columns_info:
            if col['column_name'] != geom_col:
                field_type = self.convert_pg_type_to_esri_type(col['data_type'], col['udt_name'])
                
                field_def = {
                    "name": col['column_name'],
                    "type": field_type,
                    "alias": col['column_name'],
                    "sqlType": "sqlTypeOther",
                    "nullable": col['is_nullable'] == 'YES',
                    "editable": True
                }
                
                # Add length for string fields
                if field_type == 'esriFieldTypeString':
                    field_def['length'] = 255
                
                fields.append(field_def)
        
        # Create layer definition
        layer_definition = {
            "name": layer_name,
            "type": "Feature Layer",
            "displayField": "",
            "description": f"Layer created from PostgreSQL table {schema_name}.{table_name}",
            "geometryType": geometry_type,
            "spatialReference": {"wkid": srid},
            "fields": fields,
            "objectIdField": "OBJECTID",
            "globalIdField": "",
            "capabilities": "Query,Create,Update,Delete,Uploads,Editing"
        }
        
        return layer_definition
    
    def convert_geojson_to_esri_geometry(self, geojson_geom):
        """
        Convert GeoJSON geometry to ESRI geometry format
        
        Args:
            geojson_geom (dict): GeoJSON geometry object
            
        Returns:
            dict: ESRI geometry object
        """
        if not geojson_geom:
            return None
        
        geom_type = geojson_geom.get('type', '').upper()
        coordinates = geojson_geom.get('coordinates', [])
        
        if geom_type == 'POINT':
            # Point: {"x": x, "y": y}
            return {
                "x": coordinates[0],
                "y": coordinates[1]
            }
        
        elif geom_type == 'MULTIPOINT':
            # MultiPoint: {"points": [[x1, y1], [x2, y2], ...]}
            return {
                "points": coordinates
            }
        
        elif geom_type == 'LINESTRING':
            # LineString: {"paths": [[[x1, y1], [x2, y2], ...]]}
            return {
                "paths": [coordinates]
            }
        
        elif geom_type == 'MULTILINESTRING':
            # MultiLineString: {"paths": [[[x1, y1], ...], [[x3, y3], ...]]}
            return {
                "paths": coordinates
            }
        
        elif geom_type == 'POLYGON':
            # Polygon: {"rings": [[[x1, y1], [x2, y2], ..., [x1, y1]]]}
            # First ring is exterior, others are holes
            return {
                "rings": coordinates
            }
        
        elif geom_type == 'MULTIPOLYGON':
            # MultiPolygon: {"rings": [...all rings from all polygons...]}
            # Flatten all polygons into a single rings array
            all_rings = []
            for polygon in coordinates:
                all_rings.extend(polygon)
            return {
                "rings": all_rings
            }
        
        else:
            # Unsupported geometry type, return as-is
            return geojson_geom
    
    def convert_features_to_esri_format(self, records, geom_col_name='geom_json'):
        """
        Convert PostgreSQL records to ESRI feature format
        
        Args:
            records (list): List of records from PostgreSQL
            geom_col_name (str): Name of the geometry column (GeoJSON format)
            
        Returns:
            list: List of features in ESRI format
        """
        features = []
        
        for i, record in enumerate(records):
            # Extract geometry
            geom_json = record.get(geom_col_name)
            
            # Create attributes (exclude geometry column)
            attributes = {"OBJECTID": i + 1}
            for key, value in record.items():
                if key != geom_col_name:
                    # Convert datetime to timestamp
                    if isinstance(value, datetime):
                        attributes[key] = int(value.timestamp() * 1000)
                    else:
                        attributes[key] = value
            
            # Create feature
            feature = {
                "attributes": attributes
            }
            
            # Add geometry if exists
            if geom_json:
                try:
                    geojson_geometry = json.loads(geom_json)
                    # Convert GeoJSON to ESRI format
                    esri_geometry = self.convert_geojson_to_esri_geometry(geojson_geometry)
                    if esri_geometry:
                        feature["geometry"] = esri_geometry
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse geometry for feature {i+1}")
                except Exception as e:
                    print(f"Warning: Error converting geometry for feature {i+1}: {str(e)}")
            
            features.append(feature)
        
        return features
    
    def add_layer_to_feature_service(self, feature_service_url, layer_name, 
                                     schema_name, table_name, limit=1000, 
                                     where_clause=None):
        """
        Add a new layer to an existing Feature Service
        
        Args:
            feature_service_url (str): URL of the Feature Service
            layer_name (str): Name for the new layer
            schema_name (str): PostgreSQL schema name
            table_name (str): PostgreSQL table name
            limit (int): Maximum number of records to transfer
            where_clause (str): Optional WHERE clause for filtering
            
        Returns:
            bool: Success status
        """
        try:
            # Get Feature Service item
            print(f"Accessing Feature Service: {feature_service_url}")
            flc = FeatureLayerCollection(feature_service_url, self.gis)
            
            # Get geometry information
            geom_col, geom_type, srid = self.get_geometry_type(schema_name, table_name)
            
            if not geom_col:
                print("Warning: No geometry column found. Creating a table without geometry.")
                esri_geom_type = None
            else:
                esri_geom_type = self.convert_geom_type_to_esri(geom_type)
                print(f"Geometry type: {geom_type} (SRID: {srid}) -> {esri_geom_type}")
            
            # Create layer definition
            print("Creating layer definition...")
            layer_definition = self.create_layer_definition(
                layer_name, schema_name, table_name, esri_geom_type, srid or 2154
            )
            
            # Fetch data from PostgreSQL
            print(f"Fetching data from {schema_name}.{table_name}...")
            records = self.fetch_table_data(schema_name, table_name, limit, where_clause)
            print(f"Retrieved {len(records)} records")
            
            if not records:
                print("No records found. Aborting.")
                return False
            
            # Convert to ESRI format
            print("Converting data to ESRI format...")
            features = self.convert_features_to_esri_format(records)
            
            # Add layer to Feature Service
            print(f"Adding layer '{layer_name}' to Feature Service...")
            add_result = flc.manager.add_to_definition({
                "layers": [layer_definition]
            })
            
            if not add_result.get('success', False):
                print(f"Failed to add layer definition: {add_result}")
                return False
            
            print("Layer definition added successfully!")
            
            # Get the newly created layer
            flc_refreshed = FeatureLayerCollection(feature_service_url, self.gis)
            layers = flc_refreshed.layers
            
            # Find the new layer
            new_layer = None
            for layer in layers:
                if layer.properties.name == layer_name:
                    new_layer = layer
                    break
            
            if not new_layer:
                print("Could not find the newly created layer")
                return False
            
            # Add features in batches
            print(f"Adding {len(features)} features to the layer...")
            batch_size = 100
            for i in range(0, len(features), batch_size):
                batch = features[i:i+batch_size]
                result = new_layer.edit_features(adds=batch)
                
                if 'addResults' in result:
                    success_count = sum(1 for r in result['addResults'] if r['success'])
                    print(f"Batch {i//batch_size + 1}: Added {success_count}/{len(batch)} features")
                else:
                    print(f"Batch {i//batch_size + 1}: Error - {result}")
            
            print(f"Successfully added layer '{layer_name}' to Feature Service!")
            return True
            
        except Exception as e:
            print(f"Error adding layer to Feature Service: {str(e)}")
            traceback.print_exc()
            return False
    
    def close_connections(self):
        """Close all connections"""
        if self.pg_conn:
            self.pg_conn.close()
            print("PostgreSQL connection closed")


def main():
    """Main execution function"""
    
    # Configuration
    PG_CONFIG = {
        'host': 'localhost',
        'database': 'cd12_demo',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    ARCGIS_USERNAME = "roadcare"
    ARCGIS_PASSWORD = "Antonin&TienSy2021"
    FEATURE_SERVICE_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer"
    
    # Parameters for the new layer
    SCHEMA_NAME = "client"  # Change this to your schema
    TABLE_NAME = "route_client"  # Change this to your table name
    LAYER_NAME = "Route_Client_Layer"  # Change this to desired layer name
    LIMIT = 1000  # Maximum number of records to transfer (set to None for all)
    WHERE_CLAUSE = None  # Optional: "longueur > 1000" for filtering
    
    # Create instance
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        # Connect to both services
        if not pg_to_arcgis.connect_postgres():
            return
        
        if not pg_to_arcgis.connect_arcgis():
            return
        
        # Add layer to Feature Service
        success = pg_to_arcgis.add_layer_to_feature_service(
            FEATURE_SERVICE_URL,
            LAYER_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            LIMIT,
            WHERE_CLAUSE
        )
        
        if success:
            print("\n" + "="*50)
            print("Layer successfully added to ArcGIS Online!")
            print("="*50)
        else:
            print("\n" + "="*50)
            print("Failed to add layer to ArcGIS Online")
            print("="*50)
    
    finally:
        # Clean up
        pg_to_arcgis.close_connections()


if __name__ == "__main__":
    main()
