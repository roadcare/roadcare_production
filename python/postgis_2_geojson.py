import psycopg2
import json
from decimal import Decimal
from datetime import datetime, date
from psycopg2.extras import RealDictCursor

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal, datetime, and other types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super(DecimalEncoder, self).default(obj)

def export_postgis_to_geojson(
    host,
    database,
    user,
    password,
    query,
    output_file,
    port=5432,
    geom_column='geom'
):
    """
    Export PostGIS table/query to GeoJSON format
    
    Parameters:
    -----------
    host : str
        Database host
    database : str
        Database name
    user : str
        Database user
    password : str
        Database password
    query : str
        SQL query to execute (must include geometry column)
    output_file : str
        Output GeoJSON file path
    port : int
        Database port (default: 5432)
    geom_column : str
        Name of the geometry column (default: 'geom')
    """
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Wrap the query to convert geometry to GeoJSON
        # This approach works better than string replacement
        geojson_query = f"""
        SELECT 
            ST_AsGeoJSON(t.{geom_column})::json as geometry,
            t.* 
        FROM ({query}) AS t
        """
        
        print(f"Executing query...")
        cursor.execute(geojson_query)
        
        # Fetch all results
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} records")
        
        # Build GeoJSON structure
        features = []
        for row in rows:
            # Extract geometry (already as JSON/dict)
            geom = row.pop('geometry', None)
            
            # Remove the original geometry column (binary format)
            row.pop(geom_column, None)
            
            # Convert Decimal and other types to JSON-serializable types
            properties = {}
            for key, value in row.items():
                if isinstance(value, Decimal):
                    properties[key] = float(value)
                elif isinstance(value, (datetime, date)):
                    properties[key] = value.isoformat()
                elif isinstance(value, bytes):
                    properties[key] = value.decode('utf-8')
                else:
                    properties[key] = value
            
            # Build feature
            feature = {
                "type": "Feature",
                "geometry": geom,
                "properties": properties
            }
            features.append(feature)
        
        # Create GeoJSON FeatureCollection
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        # Write to file with custom encoder
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
        
        print(f"✓ Successfully exported to {output_file}")
        print(f"  Total features: {len(features)}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def example():
    """Example usage of the export function"""
    
    # Database connection parameters
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'rcp_cd16',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    # Example 1: Export entire table
    query1 = """
        SELECT *
        FROM rendu.zh_u02_l200
        WHERE axe IS NOT NULL
    """
    
    export_postgis_to_geojson(
        **DB_CONFIG,
        query=query1,
        output_file='G:/01_Affaires/06-AFFAIRES_ROADCARE/40_CD16_CHARENTE/04-Traitement/05_Export/GEOJSON/20251023_cd16_zh_u02_l200.geojson',
        geom_column='geom'
    )
    
    

if __name__ == "__main__":
    example()