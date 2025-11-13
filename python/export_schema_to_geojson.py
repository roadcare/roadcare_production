import os
import sys
import psycopg2
from psycopg2 import sql
import subprocess
import argparse
from pathlib import Path

def get_tables_with_geometry(conn, schema_name):
    """
    Get all tables in the schema that have geometry columns
    """
    query = """
        SELECT 
            f_table_name as table_name,
            f_geometry_column as geom_column,
            type as geometry_type,
            srid
        FROM geometry_columns
        WHERE f_table_schema = %s
        ORDER BY f_table_name;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (schema_name,))
        return cur.fetchall()

def export_table_to_geojson_ogr2ogr(db_params, schema_name, table_name, geom_column, output_folder):
    """
    Export a table to GeoJSON using ogr2ogr (fastest method)
    """
    output_file = os.path.join(output_folder, f"{table_name}.geojson")
    
    # Build connection string
    conn_str = f"PG:host={db_params['host']} port={db_params['port']} " \
               f"dbname={db_params['dbname']} user={db_params['user']} " \
               f"password={db_params['password']}"
    
    # Build ogr2ogr command
    cmd = [
        'ogr2ogr',
        '-f', 'GeoJSON',
        output_file,
        conn_str,
        '-sql', f'SELECT * FROM {schema_name}.{table_name}',
        '-nln', table_name
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✓ Exported: {table_name} -> {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to export {table_name}: {e.stderr}")
        return False
    except FileNotFoundError:
        print("✗ ogr2ogr not found. Please install GDAL/OGR.")
        return False

def export_table_to_geojson_psycopg2(conn, schema_name, table_name, geom_column, output_folder):
    """
    Export a table to GeoJSON using psycopg2 and ST_AsGeoJSON (alternative method)
    """
    output_file = os.path.join(output_folder, f"{table_name}.geojson")
    
    # Build query to get GeoJSON
    query = sql.SQL("""
        SELECT jsonb_build_object(
            'type', 'FeatureCollection',
            'features', jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON({geom_col})::jsonb,
                    'properties', to_jsonb(row) - {geom_col}
                )
            )
        )
        FROM (SELECT * FROM {schema}.{table}) row
    """).format(
        geom_col=sql.Identifier(geom_column),
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name)
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            geojson_data = cur.fetchone()[0]
            
            with open(output_file, 'w', encoding='utf-8') as f:
                import json
                json.dump(geojson_data, f, ensure_ascii=False, indent=2)
            
            print(f"✓ Exported: {table_name} -> {output_file}")
            return True
    except Exception as e:
        print(f"✗ Failed to export {table_name}: {str(e)}")
        return False

def export_schema_to_geojson(db_params, schema_name, output_folder, method='ogr2ogr'):
    """
    Export all tables from a schema to GeoJSON files
    """
    # Create output folder if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    try:
        conn = psycopg2.connect(**db_params)
        print(f"✓ Connected to database: {db_params['dbname']}")
    except Exception as e:
        print(f"✗ Failed to connect to database: {str(e)}")
        sys.exit(1)
    
    try:
        # Get all tables with geometry
        tables = get_tables_with_geometry(conn, schema_name)
        
        if not tables:
            print(f"No tables with geometry found in schema '{schema_name}'")
            return
        
        print(f"\nFound {len(tables)} table(s) with geometry in schema '{schema_name}'")
        print("-" * 60)
        
        success_count = 0
        fail_count = 0
        
        # Export each table
        for table_name, geom_column, geom_type, srid in tables:
            print(f"\nExporting: {table_name} (geom: {geom_column}, type: {geom_type}, SRID: {srid})")
            
            if method == 'ogr2ogr':
                result = export_table_to_geojson_ogr2ogr(
                    db_params, schema_name, table_name, geom_column, output_folder
                )
            else:
                result = export_table_to_geojson_psycopg2(
                    conn, schema_name, table_name, geom_column, output_folder
                )
            
            if result:
                success_count += 1
            else:
                fail_count += 1
        
        print("\n" + "=" * 60)
        print(f"Export complete: {success_count} succeeded, {fail_count} failed")
        print(f"Output folder: {os.path.abspath(output_folder)}")
        
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Export PostgreSQL/PostGIS tables to GeoJSON files'
    )
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', default='5433', help='Database port')
    parser.add_argument('--dbname', default='CD93_2023', help='Database name')
    parser.add_argument('--user', default='diagway', help='Database user')
    parser.add_argument('--password', default='diagway', help='Database password')
    parser.add_argument('--schema', default='rendu_202507', help='Schema name to export')
    parser.add_argument('--output', default='D:/Tmp/CD93_20250701', help='Output folder')
    parser.add_argument('--method', choices=['ogr2ogr', 'psycopg2'], default='ogr2ogr',
                       help='Export method (ogr2ogr is faster)')
    
    args = parser.parse_args()
    
    db_params = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    export_schema_to_geojson(db_params, args.schema, args.output, args.method)

if __name__ == '__main__':
    main()