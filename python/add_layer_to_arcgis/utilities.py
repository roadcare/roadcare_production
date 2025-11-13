"""
Utility script to test connections and explore the database structure
"""

import psycopg2
import psycopg2.extras
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from tabulate import tabulate


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


def test_postgres_connection():
    """Test PostgreSQL connection and show database info"""
    print("\n" + "="*60)
    print("Testing PostgreSQL Connection")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✓ Connected to PostgreSQL")
        print(f"  Version: {version.split(',')[0]}")
        
        # Get PostGIS version
        cursor.execute("SELECT PostGIS_version();")
        postgis_version = cursor.fetchone()[0]
        print(f"  PostGIS: {postgis_version}")
        
        # Get database size
        cursor.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database()));
        """)
        db_size = cursor.fetchone()[0]
        print(f"  Database size: {db_size}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL")
        print(f"  Error: {str(e)}")
        return False


def test_arcgis_connection():
    """Test ArcGIS Online connection and show user info"""
    print("\n" + "="*60)
    print("Testing ArcGIS Online Connection")
    print("="*60 + "\n")
    
    try:
        gis = GIS("https://www.arcgis.com", ARCGIS_USERNAME, ARCGIS_PASSWORD)
        
        print(f"✓ Connected to ArcGIS Online")
        print(f"  Username: {gis.properties.user.username}")
        print(f"  Full Name: {gis.properties.user.fullName}")
        print(f"  Email: {gis.properties.user.email}")
        print(f"  Role: {gis.properties.user.role}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to ArcGIS Online")
        print(f"  Error: {str(e)}")
        return False


def test_feature_service():
    """Test access to Feature Service and list existing layers"""
    print("\n" + "="*60)
    print("Testing Feature Service Access")
    print("="*60 + "\n")
    
    try:
        gis = GIS("https://www.arcgis.com", ARCGIS_USERNAME, ARCGIS_PASSWORD)
        flc = FeatureLayerCollection(FEATURE_SERVICE_URL, gis)
        
        print(f"✓ Successfully accessed Feature Service")
        print(f"  Name: {flc.properties.name}")
        print(f"  Service Description: {flc.properties.serviceDescription}")
        
        print(f"\n  Existing Layers ({len(flc.layers)}):")
        for i, layer in enumerate(flc.layers, 1):
            print(f"    {i}. {layer.properties.name} (ID: {layer.properties.id})")
            print(f"       Type: {layer.properties.geometryType}")
            print(f"       Features: {layer.query(return_count_only=True)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to access Feature Service")
        print(f"  Error: {str(e)}")
        return False


def list_schemas():
    """List all schemas in the database"""
    print("\n" + "="*60)
    print("Database Schemas")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name;
        """)
        
        schemas = cursor.fetchall()
        
        print("Available schemas:")
        for schema in schemas:
            print(f"  - {schema[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")


def list_tables_in_schema(schema_name):
    """List all tables in a specific schema with row counts"""
    print("\n" + "="*60)
    print(f"Tables in schema: {schema_name}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """, (schema_name,))
        
        tables = cursor.fetchall()
        
        if not tables:
            print(f"No tables found in schema '{schema_name}'")
            cursor.close()
            conn.close()
            return
        
        table_info = []
        for table in tables:
            table_name = table[0]
            
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";')
            row_count = cursor.fetchone()[0]
            
            # Check if has geometry
            cursor.execute("""
                SELECT f_geometry_column, type, srid
                FROM geometry_columns
                WHERE f_table_schema = %s AND f_table_name = %s;
            """, (schema_name, table_name))
            
            geom_info = cursor.fetchone()
            
            if geom_info:
                geom_col, geom_type, srid = geom_info
                geom_str = f"{geom_type} (SRID: {srid})"
            else:
                geom_str = "No geometry"
            
            table_info.append([table_name, f"{row_count:,}", geom_str])
        
        print(tabulate(table_info, 
                      headers=["Table Name", "Row Count", "Geometry"],
                      tablefmt="grid"))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")


def describe_table(schema_name, table_name):
    """Show detailed information about a table"""
    print("\n" + "="*60)
    print(f"Table: {schema_name}.{table_name}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get columns
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema_name, table_name))
        
        columns = cursor.fetchall()
        
        if not columns:
            print(f"Table '{schema_name}.{table_name}' not found")
            cursor.close()
            conn.close()
            return
        
        # Format column info
        col_info = []
        for col in columns:
            col_name, data_type, max_length, nullable, default = col
            
            type_str = data_type
            if max_length:
                type_str += f"({max_length})"
            
            nullable_str = "YES" if nullable == "YES" else "NO"
            default_str = str(default) if default else ""
            
            col_info.append([col_name, type_str, nullable_str, default_str])
        
        print("Columns:")
        print(tabulate(col_info,
                      headers=["Column Name", "Data Type", "Nullable", "Default"],
                      tablefmt="grid"))
        
        # Get geometry info
        cursor.execute("""
            SELECT f_geometry_column, type, srid, coord_dimension
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema_name, table_name))
        
        geom_info = cursor.fetchone()
        
        if geom_info:
            geom_col, geom_type, srid, coord_dim = geom_info
            print(f"\nGeometry Information:")
            print(f"  Column: {geom_col}")
            print(f"  Type: {geom_type}")
            print(f"  SRID: {srid}")
            print(f"  Dimensions: {coord_dim}")
            
            # Get extent
            cursor.execute(f"""
                SELECT 
                    ST_XMin(ST_Extent("{geom_col}")) as xmin,
                    ST_YMin(ST_Extent("{geom_col}")) as ymin,
                    ST_XMax(ST_Extent("{geom_col}")) as xmax,
                    ST_YMax(ST_Extent("{geom_col}")) as ymax
                FROM "{schema_name}"."{table_name}"
                WHERE "{geom_col}" IS NOT NULL;
            """)
            
            extent = cursor.fetchone()
            if extent and extent[0]:
                print(f"  Extent: [{extent[0]:.2f}, {extent[1]:.2f}, {extent[2]:.2f}, {extent[3]:.2f}]")
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";')
        row_count = cursor.fetchone()[0]
        print(f"\nTotal Rows: {row_count:,}")
        
        # Sample data
        print("\nSample Data (first 3 rows):")
        col_names = [col[0] for col in columns if not col[1].startswith('geometry')]
        
        if col_names:
            cursor.execute(f"""
                SELECT {', '.join([f'"{c}"' for c in col_names])}
                FROM "{schema_name}"."{table_name}"
                LIMIT 3;
            """)
            
            sample_data = cursor.fetchall()
            
            if sample_data:
                print(tabulate(sample_data, headers=col_names, tablefmt="grid"))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


def get_unique_axes():
    """Get list of unique axes from client.troncon_client"""
    print("\n" + "="*60)
    print("Unique Axes in client.troncon_client")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                axe,
                COUNT(*) as segment_count,
                MIN(cumuld) as min_cumul,
                MAX(cumulf) as max_cumul,
                MAX(cumulf) - MIN(cumuld) as total_length
            FROM client.troncon_client
            GROUP BY axe
            ORDER BY axe;
        """)
        
        axes = cursor.fetchall()
        
        if axes:
            print(tabulate(axes,
                          headers=["Axe", "Segments", "Min Cumul", "Max Cumul", "Total Length"],
                          tablefmt="grid",
                          floatfmt=".2f"))
        else:
            print("No data found in client.troncon_client")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")


def run_all_tests():
    """Run all connection tests"""
    print("\n" + "="*70)
    print(" " * 20 + "SYSTEM DIAGNOSTICS")
    print("="*70)
    
    pg_ok = test_postgres_connection()
    arcgis_ok = test_arcgis_connection()
    
    if pg_ok and arcgis_ok:
        fs_ok = test_feature_service()
        
        if fs_ok:
            print("\n" + "="*70)
            print("✓ All systems operational!")
            print("="*70)
            return True
    
    print("\n" + "="*70)
    print("✗ Some systems are not operational. Please check the errors above.")
    print("="*70)
    return False


def main_menu():
    """Interactive menu for utility functions"""
    while True:
        print("\n" + "="*60)
        print("PostgreSQL to ArcGIS - Utility Menu")
        print("="*60)
        print("\n1. Test all connections")
        print("2. Test PostgreSQL connection")
        print("3. Test ArcGIS Online connection")
        print("4. Test Feature Service access")
        print("5. List database schemas")
        print("6. List tables in client schema")
        print("7. List tables in public schema")
        print("8. Describe client.route_client")
        print("9. Describe client.troncon_client")
        print("10. Describe public.image")
        print("11. Describe public.session")
        print("12. Get unique axes")
        print("0. Exit")
        
        choice = input("\nEnter your choice: ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            run_all_tests()
        elif choice == "2":
            test_postgres_connection()
        elif choice == "3":
            test_arcgis_connection()
        elif choice == "4":
            test_feature_service()
        elif choice == "5":
            list_schemas()
        elif choice == "6":
            list_tables_in_schema("client")
        elif choice == "7":
            list_tables_in_schema("public")
        elif choice == "8":
            describe_table("client", "route_client")
        elif choice == "9":
            describe_table("client", "troncon_client")
        elif choice == "10":
            describe_table("public", "image")
        elif choice == "11":
            describe_table("public", "session")
        elif choice == "12":
            get_unique_axes()
        else:
            print("Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    # Check if tabulate is installed
    try:
        import tabulate
    except ImportError:
        print("Warning: 'tabulate' package not found. Installing...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'tabulate'])
        import tabulate
    
    main_menu()
