"""
Geometry Troubleshooting Script
Helps diagnose and fix geometry issues before uploading to ArcGIS Online
"""

import psycopg2
import psycopg2.extras
import json
from tabulate import tabulate

# Configuration
PG_CONFIG = {
    'host': 'localhost',
    'database': 'cd12_demo',
    'user': 'diagway',
    'password': 'diagway',
    'port': 5433
}


def check_geometry_validity(schema_name, table_name):
    """Check if all geometries in a table are valid"""
    print("\n" + "="*60)
    print(f"Checking geometry validity: {schema_name}.{table_name}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get geometry column
        cursor.execute("""
            SELECT f_geometry_column, type, srid
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema_name, table_name))
        
        geom_info = cursor.fetchone()
        
        if not geom_info:
            print("No geometry column found in this table")
            cursor.close()
            conn.close()
            return
        
        geom_col, geom_type, srid = geom_info
        
        print(f"Geometry column: {geom_col}")
        print(f"Geometry type: {geom_type}")
        print(f"SRID: {srid}\n")
        
        # Check for NULL geometries
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{schema_name}"."{table_name}"
            WHERE "{geom_col}" IS NULL;
        """)
        null_count = cursor.fetchone()[0]
        
        # Check for invalid geometries
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{schema_name}"."{table_name}"
            WHERE "{geom_col}" IS NOT NULL 
            AND NOT ST_IsValid("{geom_col}");
        """)
        invalid_count = cursor.fetchone()[0]
        
        # Check for empty geometries
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{schema_name}"."{table_name}"
            WHERE "{geom_col}" IS NOT NULL 
            AND ST_IsEmpty("{geom_col}");
        """)
        empty_count = cursor.fetchone()[0]
        
        # Total count
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{schema_name}"."{table_name}";
        """)
        total_count = cursor.fetchone()[0]
        
        # Display results
        results = [
            ["Total records", total_count, ""],
            ["NULL geometries", null_count, "✓" if null_count == 0 else "⚠"],
            ["Invalid geometries", invalid_count, "✓" if invalid_count == 0 else "✗"],
            ["Empty geometries", empty_count, "✓" if empty_count == 0 else "⚠"],
            ["Valid geometries", total_count - null_count - invalid_count - empty_count, "✓"]
        ]
        
        print(tabulate(results, headers=["Category", "Count", "Status"], tablefmt="grid"))
        
        # If there are invalid geometries, show them
        if invalid_count > 0:
            print("\n" + "="*60)
            print("Invalid Geometries Details:")
            print("="*60 + "\n")
            
            cursor.execute(f"""
                SELECT 
                    id,
                    ST_IsValidReason("{geom_col}") as reason
                FROM "{schema_name}"."{table_name}"
                WHERE "{geom_col}" IS NOT NULL 
                AND NOT ST_IsValid("{geom_col}")
                LIMIT 10;
            """)
            
            invalid_geoms = cursor.fetchall()
            
            print(tabulate(invalid_geoms, headers=["ID", "Reason"], tablefmt="grid"))
            print(f"\n(Showing first 10 of {invalid_count} invalid geometries)")
        
        cursor.close()
        conn.close()
        
        return {
            'total': total_count,
            'null': null_count,
            'invalid': invalid_count,
            'empty': empty_count,
            'valid': total_count - null_count - invalid_count - empty_count
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def fix_invalid_geometries(schema_name, table_name, dry_run=True):
    """
    Fix invalid geometries using ST_MakeValid
    
    Args:
        schema_name (str): Schema name
        table_name (str): Table name
        dry_run (bool): If True, only show what would be fixed without actually fixing
    """
    print("\n" + "="*60)
    print(f"Fixing invalid geometries: {schema_name}.{table_name}")
    print(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL FIX'}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get geometry column
        cursor.execute("""
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema_name, table_name))
        
        geom_info = cursor.fetchone()
        
        if not geom_info:
            print("No geometry column found in this table")
            cursor.close()
            conn.close()
            return
        
        geom_col = geom_info[0]
        
        # Count invalid geometries
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{schema_name}"."{table_name}"
            WHERE "{geom_col}" IS NOT NULL 
            AND NOT ST_IsValid("{geom_col}");
        """)
        invalid_count = cursor.fetchone()[0]
        
        if invalid_count == 0:
            print("No invalid geometries found. Nothing to fix!")
            cursor.close()
            conn.close()
            return
        
        print(f"Found {invalid_count} invalid geometries")
        
        if dry_run:
            print("\nThis is a DRY RUN. No changes will be made.")
            print("To actually fix the geometries, run with dry_run=False")
        else:
            print("\nFixing geometries using ST_MakeValid...")
            
            cursor.execute(f"""
                UPDATE "{schema_name}"."{table_name}"
                SET "{geom_col}" = ST_MakeValid("{geom_col}")
                WHERE "{geom_col}" IS NOT NULL 
                AND NOT ST_IsValid("{geom_col}");
            """)
            
            conn.commit()
            
            print(f"✓ Fixed {invalid_count} invalid geometries")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


def test_geometry_conversion(schema_name, table_name, limit=5):
    """
    Test geometry conversion from PostGIS to ESRI format
    Shows sample geometries and their converted format
    """
    print("\n" + "="*60)
    print(f"Testing geometry conversion: {schema_name}.{table_name}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get geometry column
        cursor.execute("""
            SELECT f_geometry_column, type
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema_name, table_name))
        
        geom_info = cursor.fetchone()
        
        if not geom_info:
            print("No geometry column found in this table")
            cursor.close()
            conn.close()
            return
        
        geom_col, geom_type = geom_info
        
        print(f"Geometry column: {geom_col}")
        print(f"Geometry type: {geom_type}\n")
        
        # Get sample geometries
        cursor.execute(f"""
            SELECT 
                id,
                ST_AsGeoJSON("{geom_col}") as geom_json,
                ST_GeometryType("{geom_col}") as geom_type,
                ST_NPoints("{geom_col}") as num_points,
                ST_IsValid("{geom_col}") as is_valid
            FROM "{schema_name}"."{table_name}"
            WHERE "{geom_col}" IS NOT NULL
            LIMIT %s;
        """, (limit,))
        
        records = cursor.fetchall()
        
        if not records:
            print("No geometries found in table")
            cursor.close()
            conn.close()
            return
        
        print(f"Testing {len(records)} sample geometries:\n")
        
        for i, record in enumerate(records, 1):
            print(f"Record {i} (ID: {record['id']}):")
            print(f"  Type: {record['geom_type']}")
            print(f"  Points: {record['num_points']}")
            print(f"  Valid: {'✓' if record['is_valid'] else '✗'}")
            
            # Parse GeoJSON
            geojson = json.loads(record['geom_json'])
            print(f"  GeoJSON type: {geojson['type']}")
            
            # Convert to ESRI format
            esri_geom = convert_geojson_to_esri(geojson)
            
            print(f"  ESRI format keys: {list(esri_geom.keys())}")
            
            # Show structure for polygons
            if 'rings' in esri_geom:
                print(f"  Number of rings: {len(esri_geom['rings'])}")
                for j, ring in enumerate(esri_geom['rings']):
                    print(f"    Ring {j+1}: {len(ring)} points")
            elif 'paths' in esri_geom:
                print(f"  Number of paths: {len(esri_geom['paths'])}")
                for j, path in enumerate(esri_geom['paths']):
                    print(f"    Path {j+1}: {len(path)} points")
            elif 'points' in esri_geom:
                print(f"  Number of points: {len(esri_geom['points'])}")
            elif 'x' in esri_geom and 'y' in esri_geom:
                print(f"  Coordinates: ({esri_geom['x']}, {esri_geom['y']})")
            
            print()
        
        cursor.close()
        conn.close()
        
        print("✓ Geometry conversion test completed successfully")
        
    except Exception as e:
        print(f"Error during conversion test: {str(e)}")
        import traceback
        traceback.print_exc()


def convert_geojson_to_esri(geojson_geom):
    """
    Convert GeoJSON geometry to ESRI geometry format
    (Same function as in the main script)
    """
    if not geojson_geom:
        return None
    
    geom_type = geojson_geom.get('type', '').upper()
    coordinates = geojson_geom.get('coordinates', [])
    
    if geom_type == 'POINT':
        return {"x": coordinates[0], "y": coordinates[1]}
    
    elif geom_type == 'MULTIPOINT':
        return {"points": coordinates}
    
    elif geom_type == 'LINESTRING':
        return {"paths": [coordinates]}
    
    elif geom_type == 'MULTILINESTRING':
        return {"paths": coordinates}
    
    elif geom_type == 'POLYGON':
        return {"rings": coordinates}
    
    elif geom_type == 'MULTIPOLYGON':
        all_rings = []
        for polygon in coordinates:
            all_rings.extend(polygon)
        return {"rings": all_rings}
    
    else:
        return geojson_geom


def get_geometry_statistics(schema_name, table_name):
    """Get detailed statistics about geometries in a table"""
    print("\n" + "="*60)
    print(f"Geometry Statistics: {schema_name}.{table_name}")
    print("="*60 + "\n")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Get geometry column
        cursor.execute("""
            SELECT f_geometry_column, type, srid
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema_name, table_name))
        
        geom_info = cursor.fetchone()
        
        if not geom_info:
            print("No geometry column found in this table")
            cursor.close()
            conn.close()
            return
        
        geom_col, geom_type, srid = geom_info
        
        # Get statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT("{geom_col}") as non_null_count,
                MIN(ST_NPoints("{geom_col}")) as min_points,
                MAX(ST_NPoints("{geom_col}")) as max_points,
                AVG(ST_NPoints("{geom_col}"))::integer as avg_points,
                MIN(ST_Area("{geom_col}")) as min_area,
                MAX(ST_Area("{geom_col}")) as max_area,
                AVG(ST_Area("{geom_col}"))::numeric(10,2) as avg_area
            FROM "{schema_name}"."{table_name}";
        """)
        
        stats = cursor.fetchone()
        
        info = [
            ["Geometry column", geom_col],
            ["Geometry type", geom_type],
            ["SRID", srid],
            ["Total records", stats[0]],
            ["Non-null geometries", stats[1]],
            ["Min points per geometry", stats[2] or "N/A"],
            ["Max points per geometry", stats[3] or "N/A"],
            ["Avg points per geometry", stats[4] or "N/A"]
        ]
        
        # Only show area for polygons
        if 'POLYGON' in geom_type.upper():
            info.extend([
                ["Min area", f"{stats[5]:.2f}" if stats[5] else "N/A"],
                ["Max area", f"{stats[6]:.2f}" if stats[6] else "N/A"],
                ["Avg area", f"{stats[7]:.2f}" if stats[7] else "N/A"]
            ])
        
        print(tabulate(info, tablefmt="grid"))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


def main_menu():
    """Interactive menu for geometry troubleshooting"""
    while True:
        print("\n" + "="*60)
        print("Geometry Troubleshooting Menu")
        print("="*60)
        print("\n1. Check geometry validity")
        print("2. Get geometry statistics")
        print("3. Test geometry conversion (5 samples)")
        print("4. Fix invalid geometries (DRY RUN)")
        print("5. Fix invalid geometries (ACTUAL FIX)")
        print("0. Exit")
        
        choice = input("\nEnter your choice: ").strip()
        
        if choice == "0":
            break
        
        if choice in ["1", "2", "3", "4", "5"]:
            schema = input("Enter schema name (e.g., client, public): ").strip()
            table = input("Enter table name: ").strip()
            
            if choice == "1":
                check_geometry_validity(schema, table)
            elif choice == "2":
                get_geometry_statistics(schema, table)
            elif choice == "3":
                test_geometry_conversion(schema, table, limit=5)
            elif choice == "4":
                fix_invalid_geometries(schema, table, dry_run=True)
            elif choice == "5":
                confirm = input("\n⚠ WARNING: This will modify your database. Are you sure? (yes/no): ").strip().lower()
                if confirm == "yes":
                    fix_invalid_geometries(schema, table, dry_run=False)
                else:
                    print("Operation cancelled.")
        else:
            print("Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    # Check if tabulate is installed
    try:
        import tabulate
    except ImportError:
        print("Installing required package 'tabulate'...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'tabulate'])
        import tabulate
    
    main_menu()
