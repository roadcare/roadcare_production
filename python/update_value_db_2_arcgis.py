import psycopg2
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from psycopg2.extras import RealDictCursor
import time

def get_largeur_from_postgres(host, database, user, password, port, table_name, schema='public'):
    """
    Get id and largeur values from PostgreSQL table
    
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
    port : int
        Database port
    table_name : str
        Table name
    schema : str
        Schema name (default: 'public')
    
    Returns:
    --------
    dict : Dictionary with id as key and largeur as value
    """
    
    try:
        print(f"Connecting to PostgreSQL database...")
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query to get id and largeur
        query = f"""
            SELECT id, largeur
            FROM {schema}.{table_name}
            WHERE largeur IS NOT NULL
        """
        
        print(f"Executing query: {query}")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        print(f"✓ Retrieved {len(rows)} records from PostgreSQL")
        
        # Create dictionary: {id: largeur}
        largeur_dict = {row['id']: float(row['largeur']) for row in rows}
        
        cursor.close()
        conn.close()
        
        return largeur_dict
        
    except Exception as e:
        print(f"✗ PostgreSQL Error: {e}")
        raise


def update_agol_feature_service(
    feature_service_url,
    largeur_dict,
    id_field='id',
    largeur_field='largeur',
    username=None,
    password=None,
    portal_url='https://www.arcgis.com',
    batch_size=1000
):
    """
    Update ArcGIS Online Feature Service with largeur values
    
    Parameters:
    -----------
    feature_service_url : str
        URL of the feature service layer
    largeur_dict : dict
        Dictionary with {id: largeur_value}
    id_field : str
        Name of the ID field in feature service (default: 'id')
    largeur_field : str
        Name of the largeur field to update (default: 'largeur')
    username : str
        ArcGIS Online username
    password : str
        ArcGIS Online password
    portal_url : str
        Portal URL
    batch_size : int
        Number of features to update per batch (default: 1000)
    """
    
    try:
        # Connect to ArcGIS Online
        if username and password:
            print(f"Connecting to {portal_url} as {username}...")
            gis = GIS(portal_url, username, password)
        else:
            print(f"Connecting to {portal_url} anonymously...")
            gis = GIS(portal_url)
        
        print(f"✓ Connected successfully")
        
        # Create FeatureLayer object
        print(f"Accessing feature layer...")
        feature_layer = FeatureLayer(feature_service_url, gis)
        
        # Get layer properties
        layer_props = feature_layer.properties
        print(f"✓ Layer: {layer_props.get('name', 'Unknown')}")
        
        # Query all features
        print(f"Querying features...")
        feature_set = feature_layer.query(where='1=1', out_fields=f'{id_field},{largeur_field}', return_geometry=False)
        
        features = feature_set.features
        print(f"✓ Retrieved {len(features)} features from ArcGIS Online")
        
        # Prepare updates
        updates = []
        matched_count = 0
        not_found_count = 0
        
        for feature in features:
            feature_id = feature.attributes.get(id_field)
            
            if feature_id in largeur_dict:
                # Update the largeur value
                feature.attributes[largeur_field] = largeur_dict[feature_id]
                updates.append(feature)
                matched_count += 1
            else:
                not_found_count += 1
        
        print(f"\nUpdate summary:")
        print(f"  Matched features: {matched_count}")
        print(f"  Not found in PostgreSQL: {not_found_count}")
        print(f"  Total updates to perform: {len(updates)}")
        
        if len(updates) == 0:
            print("⚠ No features to update!")
            return
        
        # Update features in batches
        total_updated = 0
        failed_updates = []
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(updates) + batch_size - 1) // batch_size
            
            print(f"\nUpdating batch {batch_num}/{total_batches} ({len(batch)} features)...")
            
            try:
                result = feature_layer.edit_features(updates=batch)
                
                # Check results
                if result.get('updateResults'):
                    success_count = sum(1 for r in result['updateResults'] if r.get('success'))
                    failed_count = len(result['updateResults']) - success_count
                    
                    total_updated += success_count
                    
                    print(f"  ✓ Success: {success_count}")
                    if failed_count > 0:
                        print(f"  ✗ Failed: {failed_count}")
                        failed_updates.extend([r for r in result['updateResults'] if not r.get('success')])
                else:
                    print(f"  ⚠ Unexpected result format")
                
                # Small delay between batches to avoid rate limiting
                if i + batch_size < len(updates):
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  ✗ Batch update failed: {e}")
                failed_updates.extend(batch)
        
        print(f"\n{'='*60}")
        print(f"Update completed!")
        print(f"  Total successfully updated: {total_updated}")
        print(f"  Total failed: {len(failed_updates)}")
        print(f"{'='*60}")
        
        if failed_updates and len(failed_updates) > 0:
            print(f"\nFailed updates details:")
            for fail in failed_updates[:10]:  # Show first 10 failures
                if hasattr(fail, 'attributes'):
                    print(f"  ID: {fail.attributes.get(id_field)}")
                else:
                    print(f"  {fail}")
        
        return total_updated
        
    except Exception as e:
        print(f"✗ ArcGIS Online Error: {e}")
        import traceback
        traceback.print_exc()
        raise


def sync_postgres_to_agol(
    pg_config,
    agol_config,
    table_name,
    schema='public',
    id_field='id',
    largeur_field='largeur',
    batch_size=1000
):
    """
    Complete workflow: Read from PostgreSQL and update ArcGIS Online
    
    Parameters:
    -----------
    pg_config : dict
        PostgreSQL connection parameters
    agol_config : dict
        ArcGIS Online connection parameters
    table_name : str
        PostgreSQL table name
    schema : str
        PostgreSQL schema name
    id_field : str
        ID field name
    largeur_field : str
        Largeur field name
    batch_size : int
        Batch size for updates
    """
    
    print("="*60)
    print("PostgreSQL to ArcGIS Online Sync")
    print("="*60)
    
    # Step 1: Get data from PostgreSQL
    print("\nStep 1: Reading data from PostgreSQL...")
    largeur_dict = get_largeur_from_postgres(
        host=pg_config['host'],
        database=pg_config['database'],
        user=pg_config['user'],
        password=pg_config['password'],
        port=pg_config['port'],
        table_name=table_name,
        schema=schema
    )
    
    if not largeur_dict:
        print("⚠ No data retrieved from PostgreSQL. Exiting.")
        return
    
    # Step 2: Update ArcGIS Online
    print("\nStep 2: Updating ArcGIS Online Feature Service...")
    total_updated = update_agol_feature_service(
        feature_service_url=agol_config['feature_service_url'],
        largeur_dict=largeur_dict,
        id_field=id_field,
        largeur_field=largeur_field,
        username=agol_config.get('username'),
        password=agol_config.get('password'),
        portal_url=agol_config.get('portal_url', 'https://www.arcgis.com'),
        batch_size=batch_size
    )
    
    print(f"\n✓ Sync completed! {total_updated} features updated.")


def example():
    """Example usage"""
    
    # PostgreSQL configuration
    PG_CONFIG = {
        'host': 'localhost',
        'database': 'rcp_cd16',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    # ArcGIS Online configuration
    AGOL_CONFIG = {
        'feature_service_url': 'https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD16_V2_Fusion_Sens/FeatureServer/9',
        'username': "roadcare",
        'password': "Antonin&TienSy2021",
        'portal_url': 'https://www.arcgis.com'
    }
    
    # Execute sync
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='zh_u02_l200',
        schema='rendu',
        id_field='id',
        largeur_field='largeur',
        batch_size=1000
    )

if __name__ == "__main__":
    example()

