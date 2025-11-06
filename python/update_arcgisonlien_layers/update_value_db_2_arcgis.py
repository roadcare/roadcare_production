import psycopg2
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from psycopg2.extras import RealDictCursor
import time
import urllib3

# Disable SSL certificate verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_data_from_postgres(host, database, user, password, port, table_name, id_field='id', fields_to_update='*', schema='public'):
    """
    Get id and field values from PostgreSQL table
    
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
    id_field : str
        ID field name for matching (default: 'id')
    fields_to_update : str or list
        Fields to retrieve:
        - '*': all fields (default)
        - 'field_name': single field
        - ['field1', 'field2']: list of fields
    schema : str
        Schema name (default: 'public')
    
    Returns:
    --------
    dict : Dictionary with id as key and dict of field values as value
           Example: {id1: {'field1': value1, 'field2': value2}, id2: {...}}
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
        
        # Determine which fields to select
        if fields_to_update == '*':
            # Get all fields except geometry fields
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}'
                AND column_name != '{id_field}'
                AND data_type NOT IN ('USER-DEFINED')
                ORDER BY ordinal_position
            """)
            field_list = [row['column_name'] for row in cursor.fetchall()]
            fields_str = ', '.join([id_field] + field_list)
        elif isinstance(fields_to_update, list):
            field_list = fields_to_update
            fields_str = ', '.join([id_field] + field_list)
        else:
            # Single field
            field_list = [fields_to_update]
            fields_str = f"{id_field}, {fields_to_update}"
        
        # Query to get id and field values
        query = f"""
            SELECT {fields_str}
            FROM {schema}.{table_name}
        """
        
        print(f"Executing query: {query}")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        print(f"✓ Retrieved {len(rows)} records from PostgreSQL")
        print(f"✓ Fields to update: {field_list if fields_to_update != '*' else 'all fields'}")
        
        # Create dictionary: {id: {field1: value1, field2: value2, ...}}
        data_dict = {}
        for row in rows:
            record_id = row[id_field]
            field_values = {}
            for field in field_list:
                if field in row and row[field] is not None:
                    field_values[field] = row[field]
            if field_values:  # Only add if there are values to update
                data_dict[record_id] = field_values
        
        cursor.close()
        conn.close()
        
        print(f"✓ Prepared {len(data_dict)} records for update")
        return data_dict
        
    except Exception as e:
        print(f"✗ PostgreSQL Error: {e}")
        import traceback
        traceback.print_exc()
        raise


def update_agol_feature_service(
    feature_service_url,
    data_dict,
    id_field='id',
    username=None,
    password=None,
    portal_url='https://www.arcgis.com',
    batch_size=1000
):
    """
    Update ArcGIS Online Feature Service with field values from PostgreSQL
    
    Parameters:
    -----------
    feature_service_url : str
        URL of the feature service layer
    data_dict : dict
        Dictionary with {id: {field1: value1, field2: value2, ...}}
    id_field : str
        Name of the ID field in feature service (default: 'id')
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
        
        # Get all field names from the first record to know which fields to query
        if data_dict:
            first_record = next(iter(data_dict.values()))
            fields_to_query = [id_field] + list(first_record.keys())
        else:
            fields_to_query = [id_field]
        
        # Query all features
        print(f"Querying features...")
        feature_set = feature_layer.query(
            where='1=1', 
            out_fields=','.join(fields_to_query), 
            return_geometry=False
        )
        
        features = feature_set.features
        print(f"✓ Retrieved {len(features)} features from ArcGIS Online")
        
        # Prepare updates
        updates = []
        matched_count = 0
        not_found_count = 0
        
        for feature in features:
            feature_id = feature.attributes.get(id_field)
            
            if feature_id in data_dict:
                # Update all fields from the dictionary
                for field_name, field_value in data_dict[feature_id].items():
                    feature.attributes[field_name] = field_value
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
            return 0
        
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
    schema='rendu',
    id_field='id',
    fields_to_update='*',
    batch_size=1000
):
    """
    Complete workflow: Read from PostgreSQL and update ArcGIS Online
    
    Parameters:
    -----------
    pg_config : dict
        PostgreSQL connection parameters (host, database, user, password, port)
    agol_config : dict
        ArcGIS Online connection parameters (feature_service_url, username, password, portal_url)
    table_name : str
        PostgreSQL table name
    schema : str
        PostgreSQL schema name (default: 'rendu')
    id_field : str
        ID field name for matching records (default: 'id')
    fields_to_update : str or list
        Fields to update (default: '*' for all fields)
        - '*': update all fields
        - 'field_name': update single field
        - ['field1', 'field2']: update multiple specific fields
    batch_size : int
        Batch size for updates (default: 1000)
    
    Returns:
    --------
    int : Number of successfully updated features
    """
    
    print("="*60)
    print("PostgreSQL to ArcGIS Online Sync")
    print("="*60)
    print(f"Table: {schema}.{table_name}")
    print(f"ID Field: {id_field}")
    print(f"Fields to update: {fields_to_update}")
    print("="*60)
    
    # Step 1: Get data from PostgreSQL
    print("\nStep 1: Reading data from PostgreSQL...")
    data_dict = get_data_from_postgres(
        host=pg_config['host'],
        database=pg_config['database'],
        user=pg_config['user'],
        password=pg_config['password'],
        port=pg_config['port'],
        table_name=table_name,
        id_field=id_field,
        fields_to_update=fields_to_update,
        schema=schema
    )
    
    if not data_dict:
        print("⚠ No data retrieved from PostgreSQL. Exiting.")
        return 0
    
    # Step 2: Update ArcGIS Online
    print("\nStep 2: Updating ArcGIS Online Feature Service...")
    total_updated = update_agol_feature_service(
        feature_service_url=agol_config['feature_service_url'],
        data_dict=data_dict,
        id_field=id_field,
        username=agol_config.get('username'),
        password=agol_config.get('password'),
        portal_url=agol_config.get('portal_url', 'https://www.arcgis.com'),
        batch_size=batch_size
    )
    
    print(f"\n✓ Sync completed! {total_updated} features updated.")
    return total_updated


def example_usage():
    """Example usage scenarios"""
    
    # PostgreSQL configuration
    PG_CONFIG = {
        'host': 'localhost',
        'database': 'cd12_demo',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    # ArcGIS Online configuration
    AGOL_CONFIG = {
        'feature_service_url': 'https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer/2',
        'username': "roadcare",
        'password': "Antonin&TienSy2021",
        'portal_url': 'https://www.arcgis.com'
    }
    
    # Example 1: Update all fields
    print("\n" + "="*60)
    print("EXAMPLE 1: Update ALL fields")
    print("="*60)
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='zh_u02_l200',
        schema='rendu',
        id_field='id',
        fields_to_update='*',  # Update all fields
        batch_size=1000
    )
    
    # Example 2: Update single field
    print("\n" + "="*60)
    print("EXAMPLE 2: Update SINGLE field")
    print("="*60)
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='zh_u02_l200',
        schema='rendu',
        id_field='id',
        fields_to_update='note_classe',  # Update only this field
        batch_size=1000
    )
    
    # Example 3: Update multiple specific fields
    print("\n" + "="*60)
    print("EXAMPLE 3: Update MULTIPLE specific fields")
    print("="*60)
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='zh_u02_l200',
        schema='rendu',
        id_field='id',
        fields_to_update=['note_classe', 'largeur', 'other_field'],  # Update these fields
        batch_size=1000
    )
    
    # Example 4: Use different ID field
    print("\n" + "="*60)
    print("EXAMPLE 4: Use different ID field")
    print("="*60)
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='another_table',
        schema='rendu',
        id_field='custom_id',  # Different ID field name
        fields_to_update='*',
        batch_size=1000
    )


if __name__ == "__main__":
    # Run a simple example - update single field
    PG_CONFIG = {
        'host': 'localhost',
        'database': 'cd12_demo',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    AGOL_CONFIG = {
        'feature_service_url': 'https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer/2',
        'username': "roadcare",
        'password': "Antonin&TienSy2021",
        'portal_url': 'https://www.arcgis.com'
    }
    
    # Default: update single field (backward compatible)
    sync_postgres_to_agol(
        pg_config=PG_CONFIG,
        agol_config=AGOL_CONFIG,
        table_name='zh_u02_l200',
        schema='rendu',
        id_field='id',
        fields_to_update='note_classe',
        batch_size=1000
    )
