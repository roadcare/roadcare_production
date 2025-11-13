"""
Example scripts for adding different layers from roadare_sig_prod database
to ArcGIS Online Feature Service
"""

from add_layer_to_arcgis import PostgresToArcGIS

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


def example_1_add_degradation():
    """
    Example 1: Add rendu.degradation table
    Contains: degradation with MultiPolygon geometry
    """
    print("\n" + "="*60)
    print("Example 1: Adding rendu.degradation layer")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Degradation",
            schema_name="rendu",
            table_name="degradation",
            limit=None,  # Transfer all degradations
            where_clause=None
        )
    finally:
        pg_to_arcgis.close_connections()


def example_2_add_troncon_client():
    """
    Example 2: Add client.troncon_client table
    Contains: Road segments with LineStringM geometry (with linear referencing)
    """
    print("\n" + "="*60)
    print("Example 2: Adding client.troncon_client layer")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Troncons_Client",
            schema_name="client",
            table_name="troncon_client",
            limit=5000,  # Limit to 5000 segments
            where_clause=None
        )
    finally:
        pg_to_arcgis.close_connections()


def example_3_add_images_active():
    """
    Example 3: Add public.image table (only active/non-obsolete images)
    Contains: Image points with PointM geometry
    """
    print("\n" + "="*60)
    print("Example 3: Adding active images from public.image")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Images_Active",
            schema_name="public",
            table_name="image",
            limit=10000,  # Limit to 10000 images
            where_clause="is_obsolete = false OR is_obsolete IS NULL"  # Only active images
        )
    finally:
        pg_to_arcgis.close_connections()


def example_4_add_images_by_axe():
    """
    Example 4: Add images from a specific road axis (axe)
    Demonstrates filtering by specific road
    """
    print("\n" + "="*60)
    print("Example 4: Adding images from specific axe")
    print("="*60 + "\n")
    
    # First, let's check what axes are available
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        # Example: Filter by a specific axe (change 'D999' to your actual axe value)
        target_axe = 'D999'  # CHANGE THIS TO YOUR ACTUAL AXE VALUE
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name=f"Images_Axe_{target_axe}",
            schema_name="public",
            table_name="image",
            limit=None,
            where_clause=f"axe = '{target_axe}' AND is_obsolete = false"
        )
    finally:
        pg_to_arcgis.close_connections()


def example_5_add_sessions():
    """
    Example 5: Add public.session table
    Contains: Acquisition sessions with LineStringM geometry
    """
    print("\n" + "="*60)
    print("Example 5: Adding acquisition sessions")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Acquisition_Sessions",
            schema_name="public",
            table_name="session",
            limit=None,
            where_clause="state IS NOT NULL"  # Only sessions with defined state
        )
    finally:
        pg_to_arcgis.close_connections()


def example_6_add_recent_images():
    """
    Example 6: Add only recent images (from last 30 days)
    Demonstrates date filtering
    """
    print("\n" + "="*60)
    print("Example 6: Adding recent images (last 30 days)")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Images_Recent_30days",
            schema_name="public",
            table_name="image",
            limit=None,
            where_clause="\"captureDate\" >= CURRENT_DATE - INTERVAL '30 days'"
        )
    finally:
        pg_to_arcgis.close_connections()


def example_7_add_high_quality_images():
    """
    Example 7: Add only high-quality matched images
    Demonstrates filtering by map-matching quality
    """
    print("\n" + "="*60)
    print("Example 7: Adding high-quality matched images")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name="Images_High_Quality",
            schema_name="public",
            table_name="image",
            limit=None,
            where_clause="prj_quality IS NOT NULL AND prj_quality < 5.0"  # Quality threshold < 5m
        )
    finally:
        pg_to_arcgis.close_connections()


def example_8_add_specific_session_images():
    """
    Example 8: Add images from a specific acquisition session
    """
    print("\n" + "="*60)
    print("Example 8: Adding images from specific session")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        # Example session ID (change to your actual session ID)
        session_id = 'your-session-uuid-here'  # CHANGE THIS
        
        pg_to_arcgis.add_layer_to_feature_service(
            feature_service_url=FEATURE_SERVICE_URL,
            layer_name=f"Images_Session_{session_id[:8]}",
            schema_name="public",
            table_name="image",
            limit=None,
            where_clause=f"session_id = '{session_id}'"
        )
    finally:
        pg_to_arcgis.close_connections()


def example_9_batch_add_multiple_layers():
    """
    Example 9: Add multiple layers in one script execution
    """
    print("\n" + "="*60)
    print("Example 9: Adding multiple layers")
    print("="*60 + "\n")
    
    pg_to_arcgis = PostgresToArcGIS(PG_CONFIG, ARCGIS_USERNAME, ARCGIS_PASSWORD)
    
    try:
        # Connect once
        pg_to_arcgis.connect_postgres()
        pg_to_arcgis.connect_arcgis()
        
        # Add multiple layers
        layers_to_add = [
            {
                "layer_name": "Routes_All",
                "schema_name": "client",
                "table_name": "route_client",
                "limit": None,
                "where_clause": None
            },
            {
                "layer_name": "Segments_All",
                "schema_name": "client",
                "table_name": "troncon_client",
                "limit": None,
                "where_clause": None
            },
            {
                "layer_name": "Images_Current",
                "schema_name": "public",
                "table_name": "image",
                "limit": 5000,
                "where_clause": "is_obsolete = false"
            }
        ]
        
        for layer_config in layers_to_add:
            print(f"\nAdding layer: {layer_config['layer_name']}...")
            pg_to_arcgis.add_layer_to_feature_service(
                feature_service_url=FEATURE_SERVICE_URL,
                **layer_config
            )
            
    finally:
        pg_to_arcgis.close_connections()


def list_available_examples():
    """List all available examples"""
    print("\n" + "="*60)
    print("Available Examples:")
    print("="*60)
    print("\n1. Add client.route_client (all routes)")
    print("2. Add client.troncon_client (road segments)")
    print("3. Add active images (non-obsolete)")
    print("4. Add images from specific road axis")
    print("5. Add acquisition sessions")
    print("6. Add recent images (last 30 days)")
    print("7. Add high-quality matched images")
    print("8. Add images from specific session")
    print("9. Batch add multiple layers")
    print("\nTo run an example, call the corresponding function:")
    print("  example_1_add_route_client()")
    print("  example_2_add_troncon_client()")
    print("  etc.")


if __name__ == "__main__":
    # Show available examples
    #list_available_examples()
    
    # Uncomment the example you want to run:
    
    example_1_add_degradation()
    # example_2_add_troncon_client()
    # example_3_add_images_active()
    # example_4_add_images_by_axe()
    # example_5_add_sessions()
    # example_6_add_recent_images()
    # example_7_add_high_quality_images()
    # example_8_add_specific_session_images()
    # example_9_batch_add_multiple_layers()
