from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import json
from datetime import datetime
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle special types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

def export_agol_to_geojson(
    feature_service_url,
    output_file,
    username=None,
    password=None,
    portal_url='https://www.arcgis.com',
    where_clause='1=1',
    max_records=None
):
    """
    Export ArcGIS Online Feature Service layer to GeoJSON
    
    Parameters:
    -----------
    feature_service_url : str
        URL of the feature service layer
        Example: 'https://services.arcgis.com/xxx/arcgis/rest/services/MyService/FeatureServer/0'
    output_file : str
        Output GeoJSON file path
    username : str, optional
        ArcGIS Online username (if layer is private)
    password : str, optional
        ArcGIS Online password (if layer is private)
    portal_url : str
        Portal URL (default: 'https://www.arcgis.com')
    where_clause : str
        SQL where clause to filter features (default: '1=1' for all features)
    max_records : int, optional
        Maximum number of records to export (None = all records)
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
        print(f"  Geometry Type: {layer_props.get('geometryType', 'Unknown')}")
        
        # Query features
        print(f"Querying features (where: {where_clause})...")
        
        # Query all features with geometry
        feature_set = feature_layer.query(
            where=where_clause,
            out_fields='*',
            return_geometry=True,
            return_all_records=(max_records is None),
            result_record_count=max_records
        )
        
        features = feature_set.features
        print(f"✓ Retrieved {len(features)} features")
        
        if len(features) == 0:
            print("⚠ No features found. Creating empty GeoJSON.")
        
        # Convert to GeoJSON
        geojson_features = []
        
        for feature in features:
            # Get geometry
            geom = feature.geometry
            
            # Get attributes
            attributes = feature.attributes
            
            # Convert attributes to serializable format
            properties = {}
            for key, value in attributes.items():
                if isinstance(value, Decimal):
                    properties[key] = float(value)
                elif isinstance(value, datetime):
                    properties[key] = value.isoformat()
                elif value is None:
                    properties[key] = None
                else:
                    properties[key] = value
            
            # Build GeoJSON feature
            geojson_feature = {
                "type": "Feature",
                "geometry": geom if geom else None,
                "properties": properties
            }
            
            geojson_features.append(geojson_feature)
        
        # Create GeoJSON FeatureCollection
        geojson = {
            "type": "FeatureCollection",
            "features": geojson_features
        }
        
        # Write to file
        print(f"Writing to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
        
        print(f"✓ Successfully exported {len(geojson_features)} features to {output_file}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def export_from_item_id(
    item_id,
    layer_index,
    output_file,
    username=None,
    password=None,
    portal_url='https://www.arcgis.com',
    where_clause='1=1'
):
    """
    Export ArcGIS Online layer using Item ID
    
    Parameters:
    -----------
    item_id : str
        Item ID of the feature service
    layer_index : int
        Index of the layer (usually 0)
    output_file : str
        Output GeoJSON file path
    username : str, optional
        ArcGIS Online username
    password : str, optional
        ArcGIS Online password
    portal_url : str
        Portal URL
    where_clause : str
        SQL where clause to filter features
    """
    
    try:
        # Connect to ArcGIS Online
        if username and password:
            gis = GIS(portal_url, username, password)
        else:
            gis = GIS(portal_url)
        
        print(f"✓ Connected to {portal_url}")
        
        # Get the item
        item = gis.content.get(item_id)
        print(f"✓ Found item: {item.title}")
        
        # Get the feature layer
        feature_layer = item.layers[layer_index]
        print(f"✓ Accessing layer {layer_index}: {feature_layer.properties.name}")
        
        # Query features
        print(f"Querying features...")
        feature_set = feature_layer.query(
            where=where_clause,
            out_fields='*',
            return_geometry=True,
            return_all_records=True
        )
        
        features = feature_set.features
        print(f"✓ Retrieved {len(features)} features")
        
        # Convert to GeoJSON
        geojson_features = []
        for feature in features:
            geojson_feature = {
                "type": "Feature",
                "geometry": feature.geometry if feature.geometry else None,
                "properties": feature.attributes
            }
            geojson_features.append(geojson_feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": geojson_features
        }
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
        
        print(f"✓ Successfully exported to {output_file}")
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def example():
    """Example usage"""
    
    # Example 1: Export public feature service using direct URL
    print("=" * 60)
    print("Example 1: Export using Feature Service URL")
    print("=" * 60)
    
    feature_service_url = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD16_Largeur/FeatureServer/3"
    
    export_agol_to_geojson(
        feature_service_url=feature_service_url,
        output_file='G:/01_Affaires/06-AFFAIRES_ROADCARE/40_CD16_CHARENTE/04-Traitement/05_Export/GEOJSON/20251023_cd16_troncon_largeur.geojson'        
    )
    
    
    # Example 2: Export private feature service with authentication
    print("\n" + "=" * 60)
    print("Example 2: Export private layer (requires authentication)")
    print("=" * 60)
    
    # Uncomment and fill in your credentials
    """
    export_agol_to_geojson(
        feature_service_url='https://services.arcgis.com/YOUR_ORG/arcgis/rest/services/YourService/FeatureServer/0',
        output_file='private_layer.geojson',
        username='your_username',
        password='your_password',
        where_clause='1=1'
    )
    """
    
    # Example 3: Export using Item ID
    print("\n" + "=" * 60)
    print("Example 3: Export using Item ID")
    print("=" * 60)
    
    # Uncomment to use
    """
    export_from_item_id(
        item_id='a1b2c3d4e5f6g7h8i9j0',
        layer_index=0,
        output_file='layer_from_item.geojson',
        username='your_username',
        password='your_password'
    )
    """
    
    # Example 4: Export with specific fields and filters
    print("\n" + "=" * 60)
    print("Example 4: Custom export with filters")
    print("=" * 60)
    
    # This would export only features where population > 100000
    """
    export_agol_to_geojson(
        feature_service_url='YOUR_FEATURE_SERVICE_URL',
        output_file='filtered_export.geojson',
        where_clause='POP2010 > 100000',
        max_records=None  # All matching records
    )
    """


if __name__ == "__main__":
    example()