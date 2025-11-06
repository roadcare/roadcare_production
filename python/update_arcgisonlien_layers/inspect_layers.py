"""
ArcGIS Online Layer Field Inspector
Use this script to check what fields exist in your layers
"""

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import warnings
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Configuration
USERNAME = "roadcare"
PASSWORD = "Antonin&TienSy2021"
PORTAL_URL = "https://www.arcgis.com"

# Layer URLs
LAYER_1_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer/0"
LAYER_2_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/CD12_Demo/FeatureServer/2"


def inspect_layer(layer_url, layer_name):
    """Inspect and display all fields in a layer"""
    print(f"\n{'='*80}")
    print(f"Layer: {layer_name}")
    print(f"URL: {layer_url}")
    print(f"{'='*80}")
    
    try:
        layer = FeatureLayer(layer_url)
        props = layer.properties
        
        print(f"\nLayer Name: {props.name}")
        print(f"Layer Type: {props.type}")
        
        print(f"\n{'Field Name':<30} {'Type':<20} {'Editable':<10} {'Nullable'}")
        print(f"{'-'*30} {'-'*20} {'-'*10} {'-'*10}")
        
        for field in props.fields:
            field_name = field.name
            field_type = field.type
            editable = 'Yes' if field.editable else 'No'
            nullable = 'Yes' if field.nullable else 'No'
            
            print(f"{field_name:<30} {field_type:<20} {editable:<10} {nullable}")
        
        # Try to get a sample feature
        print(f"\nSample Data (first feature):")
        print(f"{'-'*80}")
        try:
            sample = layer.query(where="1=1", out_fields="*", return_geometry=False, result_record_count=1)
            if sample.features:
                attrs = sample.features[0].attributes
                for key, value in attrs.items():
                    print(f"  {key}: {value}")
            else:
                print("  No features found in layer")
        except Exception as e:
            print(f"  Could not retrieve sample data: {e}")
            
    except Exception as e:
        print(f"\n✗ Error inspecting layer: {e}")


def main():
    print("\n" + "="*80)
    print("ArcGIS Online Layer Field Inspector")
    print("="*80)
    
    try:
        # Authenticate
        print(f"\nAuthenticating to {PORTAL_URL}...")
        gis = GIS(PORTAL_URL, USERNAME, PASSWORD, verify_cert=False)
        print(f"✓ Successfully authenticated as: {gis.properties.user.username}")
        
        # Inspect both layers
        inspect_layer(LAYER_1_URL, "Layer 0 - image_note")
        inspect_layer(LAYER_2_URL, "Layer 2 - zh_u02_l200")
        
        print("\n" + "="*80)
        print("Inspection complete!")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Authentication error: {e}")


if __name__ == "__main__":
    main()
