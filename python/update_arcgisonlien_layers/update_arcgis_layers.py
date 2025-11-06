"""
ArcGIS Online Feature Layer Update Script
Updates note_classe field based on numeric rating values
"""

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import sys
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


def classify_note(note_value):
    """
    Classify a numeric note value into a text category
    
    Args:
        note_value: Numeric value to classify
        
    Returns:
        Classification string or None
    """
    if note_value is None:
        return None
    
    try:
        note = float(note_value)
        
        if note >= 0.8:
            return '1-Bon'
        elif note >= 0.6:
            return '2-Moyen+'
        elif note >= 0.4:
            return '3-Moyen-'
        else:  # note < 0.4
            return '4-Mauvais'
    except (ValueError, TypeError):
        return None


def update_layer(layer_url, source_field, target_field, layer_name):
    """
    Update a feature layer's classification field based on numeric values
    
    Args:
        layer_url: URL of the feature layer
        source_field: Field containing numeric values
        target_field: Field to update with classification
        layer_name: Name for logging purposes
    """
    print(f"\n{'='*60}")
    print(f"Processing layer: {layer_name}")
    print(f"{'='*60}")
    
    try:
        # Connect to the layer
        layer = FeatureLayer(layer_url)
        print(f"✓ Connected to layer: {layer_url}")
        
        # Get layer properties to check available fields
        print(f"Inspecting layer fields...")
        layer_props = layer.properties
        available_fields = [f.name for f in layer_props.fields]
        
        print(f"Available fields in layer:")
        for field in available_fields:
            print(f"  - {field}")
        
        # Check if required fields exist
        if source_field not in available_fields:
            print(f"\n✗ ERROR: Source field '{source_field}' not found in layer!")
            print(f"Please check the field name (case-sensitive)")
            return
        
        if target_field not in available_fields:
            print(f"\n✗ ERROR: Target field '{target_field}' not found in layer!")
            print(f"Please check the field name (case-sensitive)")
            return
        
        print(f"✓ Required fields found: {source_field}, {target_field}")
        
        # Query all features
        print(f"\nQuerying features...")
        feature_set = layer.query(where="1=1", out_fields=f"{source_field},{target_field}", return_geometry=False)
        features = feature_set.features
        
        print(f"✓ Found {len(features)} features")
        
        if len(features) == 0:
            print("No features to update")
            return
        
        # Find the object ID field name
        object_id_field = None
        for field in layer_props.fields:
            if field.type == 'esriFieldTypeOID':
                object_id_field = field.name
                break
        
        if not object_id_field:
            print("Warning: Could not find Object ID field, using 'OBJECTID'")
            object_id_field = 'OBJECTID'
        
        print(f"✓ Using '{object_id_field}' as Object ID field")
        
        # Prepare updates
        updates = []
        update_count = 0
        skip_count = 0
        
        for feature in features:
            attrs = feature.attributes
            object_id = attrs.get(object_id_field)
            source_value = attrs.get(source_field)
            current_class = attrs.get(target_field)
            
            # Calculate new classification
            new_class = classify_note(source_value)
            
            # Only update if value has changed
            if new_class is not None and new_class != current_class:
                updates.append({
                    'attributes': {
                        object_id_field: object_id,
                        target_field: new_class
                    }
                })
                update_count += 1
            else:
                skip_count += 1
        
        print(f"\nSummary:")
        print(f"  - Features to update: {update_count}")
        print(f"  - Features to skip: {skip_count}")
        
        # Apply updates in batches
        if updates:
            print(f"\nApplying updates...")
            batch_size = 100
            total_updated = 0
            
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                result = layer.edit_features(updates=batch)
                
                if result.get('updateResults'):
                    success = sum(1 for r in result['updateResults'] if r.get('success'))
                    total_updated += success
                    print(f"  Batch {i//batch_size + 1}: {success}/{len(batch)} updated")
            
            print(f"\n✓ Successfully updated {total_updated} features in {layer_name}")
        else:
            print(f"\n✓ No updates needed for {layer_name}")
            
    except Exception as e:
        print(f"\n✗ Error processing {layer_name}: {str(e)}")
        raise


def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("ArcGIS Online Feature Layer Update Tool")
    print("="*60)
    
    try:
        # Authenticate
        print(f"\nAuthenticating to {PORTAL_URL}...")
        gis = GIS(PORTAL_URL, USERNAME, PASSWORD, verify_cert=False)
        print(f"✓ Successfully authenticated as: {gis.properties.user.username}")
        
        # Update Layer 1: image_note (note_globale -> note_classe)
        update_layer(
            layer_url=LAYER_1_URL,
            source_field="note_globale",
            target_field="note_classe",
            layer_name="image_note (Layer 0)"
        )
        
        # Update Layer 2: zh_u02_l200 (note_num -> note_classe)
        update_layer(
            layer_url=LAYER_2_URL,
            source_field="note_num",
            target_field="note_classe",
            layer_name="zh_u02_l200 (Layer 2)"
        )
        
        print("\n" + "="*60)
        print("✓ All layers updated successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
