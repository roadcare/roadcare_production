from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import pandas as pd
import re
import urllib3
import json
import os
from datetime import datetime

# Disable SSL warnings (if you're having certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# CONFIGURATION - MODIFY THESE PARAMETERS
# ============================================================================

# ArcGIS Online credentials
ARCGIS_USERNAME = "roadcare"
ARCGIS_PASSWORD = "Antonin&TienSy2021"

# Feature layer URL
FEATURE_LAYER_URL = "https://services-eu1.arcgis.com/PB4bGIQ2JEvZVdru/arcgis/rest/services/Carte_CD16_V1/FeatureServer/1"

# Export folder - change this to your desired location
# Examples: "export_arcgis", "C:/exports", "../data/exports"
EXPORT_FOLDER = "export_arcgis"

# New SAS token to replace all old ones
NEW_SAS = "?sv=2023-01-03&st=2025-02-13T08%3A27%3A49Z&se=2028-02-14T08%3A27%3A00Z&sr=c&sp=r&sig=6STZ6XA8DiGkBLg5Z4xfmtQ3zyak0HJEqyNnSPJCjmQ%3D"

# ============================================================================

# Regex pattern to match SAS tokens (starting with ?sv= and continuing with SAS parameters)
SAS_PATTERN = re.compile(r'\?sv=[^?\s]*')


def export_layer():
    """
    Step 1: Export the feature layer to local files
    """
    print("="*70)
    print("STEP 1: EXPORTING LAYER")
    print("="*70)
    
    try:
        # Create export folder if it doesn't exist
        if not os.path.exists(EXPORT_FOLDER):
            os.makedirs(EXPORT_FOLDER)
            print(f"✓ Created export folder: {EXPORT_FOLDER}")
        else:
            print(f"✓ Using export folder: {EXPORT_FOLDER}")
        
        # Connect to ArcGIS Online
        print("\nConnecting to ArcGIS Online...")
        gis = GIS("https://www.arcgis.com", ARCGIS_USERNAME, ARCGIS_PASSWORD, verify_cert=False)
        print(f"✓ Connected as: {gis.properties.user.username}")
        
        # Access the feature layer
        print("\nAccessing feature layer...")
        feature_layer = FeatureLayer(FEATURE_LAYER_URL, gis)
        layer_name = feature_layer.properties.name
        print(f"✓ Layer: {layer_name}")
        
        # Query all features
        print("\nQuerying all features...")
        feature_set = feature_layer.query(where="1=1", return_all_records=True)
        features = feature_set.features
        print(f"✓ Found {len(features)} features")
        
        # Convert to DataFrame
        print("\nConverting to DataFrame...")
        df = feature_set.sdf  # Spatial DataFrame
        print(f"✓ DataFrame created with {len(df)} rows and {len(df.columns)} columns")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Save to multiple formats
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as CSV (without geometry)
        csv_file = os.path.join(EXPORT_FOLDER, f"export_{layer_name}_{timestamp}.csv")
        df_export = df.copy()
        if 'SHAPE' in df_export.columns:
            df_export = df_export.drop('SHAPE', axis=1)
        df_export.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ Exported to CSV: {csv_file}")
        
        # Save as JSON (raw features with geometry)
        json_file = os.path.join(EXPORT_FOLDER, f"export_{layer_name}_{timestamp}.json")
        features_json = {
            "features": [f.as_dict for f in features],
            "spatialReference": feature_set.spatial_reference,
            "geometryType": feature_set.geometry_type
        }
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(features_json, f, indent=2, ensure_ascii=False)
        print(f"✓ Exported to JSON: {json_file}")
        
        # Save schema info
        schema_file = os.path.join(EXPORT_FOLDER, f"schema_{layer_name}_{timestamp}.txt")
        with open(schema_file, 'w', encoding='utf-8') as f:
            f.write("LAYER SCHEMA\n")
            f.write("="*70 + "\n\n")
            f.write(f"Layer Name: {layer_name}\n")
            f.write(f"Layer URL: {FEATURE_LAYER_URL}\n")
            f.write(f"Feature Count: {len(df)}\n\n")
            f.write("FIELDS:\n")
            for field in feature_layer.properties.fields:
                f.write(f"  - {field['name']} ({field['type']})\n")
        print(f"✓ Schema saved to: {schema_file}")
        
        # Return both features (with geometry) and dataframe
        return features, df, csv_file, json_file, gis, feature_layer, layer_name
        
    except Exception as e:
        print(f"\n✗ Export failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None, None, None, None, None


def edit_data_locally(features, df, csv_file, layer_name):
    """
    Step 2: Edit the data locally - replace SAS tokens
    Preserves geometry and OBJECTID for updates
    """
    print("\n" + "="*70)
    print("STEP 2: EDITING DATA LOCALLY")
    print("="*70)
    
    try:
        # Check if filename field exists
        if 'filename' not in df.columns:
            print("\n✗ 'filename' column not found in the data!")
            print(f"Available columns: {', '.join(df.columns.tolist())}")
            return None, None
        
        # Find and replace SAS tokens in the features
        print("\nSearching for SAS tokens in 'filename' field...")
        old_sas_tokens = set()
        modified_features = []
        update_count = 0
        
        for feature in features:
            attrs = feature.attributes
            feature_dict = feature.as_dict.copy()
            
            if 'filename' in attrs and attrs['filename']:
                filename = str(attrs['filename'])
                match = SAS_PATTERN.search(filename)
                
                if match:
                    old_sas = match.group(0)
                    old_sas_tokens.add(old_sas)
                    
                    # Replace with new SAS token
                    new_filename = SAS_PATTERN.sub(NEW_SAS, filename)
                    
                    # Update the feature (preserving OBJECTID and geometry)
                    feature_dict['attributes']['filename'] = new_filename
                    modified_features.append(feature_dict)
                    
                    update_count += 1
                    
                    if update_count <= 3:
                        print(f"\nExample {update_count}:")
                        print(f"  OBJECTID: {attrs.get('OBJECTID', 'N/A')}")
                        print(f"  Old: ...{old_sas[:60]}...")
                        print(f"  New: ...{NEW_SAS[:60]}...")
        
        # Summary
        print(f"\n{'='*70}")
        if len(old_sas_tokens) > 0:
            print(f"Found {len(old_sas_tokens)} different old SAS token(s):")
            for i, token in enumerate(old_sas_tokens, 1):
                print(f"  {i}. {token[:70]}...")
        print(f"{'='*70}")
        print(f"\n✓ {update_count} records will be updated (preserving geometry and OBJECTID)")
        
        if update_count == 0:
            print("\n⚠ No SAS tokens found to update!")
            return None, None
        
        # Save modified data (JSON with geometry)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(EXPORT_FOLDER, f"modified_{layer_name}_{timestamp}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({"features": modified_features}, f, indent=2, ensure_ascii=False)
        print(f"✓ Modified data saved to: {output_file}")
        
        # Also save as CSV for review
        csv_output = os.path.join(EXPORT_FOLDER, f"modified_{layer_name}_{timestamp}.csv")
        df_modified = df.copy()
        
        # Update the dataframe with new filenames
        for mod_feature in modified_features:
            mod_attrs = mod_feature['attributes']
            objectid = mod_attrs.get('OBJECTID')
            if objectid and 'filename' in mod_attrs:
                # Find matching row in dataframe
                mask = df_modified['OBJECTID'] == objectid
                if mask.any():
                    df_modified.loc[mask, 'filename'] = mod_attrs['filename']
        
        df_export = df_modified.copy()
        if 'SHAPE' in df_export.columns:
            df_export = df_export.drop('SHAPE', axis=1)
        df_export.to_csv(csv_output, index=False, encoding='utf-8-sig')
        print(f"✓ CSV review file saved to: {csv_output}")
        
        return modified_features, output_file
        
    except Exception as e:
        print(f"\n✗ Edit failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None


def replace_layer_data(gis, feature_layer, modified_features):
    """
    Step 3: Update the data in the feature layer
    Uses UPDATE with smaller batches and retry logic to avoid SQL transaction errors
    """
    print("\n" + "="*70)
    print("STEP 3: UPDATING LAYER DATA")
    print("="*70)
    
    try:
        # Check if layer supports updates
        print("\nChecking layer capabilities...")
        capabilities = feature_layer.properties.get('capabilities', '')
        print(f"Layer capabilities: {capabilities}")
        
        if 'Update' not in capabilities and 'Edit' not in capabilities:
            print("✗ This layer does not support updates!")
            return False
        
        # Confirm before updating
        print(f"\n⚠ WARNING: This will update features in the layer!")
        print(f"  Layer: {feature_layer.properties.name}")
        print(f"  URL: {FEATURE_LAYER_URL}")
        print(f"  Records to update: {len(modified_features)}")
        
        # Test with one feature first
        print("\nTesting update with 1 feature first...")
        test_feature = [modified_features[0]]
        test_result = feature_layer.edit_features(updates=test_feature)
        
        if test_result.get('updateResults'):
            if test_result['updateResults'][0].get('success'):
                print("✓ Test update successful!")
            else:
                error = test_result['updateResults'][0].get('error', 'Unknown error')
                print(f"✗ Test update failed: {error}")
                return False
        else:
            print("✗ Test update returned no results")
            return False
        
        response = input("\nTest successful! Do you want to proceed with all updates? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("✗ Operation cancelled.")
            return False
        
        # Update features in small batches with retry logic
        print("\nUpdating features in small batches...")
        batch_size = 100  # Smaller batch size to avoid transaction errors
        total_updated = 1  # Already updated the test feature
        max_retries = 3
        
        # Skip the first feature since we already updated it
        remaining_features = modified_features[1:]
        
        for i in range(0, len(remaining_features), batch_size):
            batch = remaining_features[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    result = feature_layer.edit_features(updates=batch)
                    
                    if result.get('updateResults'):
                        success_count = sum(1 for r in result['updateResults'] if r.get('success'))
                        total_updated += success_count
                        
                        if success_count > 0:
                            print(f"  Batch {batch_num}: Updated {success_count}/{len(batch)} features")
                            success = True
                        
                        # Print any errors
                        errors_found = False
                        for r in result['updateResults']:
                            if not r.get('success'):
                                if not errors_found:
                                    print("    Errors:")
                                    errors_found = True
                                error_msg = r.get('error', {})
                                if isinstance(error_msg, dict):
                                    print(f"      OBJECTID {r.get('objectId', 'N/A')}: {error_msg.get('description', 'Unknown error')}")
                                else:
                                    print(f"      OBJECTID {r.get('objectId', 'N/A')}: {error_msg}")
                        
                        if not success and retry_count < max_retries - 1:
                            retry_count += 1
                            print(f"    Retrying batch {batch_num} (attempt {retry_count + 1}/{max_retries})...")
                            import time
                            time.sleep(2)  # Wait 2 seconds before retry
                    else:
                        retry_count += 1
                        if retry_count < max_retries:
                            print(f"  Batch {batch_num}: No results, retrying (attempt {retry_count + 1}/{max_retries})...")
                            import time
                            time.sleep(2)
                        
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"  Batch {batch_num}: Error - {str(e)}, retrying (attempt {retry_count + 1}/{max_retries})...")
                        import time
                        time.sleep(2)
                    else:
                        print(f"  Batch {batch_num}: Failed after {max_retries} attempts - {str(e)}")
            
            if not success:
                print(f"  ⚠ Batch {batch_num} failed after all retries")
        
        print(f"\n✓ Successfully updated {total_updated} out of {len(modified_features)} features!")
        
        if total_updated < len(modified_features):
            print(f"⚠ Warning: Only {total_updated} out of {len(modified_features)} features were updated")
            print(f"   You may want to run the script again to retry failed updates")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Update failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main workflow: Export → Edit → Update
    """
    print("="*70)
    print("ArcGIS ONLINE - EXPORT, EDIT, UPDATE WORKFLOW")
    print("="*70)
    print(f"Export folder: {os.path.abspath(EXPORT_FOLDER)}\n")
    
    # Step 1: Export
    features, df, csv_file, json_file, gis, feature_layer, layer_name = export_layer()
    if features is None:
        return
    
    # Step 2: Edit locally (preserving geometry and OBJECTID)
    result = edit_data_locally(features, df, csv_file, layer_name)
    if result is None or result[0] is None:
        return
    modified_features, output_file = result
    
    # Step 3: Update (not replace - uses UPDATE operation)
    success = replace_layer_data(gis, feature_layer, modified_features)
    
    if success:
        print("\n" + "="*70)
        print("✓ WORKFLOW COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"\nNew SAS token expires: 2028-02-14")
        print(f"Geometry and OBJECTID preserved for all features")
        print(f"Only features with SAS tokens were updated")
        print(f"\nAll files saved to: {os.path.abspath(EXPORT_FOLDER)}")
    else:
        print("\n" + "="*70)
        print("✗ WORKFLOW FAILED")
        print("="*70)


if __name__ == "__main__":
    main()