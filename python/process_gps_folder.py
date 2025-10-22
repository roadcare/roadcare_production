import pandas as pd
import math
import shutil
import sys
from pathlib import Path

def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in meters between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in meters
    r = 6371000
    
    return c * r

def process_single_gps_file(input_file, output_file):
    """
    Process a single GPS CSV file WITHOUT sorting:
    - Check if first line timestamp > second line timestamp, if so, correct it
    - Calculate Eslapted Time (elapsed time in milliseconds from start)
    - Calculate Eslapted Distance (cumulative distance in meters from start)
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    # Read the CSV file
    df = pd.read_csv(input_file, sep=',')
    
    # Check if we have at least 2 rows
    if len(df) >= 2:
        # Check if first timestamp is bigger than second timestamp
        if df['TimeStamp'].iloc[0] > df['TimeStamp'].iloc[1]:
            print(f"  ⚠ First timestamp ({df['TimeStamp'].iloc[0]}) > second timestamp ({df['TimeStamp'].iloc[1]})")
            # Delete the first line
            df = df.drop(index=0).reset_index(drop=True)
            print(f"  ✓ First data line deleted")
    
    # Calculate Eslapted Time (elapsed time from the start)
    first_timestamp = df['TimeStamp'].iloc[0]
    df['Eslapted Time'] = df['TimeStamp'] - first_timestamp
    
    # Calculate Eslapted Distance (cumulative distance in meters from the start)
    distances = [0]  # First point has distance 0
    
    for i in range(1, len(df)):
        lon1 = df['Longitude'].iloc[i-1]
        lat1 = df['Latitude'].iloc[i-1]
        lon2 = df['Longitude'].iloc[i]
        lat2 = df['Latitude'].iloc[i]
        
        # Calculate distance from previous point
        dist = haversine_distance(lon1, lat1, lon2, lat2)
        
        # Add to cumulative distance
        cumulative_dist = distances[-1] + dist
        distances.append(cumulative_dist)
    
    df['Eslapted Distance'] = distances
    
    # Save the processed file
    df.to_csv(output_file, sep=',', index=False)
    
    return df

def process_gps_folder(folder_path):
    """
    Process all GPS CSV files in a folder (recursively):
    - Find all files ending with _GPS.csv
    - Backup original files to _GPS.backup
    - Process and save results with original filename
    
    Args:
        folder_path: Path to the folder containing GPS files
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"Error: Folder '{folder_path}' does not exist!")
        return
    
    if not folder.is_dir():
        print(f"Error: '{folder_path}' is not a directory!")
        return
    
    # Find all files ending with _GPS.csv (recursively in all subfolders)
    gps_files = list(folder.rglob('*_GPS.csv'))
    
    if not gps_files:
        print(f"No files ending with '_GPS.csv' found in folder and subfolders: {folder_path}")
        return
    
    print(f"Found {len(gps_files)} GPS file(s) to process in folder and subfolders:\n")
    
    processed_count = 0
    error_count = 0
    
    for gps_file in gps_files:
        try:
            # Show relative path from the input folder
            relative_path = gps_file.relative_to(folder)
            print(f"Processing: {relative_path}")
            
            # Create backup filename by replacing _GPS.csv with _GPS.backup
            backup_file = gps_file.parent / gps_file.name.replace('_GPS.csv', '_GPS.backup')
            
            # Backup the original file
            shutil.copy2(gps_file, backup_file)
            print(f"  ✓ Backup created: {backup_file.name}")
            
            # Process the file
            df_processed = process_single_gps_file(gps_file, gps_file)
            
            # Display statistics
            total_time_ms = df_processed['Eslapted Time'].iloc[-1]
            total_distance_m = df_processed['Eslapted Distance'].iloc[-1]
            
            print(f"  ✓ Processed {len(df_processed)} rows")
            print(f"  ✓ Total elapsed time: {total_time_ms:.0f} ms ({total_time_ms/1000:.2f} seconds)")
            print(f"  ✓ Total distance: {total_distance_m:.2f} meters")
            print(f"  ✓ Output saved: {relative_path}\n")
            
            processed_count += 1
            
        except Exception as e:
            print(f"  ✗ Error processing {gps_file.name}: {str(e)}\n")
            error_count += 1
    
    # Summary
    print("="*60)
    print(f"Processing complete!")
    print(f"Successfully processed: {processed_count} file(s)")
    if error_count > 0:
        print(f"Errors: {error_count} file(s)")
    print("="*60)

# Example usage
if __name__ == "__main__":
    # Check if folder path is provided as command line argument
    if len(sys.argv) > 1:
        folder_path = sys.argv[1]
    else:
        # Default folder (current directory)
        folder_path = "."
        print(f"No folder specified, using current directory: {Path(folder_path).absolute()}\n")
    
    # Process all GPS files in the folder
    process_gps_folder(folder_path)