"""
Script to read and list all layers/tables in an ArcGIS GeoDatabase (.gdb)

Requirements:
    pip install fiona geopandas

Usage:
    python list_gdb_layers.py /path/to/your/geodatabase.gdb
"""

import fiona
import sys
import os
from pathlib import Path


def list_gdb_layers(gdb_path):
    """
    List all layers/tables in a GeoDatabase file
    
    Args:
        gdb_path (str): Path to the .gdb file
        
    Returns:
        list: List of layer names
    """
    try:
        # Check if the path exists
        if not os.path.exists(gdb_path):
            print(f"Error: The path '{gdb_path}' does not exist.")
            return None
        
        # List all layers in the GeoDatabase
        layers = fiona.listlayers(gdb_path)
        
        if not layers:
            print(f"No layers found in {gdb_path}")
            return []
        
        print(f"\n{'='*60}")
        print(f"GeoDatabase: {gdb_path}")
        print(f"{'='*60}")
        print(f"Total number of layers: {len(layers)}\n")
        
        # Display layers with details
        for idx, layer in enumerate(layers, 1):
            print(f"{idx}. {layer}")
            
            # Try to get additional information about each layer
            try:
                with fiona.open(gdb_path, layer=layer) as src:
                    print(f"   - Geometry type: {src.schema['geometry']}")
                    print(f"   - CRS: {src.crs}")
                    print(f"   - Feature count: {len(src)}")
                    print(f"   - Fields: {', '.join(src.schema['properties'].keys())}")
                    print()
            except Exception as e:
                print(f"   - Could not read layer details: {e}\n")
        
        return layers
        
    except Exception as e:
        print(f"Error reading GeoDatabase: {e}")
        return None


def export_layer_info_to_file(gdb_path, output_file="gdb_layers_info.txt"):
    """
    Export layer information to a text file
    
    Args:
        gdb_path (str): Path to the .gdb file
        output_file (str): Output text file name
    """
    try:
        layers = fiona.listlayers(gdb_path)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"GeoDatabase: {gdb_path}\n")
            f.write(f"{'='*60}\n")
            f.write(f"Total layers: {len(layers)}\n\n")
            
            for idx, layer in enumerate(layers, 1):
                f.write(f"{idx}. {layer}\n")
                
                try:
                    with fiona.open(gdb_path, layer=layer) as src:
                        f.write(f"   - Geometry type: {src.schema['geometry']}\n")
                        f.write(f"   - CRS: {src.crs}\n")
                        f.write(f"   - Feature count: {len(src)}\n")
                        f.write(f"   - Fields: {', '.join(src.schema['properties'].keys())}\n")
                        f.write("\n")
                except Exception as e:
                    f.write(f"   - Error: {e}\n\n")
        
        print(f"\nLayer information exported to: {output_file}")
        
    except Exception as e:
        print(f"Error exporting layer info: {e}")


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python list_gdb_layers.py <path_to_geodatabase.gdb>")
        print("\nExample:")
        print("  python list_gdb_layers.py /path/to/your/geodatabase.gdb")
        sys.exit(1)
    
    gdb_path = sys.argv[1]
    
    # List layers
    layers = list_gdb_layers(gdb_path)
    
    # Optionally export to file
    if layers:
        export = input("\nDo you want to export this information to a file? (y/n): ")
        if export.lower() == 'y':
            export_layer_info_to_file(gdb_path)


if __name__ == "__main__":
    main()