#!/usr/bin/env python3
"""
Recursive CSV, Shapefile, and GeoJSON Importer for PostgreSQL/PostGIS

This script recursively searches a source folder for CSV, ESRI shapefiles, and GeoJSON files,
then imports them into a PostgreSQL/PostGIS database with a specified schema.
"""

import os
import sys
from pathlib import Path
import logging
from typing import List, Tuple

try:
    import pandas as pd
    import geopandas as gpd
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as e:
    print(f"Missing required package: {e}")
    print("\nPlease install required packages:")
    print("pip install pandas geopandas sqlalchemy psycopg2-binary --break-system-packages")
    sys.exit(1)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GeoDataImporter:
    """Import CSV, shapefiles, and GeoJSON files into PostgreSQL/PostGIS database."""
    
    def __init__(self, host: str, port: int, database: str, user: str, 
                 password: str, schema: str = 'public', try_to_fusion: bool = False):
        """
        Initialize database connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            schema: Target schema (default: 'public')
            try_to_fusion: If True, merge files with same schema into single tables (default: False)
        """
        self.schema = schema
        self.try_to_fusion = try_to_fusion
        self.connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        self.engine = None
        
    def connect(self) -> bool:
        """Establish database connection and create schema if needed."""
        try:
            self.engine = create_engine(self.connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
                # Create schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
                
                # Enable PostGIS extension if not already enabled
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                conn.commit()
                
            logger.info(f"Successfully connected to database")
            logger.info(f"Using schema: {self.schema}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def find_files(self, source_folder: str) -> Tuple[List[Path], List[Path], List[Path]]:
        """
        Recursively find CSV, shapefile, and GeoJSON files.
        
        Args:
            source_folder: Root folder to search
            
        Returns:
            Tuple of (csv_files, shapefile_paths, geojson_files)
        """
        source_path = Path(source_folder)
        
        if not source_path.exists():
            logger.error(f"Source folder does not exist: {source_folder}")
            return [], [], []
        
        csv_files = []
        shapefiles = []
        geojson_files = []
        
        # Recursively search for files
        for root, dirs, files in os.walk(source_path):
            for file in files:
                file_path = Path(root) / file
                
                if file.lower().endswith('.csv'):
                    csv_files.append(file_path)
                    logger.debug(f"Found CSV: {file_path}")
                    
                elif file.lower().endswith('.shp'):
                    shapefiles.append(file_path)
                    logger.debug(f"Found Shapefile: {file_path}")
                    
                elif file.lower().endswith(('.geojson', '.json')):
                    # Check if it's a valid GeoJSON by looking at the file
                    geojson_files.append(file_path)
                    logger.debug(f"Found GeoJSON: {file_path}")
        
        logger.info(f"Found {len(csv_files)} CSV files, {len(shapefiles)} shapefiles, and {len(geojson_files)} GeoJSON files")
        return csv_files, shapefiles, geojson_files
    
    def sanitize_table_name(self, file_path: Path) -> str:
        """
        Create a valid PostgreSQL table name from file path.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Sanitized table name
        """
        # Use the filename without extension
        table_name = file_path.stem
        
        # Replace invalid characters with underscores
        table_name = ''.join(c if c.isalnum() or c == '_' else '_' 
                            for c in table_name)
        
        # Ensure it starts with a letter or underscore
        if table_name[0].isdigit():
            table_name = f"table_{table_name}"
        
        # Convert to lowercase
        table_name = table_name.lower()
        
        return table_name
    
    def get_csv_schema(self, csv_path: Path) -> tuple:
        """
        Get column names from a CSV file.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Tuple of sorted column names
        """
        try:
            df = pd.read_csv(csv_path, nrows=0)
            return tuple(sorted(df.columns.tolist()))
        except Exception as e:
            logger.warning(f"Could not read schema from {csv_path}: {e}")
            return None
    
    def get_shapefile_schema(self, shp_path: Path) -> tuple:
        """
        Get field names and geometry type from a shapefile.
        
        Args:
            shp_path: Path to shapefile
            
        Returns:
            Tuple of (sorted field names, geometry type)
        """
        try:
            gdf = gpd.read_file(shp_path, rows=1)
            fields = tuple(sorted([col for col in gdf.columns if col != 'geometry']))
            geom_type = gdf.geometry.type.iloc[0] if not gdf.empty else None
            return (fields, geom_type)
        except Exception as e:
            logger.warning(f"Could not read schema from {shp_path}: {e}")
            return None
    
    def get_geojson_schema(self, geojson_path: Path) -> tuple:
        """
        Get property names and geometry type from a GeoJSON file.
        
        Args:
            geojson_path: Path to GeoJSON file
            
        Returns:
            Tuple of (sorted property names, geometry type)
        """
        try:
            gdf = gpd.read_file(geojson_path, rows=1)
            fields = tuple(sorted([col for col in gdf.columns if col != 'geometry']))
            geom_type = gdf.geometry.type.iloc[0] if not gdf.empty else None
            return (fields, geom_type)
        except Exception as e:
            logger.warning(f"Could not read schema from {geojson_path}: {e}")
            return None
    
    def group_files_by_schema(self, files: List[Path], file_type: str) -> dict:
        """
        Group files by matching schemas.
        
        Args:
            files: List of file paths
            file_type: Type of files ('csv', 'shapefile', or 'geojson')
            
        Returns:
            Dictionary mapping schema to list of files with that schema
        """
        schema_groups = {}
        
        for file_path in files:
            # Get schema based on file type
            if file_type == 'csv':
                schema = self.get_csv_schema(file_path)
            elif file_type == 'shapefile':
                schema = self.get_shapefile_schema(file_path)
            elif file_type == 'geojson':
                schema = self.get_geojson_schema(file_path)
            else:
                continue
            
            if schema is None:
                continue
            
            # Group files by schema
            if schema not in schema_groups:
                schema_groups[schema] = []
            schema_groups[schema].append(file_path)
        
        return schema_groups
    
    def generate_fusion_table_name(self, files: List[Path], file_type: str) -> str:
        """
        Generate a table name for fused files.
        
        Args:
            files: List of file paths to be fused
            file_type: Type of files
            
        Returns:
            Table name for the fused table
        """
        # Use the file type as base name
        if len(files) == 1:
            return self.sanitize_table_name(files[0])
        else:
            # Create a generic name based on file type
            return f"{file_type}_fusion_{len(files)}_files"
    
    def import_csv(self, csv_path: Path, table_name: str = None, mode: str = 'replace') -> bool:
        """
        Import a CSV file into PostgreSQL.
        
        Args:
            csv_path: Path to CSV file
            table_name: Target table name (default: auto-generated from filename)
            mode: Import mode - 'replace', 'append', or 'fail' (default: 'replace')
            
        Returns:
            True if successful, False otherwise
        """
        if table_name is None:
            table_name = self.sanitize_table_name(csv_path)
        
        try:
            logger.info(f"Importing CSV: {csv_path.name} -> {self.schema}.{table_name} (mode: {mode})")
            
            # Read CSV
            df = pd.read_csv(csv_path)
            
            if df.empty:
                logger.warning(f"CSV file is empty: {csv_path}")
                return False
            
            # Add source_filename column
            df['source_filename'] = csv_path.name
            
            # Import to database
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=mode,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✓ Successfully imported {len(df)} rows to {self.schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import CSV {csv_path}: {e}")
            return False
    
    def import_shapefile(self, shp_path: Path, table_name: str = None, mode: str = 'replace') -> bool:
        """
        Import a shapefile into PostGIS.
        
        Args:
            shp_path: Path to shapefile (.shp)
            table_name: Target table name (default: auto-generated from filename)
            mode: Import mode - 'replace', 'append', or 'fail' (default: 'replace')
            
        Returns:
            True if successful, False otherwise
        """
        if table_name is None:
            table_name = self.sanitize_table_name(shp_path)
        
        try:
            logger.info(f"Importing Shapefile: {shp_path.name} -> {self.schema}.{table_name} (mode: {mode})")
            
            # Read shapefile
            gdf = gpd.read_file(shp_path)
            
            if gdf.empty:
                logger.warning(f"Shapefile is empty: {shp_path}")
                return False
            
            # Get CRS information
            crs_info = f"EPSG:{gdf.crs.to_epsg()}" if gdf.crs else "Unknown"
            logger.info(f"  CRS: {crs_info}, Geometry Type: {gdf.geometry.type.unique()}")
            
            # Add source_filename column
            gdf['source_filename'] = shp_path.name
            
            # Import to PostGIS
            gdf.to_postgis(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=mode,
                index=False,
                chunksize=1000
            )
            
            logger.info(f"✓ Successfully imported {len(gdf)} features to {self.schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import shapefile {shp_path}: {e}")
            return False
    
    def import_geojson(self, geojson_path: Path, table_name: str = None, mode: str = 'replace') -> bool:
        """
        Import a GeoJSON file into PostGIS.
        
        Args:
            geojson_path: Path to GeoJSON file (.geojson or .json)
            table_name: Target table name (default: auto-generated from filename)
            mode: Import mode - 'replace', 'append', or 'fail' (default: 'replace')
            
        Returns:
            True if successful, False otherwise
        """
        if table_name is None:
            table_name = self.sanitize_table_name(geojson_path)
        
        try:
            logger.info(f"Importing GeoJSON: {geojson_path.name} -> {self.schema}.{table_name} (mode: {mode})")
            
            # Read GeoJSON
            gdf = gpd.read_file(geojson_path)
            
            if gdf.empty:
                logger.warning(f"GeoJSON file is empty: {geojson_path}")
                return False
            
            # Get CRS information
            crs_info = f"EPSG:{gdf.crs.to_epsg()}" if gdf.crs else "Unknown"
            logger.info(f"  CRS: {crs_info}, Geometry Type: {gdf.geometry.type.unique()}")
            
            # Add source_filename column
            gdf['source_filename'] = geojson_path.name
            
            # Import to PostGIS
            gdf.to_postgis(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=mode,
                index=False,
                chunksize=1000
            )
            
            logger.info(f"✓ Successfully imported {len(gdf)} features to {self.schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import GeoJSON {geojson_path}: {e}")
            return False
    
    def import_all(self, source_folder: str) -> dict:
        """
        Import all CSV, shapefiles, and GeoJSON files from source folder.
        
        Args:
            source_folder: Root folder to search
            
        Returns:
            Dictionary with import statistics
        """
        # Find all files
        csv_files, shapefiles, geojson_files = self.find_files(source_folder)
        
        stats = {
            'csv_success': 0,
            'csv_failed': 0,
            'shp_success': 0,
            'shp_failed': 0,
            'geojson_success': 0,
            'geojson_failed': 0,
            'csv_tables': 0,
            'shp_tables': 0,
            'geojson_tables': 0
        }
        
        if self.try_to_fusion:
            logger.info("\n" + "="*60)
            logger.info("FUSION MODE ENABLED")
            logger.info("Grouping files by matching schemas...")
            logger.info("="*60)
            
            # Import CSV files with fusion
            logger.info("\n" + "="*60)
            logger.info("Processing CSV files...")
            logger.info("="*60)
            
            csv_groups = self.group_files_by_schema(csv_files, 'csv')
            logger.info(f"Found {len(csv_groups)} unique CSV schema(s)")
            
            for idx, (schema, files) in enumerate(csv_groups.items(), 1):
                table_name = self.generate_fusion_table_name(files, 'csv')
                if len(files) > 1:
                    table_name = f"csv_group_{idx}"
                    
                logger.info(f"\nGroup {idx}: {len(files)} file(s) with matching schema -> {self.schema}.{table_name}")
                logger.info(f"  Fields: {', '.join(schema[:5])}{'...' if len(schema) > 5 else ''}")
                
                for i, csv_file in enumerate(files):
                    mode = 'replace' if i == 0 else 'append'
                    if self.import_csv(csv_file, table_name, mode):
                        stats['csv_success'] += 1
                    else:
                        stats['csv_failed'] += 1
                
                if stats['csv_success'] > 0:
                    stats['csv_tables'] += 1
            
            # Import shapefiles with fusion
            logger.info("\n" + "="*60)
            logger.info("Processing Shapefiles...")
            logger.info("="*60)
            
            shp_groups = self.group_files_by_schema(shapefiles, 'shapefile')
            logger.info(f"Found {len(shp_groups)} unique shapefile schema(s)")
            
            for idx, (schema, files) in enumerate(shp_groups.items(), 1):
                fields, geom_type = schema
                table_name = self.generate_fusion_table_name(files, 'shapefile')
                if len(files) > 1:
                    table_name = f"shapefile_group_{idx}"
                
                logger.info(f"\nGroup {idx}: {len(files)} file(s) with matching schema -> {self.schema}.{table_name}")
                logger.info(f"  Fields: {', '.join(fields[:5])}{'...' if len(fields) > 5 else ''}")
                logger.info(f"  Geometry: {geom_type}")
                
                for i, shp_file in enumerate(files):
                    mode = 'replace' if i == 0 else 'append'
                    if self.import_shapefile(shp_file, table_name, mode):
                        stats['shp_success'] += 1
                    else:
                        stats['shp_failed'] += 1
                
                if stats['shp_success'] > 0:
                    stats['shp_tables'] += 1
            
            # Import GeoJSON files with fusion
            logger.info("\n" + "="*60)
            logger.info("Processing GeoJSON files...")
            logger.info("="*60)
            
            geojson_groups = self.group_files_by_schema(geojson_files, 'geojson')
            logger.info(f"Found {len(geojson_groups)} unique GeoJSON schema(s)")
            
            for idx, (schema, files) in enumerate(geojson_groups.items(), 1):
                fields, geom_type = schema
                table_name = self.generate_fusion_table_name(files, 'geojson')
                if len(files) > 1:
                    table_name = f"geojson_group_{idx}"
                
                logger.info(f"\nGroup {idx}: {len(files)} file(s) with matching schema -> {self.schema}.{table_name}")
                logger.info(f"  Fields: {', '.join(fields[:5])}{'...' if len(fields) > 5 else ''}")
                logger.info(f"  Geometry: {geom_type}")
                
                for i, geojson_file in enumerate(files):
                    mode = 'replace' if i == 0 else 'append'
                    if self.import_geojson(geojson_file, table_name, mode):
                        stats['geojson_success'] += 1
                    else:
                        stats['geojson_failed'] += 1
                
                if stats['geojson_success'] > 0:
                    stats['geojson_tables'] += 1
        
        else:
            # Normal mode - one file per table
            logger.info("\n" + "="*60)
            logger.info("Importing CSV files...")
            logger.info("="*60)
            
            for csv_file in csv_files:
                if self.import_csv(csv_file):
                    stats['csv_success'] += 1
                    stats['csv_tables'] += 1
                else:
                    stats['csv_failed'] += 1
            
            # Import shapefiles
            logger.info("\n" + "="*60)
            logger.info("Importing Shapefiles...")
            logger.info("="*60)
            
            for shp_file in shapefiles:
                if self.import_shapefile(shp_file):
                    stats['shp_success'] += 1
                    stats['shp_tables'] += 1
                else:
                    stats['shp_failed'] += 1
            
            # Import GeoJSON files
            logger.info("\n" + "="*60)
            logger.info("Importing GeoJSON files...")
            logger.info("="*60)
            
            for geojson_file in geojson_files:
                if self.import_geojson(geojson_file):
                    stats['geojson_success'] += 1
                    stats['geojson_tables'] += 1
                else:
                    stats['geojson_failed'] += 1
        
        return stats
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def main():
    """Main function with example usage."""
    
    print("="*60)
    print("CSV, Shapefile, and GeoJSON to PostgreSQL/PostGIS Importer")
    print("="*60)
    print()
    
    # Configuration - EDIT THESE VALUES
    config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'CD93_2023',
        'user': 'diagway',
        'password': 'diagway',
        'schema': 'offroad',  # Target schema
        'source_folder': 'D:/Tmp/Offroad_cd93',  # Source folder
        'try_to_fusion': True  # Set to True to merge files with matching schemas
    }

    # You can also get values from environment variables for security
    # import os
    # config['password'] = os.getenv('DB_PASSWORD', 'default_password')
    
    print("Configuration:")
    print(f"  Host: {config['host']}:{config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  Schema: {config['schema']}")
    print(f"  Source Folder: {config['source_folder']}")
    print(f"  Fusion Mode: {config['try_to_fusion']}")
    print()
    
    # Create importer instance
    importer = GeoDataImporter(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password'],
        schema=config['schema'],
        try_to_fusion=config['try_to_fusion']
    )
    
    # Connect to database
    if not importer.connect():
        logger.error("Failed to connect to database. Exiting.")
        sys.exit(1)
    
    try:
        # Import all files
        stats = importer.import_all(config['source_folder'])
        
        # Print summary
        print()
        print("="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"CSV Files:")
        print(f"  ✓ Success: {stats['csv_success']}")
        print(f"  ✗ Failed:  {stats['csv_failed']}")
        print(f"  → Tables:  {stats['csv_tables']}")
        print(f"\nShapefiles:")
        print(f"  ✓ Success: {stats['shp_success']}")
        print(f"  ✗ Failed:  {stats['shp_failed']}")
        print(f"  → Tables:  {stats['shp_tables']}")
        print(f"\nGeoJSON Files:")
        print(f"  ✓ Success: {stats['geojson_success']}")
        print(f"  ✗ Failed:  {stats['geojson_failed']}")
        print(f"  → Tables:  {stats['geojson_tables']}")
        print(f"\nTotal:")
        print(f"  ✓ Files Imported: {stats['csv_success'] + stats['shp_success'] + stats['geojson_success']}")
        print(f"  ✗ Files Failed:   {stats['csv_failed'] + stats['shp_failed'] + stats['geojson_failed']}")
        print(f"  → Total Tables:   {stats['csv_tables'] + stats['shp_tables'] + stats['geojson_tables']}")
        print("="*60)
        
    finally:
        importer.close()


if __name__ == "__main__":
    main()
