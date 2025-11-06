#!/usr/bin/env python3
"""
Recursive CSV and Shapefile Importer for PostgreSQL/PostGIS

This script recursively searches a source folder for CSV and ESRI shapefiles,
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
    """Import CSV and shapefiles into PostgreSQL/PostGIS database."""
    
    def __init__(self, host: str, port: int, database: str, user: str, 
                 password: str, schema: str = 'public'):
        """
        Initialize database connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            schema: Target schema (default: 'public')
        """
        self.schema = schema
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
    
    def find_files(self, source_folder: str) -> Tuple[List[Path], List[Path]]:
        """
        Recursively find CSV and shapefile files.
        
        Args:
            source_folder: Root folder to search
            
        Returns:
            Tuple of (csv_files, shapefile_paths)
        """
        source_path = Path(source_folder)
        
        if not source_path.exists():
            logger.error(f"Source folder does not exist: {source_folder}")
            return [], []
        
        csv_files = []
        shapefiles = []
        
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
        
        logger.info(f"Found {len(csv_files)} CSV files and {len(shapefiles)} shapefiles")
        return csv_files, shapefiles
    
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
    
    def import_csv(self, csv_path: Path) -> bool:
        """
        Import a CSV file into PostgreSQL.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            True if successful, False otherwise
        """
        table_name = self.sanitize_table_name(csv_path)
        
        try:
            logger.info(f"Importing CSV: {csv_path.name} -> {self.schema}.{table_name}")
            
            # Read CSV
            df = pd.read_csv(csv_path)
            
            if df.empty:
                logger.warning(f"CSV file is empty: {csv_path}")
                return False
            
            # Import to database
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists='replace',  # Change to 'append' or 'fail' as needed
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✓ Successfully imported {len(df)} rows to {self.schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import CSV {csv_path}: {e}")
            return False
    
    def import_shapefile(self, shp_path: Path) -> bool:
        """
        Import a shapefile into PostGIS.
        
        Args:
            shp_path: Path to shapefile (.shp)
            
        Returns:
            True if successful, False otherwise
        """
        table_name = self.sanitize_table_name(shp_path)
        
        try:
            logger.info(f"Importing Shapefile: {shp_path.name} -> {self.schema}.{table_name}")
            
            # Read shapefile
            gdf = gpd.read_file(shp_path)
            
            if gdf.empty:
                logger.warning(f"Shapefile is empty: {shp_path}")
                return False
            
            # Get CRS information
            crs_info = f"EPSG:{gdf.crs.to_epsg()}" if gdf.crs else "Unknown"
            logger.info(f"  CRS: {crs_info}, Geometry Type: {gdf.geometry.type.unique()}")
            
            # Import to PostGIS
            gdf.to_postgis(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists='replace',  # Change to 'append' or 'fail' as needed
                index=False,
                chunksize=1000
            )
            
            logger.info(f"✓ Successfully imported {len(gdf)} features to {self.schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import shapefile {shp_path}: {e}")
            return False
    
    def import_all(self, source_folder: str) -> dict:
        """
        Import all CSV and shapefiles from source folder.
        
        Args:
            source_folder: Root folder to search
            
        Returns:
            Dictionary with import statistics
        """
        # Find all files
        csv_files, shapefiles = self.find_files(source_folder)
        
        stats = {
            'csv_success': 0,
            'csv_failed': 0,
            'shp_success': 0,
            'shp_failed': 0
        }
        
        # Import CSV files
        logger.info("\n" + "="*60)
        logger.info("Importing CSV files...")
        logger.info("="*60)
        
        for csv_file in csv_files:
            if self.import_csv(csv_file):
                stats['csv_success'] += 1
            else:
                stats['csv_failed'] += 1
        
        # Import shapefiles
        logger.info("\n" + "="*60)
        logger.info("Importing Shapefiles...")
        logger.info("="*60)
        
        for shp_file in shapefiles:
            if self.import_shapefile(shp_file):
                stats['shp_success'] += 1
            else:
                stats['shp_failed'] += 1
        
        return stats
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def main():
    """Main function with example usage."""
    
    print("="*60)
    print("CSV and Shapefile to PostgreSQL/PostGIS Importer")
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
        'source_folder': 'G:/01_Affaires/06-AFFAIRES_ROADCARE/37_SIGNATURE_SH_CD93/04-Traitement/05_OFFROAD/CD93'  # Source folder
    }
    
    # You can also get values from environment variables for security
    # import os
    # config['password'] = os.getenv('DB_PASSWORD', 'default_password')
    
    print("Configuration:")
    print(f"  Host: {config['host']}:{config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  Schema: {config['schema']}")
    print(f"  Source Folder: {config['source_folder']}")
    print()
    
    # Create importer instance
    importer = GeoDataImporter(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password'],
        schema=config['schema']
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
        print(f"\nShapefiles:")
        print(f"  ✓ Success: {stats['shp_success']}")
        print(f"  ✗ Failed:  {stats['shp_failed']}")
        print(f"\nTotal:")
        print(f"  ✓ Success: {stats['csv_success'] + stats['shp_success']}")
        print(f"  ✗ Failed:  {stats['csv_failed'] + stats['shp_failed']}")
        print("="*60)
        
    finally:
        importer.close()


if __name__ == "__main__":
    main()
