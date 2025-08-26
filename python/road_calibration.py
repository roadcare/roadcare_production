#!/usr/bin/env python3
"""
Road Network Linear Referencing Calibration Script
Transforms MultiLineString geometries to LineStringM (measured) for linear referencing
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Optional
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RoadCalibrator:
    """Class to handle road network calibration and linear referencing"""
    
    def __init__(self, db_config: dict):
        """
        Initialize the RoadCalibrator with database configuration
        
        Args:
            db_config (dict): Database connection parameters
        """
        self.db_config = db_config
        self.connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL/PostGIS database
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Successfully connected to database")
            
            # Verify PostGIS extension
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT PostGIS_Version();")
                postgis_version = cursor.fetchone()[0]
                logger.info(f"PostGIS Version: {postgis_version}")
            
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_troncon_table(self) -> bool:
        """
        Create the client.troncon_client table if it doesn't exist
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS client.troncon_client (
                    id SERIAL PRIMARY KEY,
                    route_id INTEGER REFERENCES client.route_client(id),
                    axe VARCHAR,
                    longueur DECIMAL,
                    geom_calibrated GEOMETRY(LINESTRINGM, 2154),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                cursor.execute(create_table_sql)
                
                # Create spatial index
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_geom 
                ON client.troncon_client USING GIST (geom_calibrated);
                """)
                
                self.connection.commit()
                logger.info("Table client.troncon_client created/verified successfully")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to create troncon table: {e}")
            self.connection.rollback()
            return False
    
    def calibrate_routes(self) -> bool:
        """
        Main calibration function: Transform MultiLineString to LineStringM
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Clear existing calibrated data
                cursor.execute("DELETE FROM client.troncon_client;")
                logger.info("Cleared existing calibrated data")
                
                # Fetch routes to calibrate
                select_sql = """
                SELECT id, axe, longueur, geom
                FROM client.route_client
                ORDER BY id;
                """
                
                cursor.execute(select_sql)
                routes = cursor.fetchall()
                logger.info(f"Found {len(routes)} routes to calibrate")
                
                calibrated_count = 0
                
                for route in routes:
                    if self._calibrate_single_route(route):
                        calibrated_count += 1
                
                self.connection.commit()
                logger.info(f"Successfully calibrated {calibrated_count}/{len(routes)} routes")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed during route calibration: {e}")
            self.connection.rollback()
            return False
    
    def _calibrate_single_route(self, route: dict) -> bool:
        """
        Calibrate a single route geometry, handling MultiLineString with gaps
        Maintains continuous measure values across all segments
        
        Args:
            route (dict): Route data from database
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Get geometry information
                cursor.execute("""
                SELECT 
                    ST_GeometryType(%(geom)s) as geom_type,
                    ST_NumGeometries(%(geom)s) as num_parts
                """, {'geom': route['geom']})
                
                geom_info = cursor.fetchone()
                geom_type, num_parts = geom_info
                
                if geom_type == 'ST_MultiLineString' and num_parts > 1:
                    # Handle MultiLineString with multiple segments
                    return self._calibrate_multilinestring_route(route, num_parts)
                else:
                    # Handle single LineString or single-part MultiLineString
                    return self._calibrate_simple_route(route)
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate route {route['id']}: {e}")
            return False
    
    def _calibrate_simple_route(self, route: dict) -> bool:
        """Calibrate a simple single-segment route"""
        try:
            with self.connection.cursor() as cursor:
                calibration_sql = """
                INSERT INTO client.troncon_client (route_id, axe, longueur, geom_calibrated)
                SELECT 
                    %(route_id)s,
                    %(axe)s,
                    %(longueur)s,
                    ST_AddMeasure(
                        CASE 
                            WHEN ST_GeometryType(%(geom)s) = 'ST_MultiLineString' THEN
                                ST_GeometryN(%(geom)s, 1)
                            ELSE 
                                %(geom)s
                        END,
                        0.0,
                        CASE 
                            WHEN ST_GeometryType(%(geom)s) = 'ST_MultiLineString' THEN
                                ST_Length(ST_GeometryN(%(geom)s, 1))
                            ELSE 
                                ST_Length(%(geom)s)
                        END
                    ) as geom_calibrated;
                """
                
                cursor.execute(calibration_sql, {
                    'route_id': route['id'],
                    'axe': route['axe'],
                    'longueur': route['longueur'],
                    'geom': route['geom']
                })
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate simple route {route['id']}: {e}")
            return False
    
    def _calibrate_multilinestring_route(self, route: dict, num_parts: int) -> bool:
        """
        Calibrate MultiLineString route with continuous measures across all segments
        Each segment gets continuous measure values, maintaining linear referencing integrity
        """
        try:
            with self.connection.cursor() as cursor:
                # Calculate cumulative measures for each segment
                cumulative_measure = 0.0
                
                for part_num in range(1, num_parts + 1):
                    # Get segment length
                    cursor.execute("""
                    SELECT ST_Length(ST_GeometryN(%(geom)s, %(part_num)s)) as segment_length
                    """, {'geom': route['geom'], 'part_num': part_num})
                    
                    segment_length = cursor.fetchone()[0]
                    start_measure = cumulative_measure
                    end_measure = cumulative_measure + segment_length
                    
                    # Insert calibrated segment with continuous measures
                    part_calibration_sql = """
                    INSERT INTO client.troncon_client (route_id, axe, longueur, geom_calibrated)
                    SELECT 
                        %(route_id)s,
                        %(axe)s || '_' || %(part_num)s::text,  -- Add segment identifier
                        %(segment_length)s,
                        ST_AddMeasure(
                            ST_GeometryN(%(geom)s, %(part_num)s),
                            %(start_measure)s,
                            %(end_measure)s
                        ) as geom_calibrated;
                    """
                    
                    cursor.execute(part_calibration_sql, {
                        'route_id': route['id'],
                        'axe': route['axe'],
                        'part_num': part_num,
                        'segment_length': segment_length,
                        'start_measure': start_measure,
                        'end_measure': end_measure,
                        'geom': route['geom']
                    })
                    
                    cumulative_measure = end_measure
                    
                    logger.debug(f"Calibrated segment {part_num}/{num_parts} of route {route['axe']}: "
                               f"measures {start_measure:.2f} to {end_measure:.2f}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate MultiLineString route {route['id']}: {e}")
            return False
    
    def validate_calibration(self) -> bool:
        """
        Validate the calibration results
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Check if all routes were calibrated
                cursor.execute("SELECT COUNT(*) FROM client.route_client;")
                original_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM client.troncon_client;")
                calibrated_count = cursor.fetchone()[0]
                
                logger.info(f"Original routes: {original_count}")
                logger.info(f"Calibrated segments: {calibrated_count}")
                
                # Validate geometry types
                cursor.execute("""
                SELECT COUNT(*) FROM client.troncon_client 
                WHERE ST_GeometryType(geom_calibrated) != 'ST_LineStringM';
                """)
                
                invalid_geom_count = cursor.fetchone()[0]
                
                if invalid_geom_count > 0:
                    logger.warning(f"Found {invalid_geom_count} geometries that are not LineStringM")
                    return False
                
                # Check measure values
                cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN ST_M(ST_StartPoint(geom_calibrated)) = 0 THEN 1 END) as start_zero,
                    AVG(ST_M(ST_EndPoint(geom_calibrated))) as avg_end_measure
                FROM client.troncon_client;
                """)
                
                stats = cursor.fetchone()
                logger.info(f"Validation - Total: {stats[0]}, Start at 0: {stats[1]}, Avg end measure: {stats[2]:.2f}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Validation failed: {e}")
            return False
    
    def get_calibration_summary(self) -> Optional[dict]:
        """
        Get summary statistics of the calibration process
        
        Returns:
            dict: Summary statistics or None if error
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_segments,
                    MIN(longueur) as min_length,
                    MAX(longueur) as max_length,
                    AVG(longueur) as avg_length,
                    SUM(longueur) as total_length,
                    MIN(ST_M(ST_EndPoint(geom_calibrated))) as min_measure,
                    MAX(ST_M(ST_EndPoint(geom_calibrated))) as max_measure
                FROM client.troncon_client;
                """)
                
                return dict(cursor.fetchone())
                
        except psycopg2.Error as e:
            logger.error(f"Failed to get summary: {e}")
            return None


def main():
    """Main execution function"""
    
    # Database configuration
    db_config = {
        'host': 'localhost',          # Update with your host
        'database': 'cd08_demo',     # Update with your database name
        'user': 'diagway',           # Update with your username
        'password': 'diagway',  # Update with your password
        'port': 5433                  # Update with your port
    }
    
    # Initialize calibrator
    calibrator = RoadCalibrator(db_config)
    
    try:
        # Connect to database
        if not calibrator.connect():
            logger.error("Failed to connect to database. Exiting.")
            sys.exit(1)
        
        # Create target table
        if not calibrator.create_troncon_table():
            logger.error("Failed to create target table. Exiting.")
            sys.exit(1)
        
        # Perform calibration
        logger.info("Starting route calibration...")
        if not calibrator.calibrate_routes():
            logger.error("Calibration failed. Exiting.")
            sys.exit(1)
        
        # Validate results
        logger.info("Validating calibration results...")
        if not calibrator.validate_calibration():
            logger.warning("Validation found issues, but calibration completed")
        
        # Get summary
        summary = calibrator.get_calibration_summary()
        if summary:
            logger.info("Calibration Summary:")
            logger.info(f"  Total segments: {summary['total_segments']}")
            logger.info(f"  Length range: {summary['min_length']:.2f} - {summary['max_length']:.2f}")
            logger.info(f"  Average length: {summary['avg_length']:.2f}")
            logger.info(f"  Total network length: {summary['total_length']:.2f}")
            logger.info(f"  Measure range: {summary['min_measure']:.2f} - {summary['max_measure']:.2f}")
        
        logger.info("Road network calibration completed successfully!")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        calibrator.disconnect()


if __name__ == "__main__":
    main()