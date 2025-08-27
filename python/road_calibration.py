#!/usr/bin/env python3
"""
Road Network Linear Referencing Calibration Script
Transforms MultiLineString geometries to LineStringM (measured) for linear referencing
with ratio adjustment to match longueur field values

Final Version - Updated with id_tronc structure
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
    """Class to handle road network calibration and linear referencing with ratio adjustment"""
    
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
                    axe VARCHAR,  -- Keep original axe name from route_client
                    id_tronc TEXT,  -- Segment identifier (axe + segment number)
                    geom_calib GEOMETRY(LINESTRINGM, 2154),
                    cumuld DECIMAL,  -- Start measure adjusted by ratio
                    cumulf DECIMAL,  -- End measure adjusted by ratio
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                cursor.execute(create_table_sql)
                
                # Create spatial index
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_geom 
                ON client.troncon_client USING GIST (geom_calib);
                """)
                
                # Create indexes on measure fields
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_cumuld 
                ON client.troncon_client (cumuld);
                """)
                
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_cumulf 
                ON client.troncon_client (cumulf);
                """)
                
                # Create index on axe and id_tronc
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_axe_tronc 
                ON client.troncon_client (axe, id_tronc);
                """)
                
                self.connection.commit()
                logger.info("Table client.troncon_client created/verified successfully with id_tronc structure")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to create troncon table: {e}")
            self.connection.rollback()
            return False
    
    def calibrate_routes(self) -> bool:
        """
        Main calibration function: Transform MultiLineString to LineStringM with ratio adjustment
        
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
                logger.info(f"Successfully calibrated {calibrated_count}/{len(routes)} routes with ratio adjustment")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed during route calibration: {e}")
            self.connection.rollback()
            return False
    
    def _calibrate_single_route(self, route: dict) -> bool:
        """
        Calibrate a single route geometry, handling MultiLineString with gaps
        Maintains continuous measure values across all segments with ratio adjustment
        
        Args:
            route (dict): Route data from database
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Get geometry information and calculate total geometric length
                cursor.execute("""
                SELECT 
                    ST_GeometryType(%(geom)s) as geom_type,
                    ST_NumGeometries(%(geom)s) as num_parts,
                    ST_Length(%(geom)s) as total_geometric_length
                """, {'geom': route['geom']})
                
                geom_info = cursor.fetchone()
                geom_type, num_parts, total_geometric_length = geom_info
                
                # Calculate ratio: longueur_field / real_geometric_length
                ratio = float(route['longueur']) / float(total_geometric_length) if total_geometric_length > 0 else 1.0
                
                logger.debug(f"Route {route['axe']}: longueur_field={route['longueur']}, "
                           f"geometric_length={total_geometric_length:.2f}, ratio={ratio:.6f}")
                
                if geom_type == 'ST_MultiLineString' and num_parts > 1:
                    # Handle MultiLineString with multiple segments
                    return self._calibrate_multilinestring_route(route, num_parts, ratio)
                else:
                    # Handle single LineString or single-part MultiLineString
                    return self._calibrate_simple_route(route, ratio)
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate route {route['id']}: {e}")
            return False
    
    def _calibrate_simple_route(self, route: dict, ratio: float) -> bool:
        """Calibrate a simple single-segment route with ratio adjustment"""
        try:
            with self.connection.cursor() as cursor:
                calibration_sql = """
                INSERT INTO client.troncon_client 
                (axe, id_tronc, geom_calib, cumuld, cumulf)
                SELECT 
                    %(axe)s,
                    %(axe)s || '_1' as id_tronc,  -- Single segment gets _1
                    ST_AddMeasure(
                        CASE 
                            WHEN ST_GeometryType(%(geom)s) = 'ST_MultiLineString' THEN
                                ST_GeometryN(%(geom)s, 1)
                            ELSE 
                                %(geom)s
                        END,
                        0.0,  -- Geometric start measure
                        CASE 
                            WHEN ST_GeometryType(%(geom)s) = 'ST_MultiLineString' THEN
                                ST_Length(ST_GeometryN(%(geom)s, 1))
                            ELSE 
                                ST_Length(%(geom)s)
                        END   -- Geometric end measure
                    ) as geom_calib,
                    0.0 as cumuld,  -- Adjusted start measure
                    %(longueur)s as cumulf;  -- Adjusted end measure (should equal longueur field)
                """
                
                cursor.execute(calibration_sql, {
                    'axe': route['axe'],
                    'longueur': route['longueur'],
                    'geom': route['geom']
                })
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate simple route {route['id']}: {e}")
            return False
    
    def _calibrate_multilinestring_route(self, route: dict, num_parts: int, ratio: float) -> bool:
        """
        Calibrate MultiLineString route with continuous measures across all segments
        Each segment gets continuous measure values with ratio adjustment
        """
        try:
            with self.connection.cursor() as cursor:
                # Calculate cumulative measures for each segment (geometric)
                cumulative_geometric_measure = 0.0
                cumulative_adjusted_measure = 0.0
                
                for part_num in range(1, num_parts + 1):
                    # Get segment length
                    cursor.execute("""
                    SELECT ST_Length(ST_GeometryN(%(geom)s, %(part_num)s)) as segment_length
                    """, {'geom': route['geom'], 'part_num': part_num})
                    
                    segment_geometric_length = cursor.fetchone()[0]
                    segment_adjusted_length = segment_geometric_length * ratio
                    
                    # Geometric measures (for ST_AddMeasure)
                    geometric_start = cumulative_geometric_measure
                    geometric_end = cumulative_geometric_measure + segment_geometric_length
                    
                    # Adjusted measures (for cumuld/cumulf fields)
                    adjusted_start = cumulative_adjusted_measure
                    adjusted_end = cumulative_adjusted_measure + segment_adjusted_length
                    
                    # For the last segment, ensure it ends exactly at longueur value
                    if part_num == num_parts:
                        adjusted_end = float(route['longueur'])
                    
                    # Insert calibrated segment with both geometric and adjusted measures
                    part_calibration_sql = """
                    INSERT INTO client.troncon_client 
                    (axe, id_tronc, geom_calib, cumuld, cumulf)
                    SELECT 
                        %(axe)s,  -- Keep original axe name
                        %(axe)s || '_' || %(part_num)s::text as id_tronc,  -- axe + segment number
                        ST_AddMeasure(
                            ST_GeometryN(%(geom)s, %(part_num)s),
                            %(geometric_start)s,
                            %(geometric_end)s
                        ) as geom_calib,
                        %(adjusted_start)s as cumuld,
                        %(adjusted_end)s as cumulf;
                    """
                    
                    cursor.execute(part_calibration_sql, {
                        'axe': route['axe'],
                        'part_num': part_num,
                        'geometric_start': geometric_start,
                        'geometric_end': geometric_end,
                        'adjusted_start': adjusted_start,
                        'adjusted_end': adjusted_end,
                        'geom': route['geom']
                    })
                    
                    cumulative_geometric_measure = geometric_end
                    cumulative_adjusted_measure = adjusted_end
                    
                    logger.debug(f"Calibrated segment {part_num}/{num_parts} of route {route['axe']}: "
                               f"id_tronc={route['axe']}_{part_num}, "
                               f"geometric: {geometric_start:.2f}-{geometric_end:.2f}, "
                               f"adjusted: {adjusted_start:.2f}-{adjusted_end:.2f}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to calibrate MultiLineString route {route['id']}: {e}")
            return False
    
    def validate_calibration(self) -> bool:
        """
        Validate the calibration results including ratio adjustments
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Check if all routes were calibrated
                cursor.execute("SELECT COUNT(*) FROM client.route_client;")
                original_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(DISTINCT axe) FROM client.troncon_client;")
                calibrated_routes_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM client.troncon_client;")
                calibrated_segments_count = cursor.fetchone()[0]
                
                logger.info(f"Original routes: {original_count}")
                logger.info(f"Calibrated routes: {calibrated_routes_count}")
                logger.info(f"Calibrated segments: {calibrated_segments_count}")
                
                # Validate geometry types
                cursor.execute("""
                SELECT COUNT(*) FROM client.troncon_client 
                WHERE ST_GeometryType(geom_calib) != 'ST_LineStringM';
                """)
                
                invalid_geom_count = cursor.fetchone()[0]
                
                if invalid_geom_count > 0:
                    logger.warning(f"Found {invalid_geom_count} geometries that are not LineStringM")
                    return False
                
                # Validate ratio adjustments - check that final cumulf matches longueur for each route
                cursor.execute("""
                WITH route_max_cumulf AS (
                    SELECT 
                        tc.axe,
                        MAX(tc.cumulf) as max_cumulf
                    FROM client.troncon_client tc
                    GROUP BY tc.axe
                )
                SELECT 
                    COUNT(*) as total_routes,
                    COUNT(CASE WHEN ABS(rmc.max_cumulf - rc.longueur) < 0.01 THEN 1 END) as correctly_terminated_routes
                FROM route_max_cumulf rmc
                JOIN client.route_client rc ON rmc.axe = rc.axe;
                """)
                
                route_stats = cursor.fetchone()
                logger.info(f"Routes validation: {route_stats[0]} total, {route_stats[1]} correctly terminated")
                
                # Check measure values
                cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN cumuld = 0 AND id_tronc LIKE '%_1' THEN 1 END) as routes_start_zero,
                    AVG(cumulf) as avg_end_measure
                FROM client.troncon_client
                WHERE id_tronc LIKE '%_1';  -- Only first segments
                """)
                
                stats = cursor.fetchone()
                logger.info(f"Validation - Total first segments: {stats[0]}, Start at 0: {stats[1]}, "
                          f"Avg end measure: {stats[2]:.2f}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Validation failed: {e}")
            return False
    
    def get_calibration_summary(self) -> Optional[dict]:
        """
        Get summary statistics of the calibration process including ratio adjustments
        
        Returns:
            dict: Summary statistics or None if error
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_segments,
                    COUNT(DISTINCT axe) as total_routes,
                    COUNT(DISTINCT SUBSTRING(id_tronc FROM '^(.*)_[0-9]+$')) as total_routes_from_tronc,
                    MIN(cumuld) as min_start_measure,
                    MAX(cumulf) as max_end_measure,
                    AVG(cumulf - cumuld) as avg_segment_length
                FROM client.troncon_client;
                """)
                
                summary = dict(cursor.fetchone())
                
                # Add validation of ratio correctness
                cursor.execute("""
                WITH route_validation AS (
                    SELECT 
                        tc.axe,
                        MAX(tc.cumulf) as final_cumulf,
                        rc.longueur as original_longueur,
                        ABS(MAX(tc.cumulf) - rc.longueur) as difference
                    FROM client.troncon_client tc
                    JOIN client.route_client rc ON tc.axe = rc.axe
                    GROUP BY tc.axe, rc.longueur
                )
                SELECT 
                    COUNT(*) as total_routes_checked,
                    COUNT(CASE WHEN difference < 0.01 THEN 1 END) as correctly_calibrated_routes,
                    AVG(difference) as avg_difference
                FROM route_validation;
                """)
                
                validation = cursor.fetchone()
                summary['correctly_calibrated_routes'] = validation['correctly_calibrated_routes']
                summary['total_routes_checked'] = validation['total_routes_checked']
                summary['avg_difference'] = validation['avg_difference']
                
                return summary
                
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
        logger.info("Starting route calibration with ratio adjustment...")
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
            logger.info(f"  Total routes: {summary['total_routes']}")
            logger.info(f"  Average segment length: {summary['avg_segment_length']:.2f}")
            logger.info(f"  Measure range: {summary['min_start_measure']:.2f} - {summary['max_end_measure']:.2f}")
            logger.info(f"  Correctly calibrated routes: {summary['correctly_calibrated_routes']}/{summary['total_routes_checked']}")
            logger.info(f"  Average calibration difference: {summary['avg_difference']:.6f}")
        
        logger.info("Road network calibration with ratio adjustment completed successfully!")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        calibrator.disconnect()


if __name__ == "__main__":
    main()