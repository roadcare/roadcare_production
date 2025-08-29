#!/usr/bin/env python3
"""
Road Network Linear Referencing Calibration Script
Transforms MultiLineString geometries to LineStringM (measured) for linear referencing
with ratio adjustment to match longueur field values

COMPLETE FINAL VERSION - Includes route-level calibrated geometries
Features:
- Creates client.troncon_client with individual calibrated segments
- Updates client.route_client with geom_calib column (grouped calibrated geometries)
- Ratio adjustment to match declared route lengths
- Support for MultiLineString with gaps
- Comprehensive validation and reporting
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
    """
    Complete road network calibration class for linear referencing
    Handles both segment-level and route-level calibrated geometries
    """
    
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
        Create the client.troncon_client table for individual calibrated segments
        
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
                    geom_calib GEOMETRY(LINESTRINGM, 2154),  -- Calibrated segment geometry
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
                
                # Create indexes on measure fields for performance
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_cumuld 
                ON client.troncon_client (cumuld);
                """)
                
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_cumulf 
                ON client.troncon_client (cumulf);
                """)
                
                # Create composite index for route and segment queries
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_troncon_client_axe_tronc 
                ON client.troncon_client (axe, id_tronc);
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
        Main calibration function: Transform MultiLineString to LineStringM with ratio adjustment
        Creates individual calibrated segments in client.troncon_client
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Clear existing calibrated data
                cursor.execute("DELETE FROM client.troncon_client;")
                logger.info("Cleared existing calibrated segment data")
                
                # Fetch routes to calibrate
                select_sql = """
                SELECT id, axe, longueur, geom
                FROM client.route_client
                ORDER BY axe;
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
                
                # Calculate ratio: declared_length / actual_geometric_length
                ratio = float(route['longueur']) / float(total_geometric_length) if total_geometric_length > 0 else 1.0
                
                logger.debug(f"Route {route['axe']}: declared_length={route['longueur']}, "
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
        """
        Calibrate a simple single-segment route with ratio adjustment
        
        Args:
            route (dict): Route data from database
            ratio (float): Adjustment ratio for measures
            
        Returns:
            bool: True if successful, False otherwise
        """
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
                    %(longueur)s as cumulf;  -- Adjusted end measure (equals declared length)
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
        
        Args:
            route (dict): Route data from database
            num_parts (int): Number of parts in MultiLineString
            ratio (float): Adjustment ratio for measures
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Calculate cumulative measures for each segment
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
                    
                    # For the last segment, ensure it ends exactly at declared length
                    if part_num == num_parts:
                        adjusted_end = float(route['longueur'])
                    
                    # Insert calibrated segment
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
                logger.info(f"Total calibrated segments: {calibrated_segments_count}")
                
                # Validate geometry types
                cursor.execute("""
                SELECT COUNT(*) FROM client.troncon_client 
                WHERE ST_GeometryType(geom_calib) != 'ST_LineStringM';
                """)
                
                invalid_geom_count = cursor.fetchone()[0]
                
                if invalid_geom_count > 0:
                    logger.warning(f"Found {invalid_geom_count} geometries that are not LineStringM")
                    return False
                
                # Validate that final cumulf matches longueur for each route
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
                    COUNT(*) as total_first_segments,
                    COUNT(CASE WHEN cumuld = 0 THEN 1 END) as segments_starting_at_zero,
                    AVG(cumulf) as avg_end_measure
                FROM client.troncon_client
                WHERE id_tronc LIKE '%_1';  -- Only first segments
                """)
                
                stats = cursor.fetchone()
                logger.info(f"First segments validation: {stats[0]} total, {stats[1]} start at 0, "
                          f"avg end measure: {stats[2]:.2f}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Validation failed: {e}")
            return False
    
    def update_route_client_with_calibrated_geom(self) -> bool:
        """
        Add geom_calib column to client.route_client and populate it with 
        grouped geometries from client.troncon_client
        
        This creates route-level calibrated geometries by collecting all segments
        for each route in the correct order.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Add the geom_calib column if it doesn't exist
                logger.info("Adding geom_calib column to client.route_client...")
                cursor.execute("""
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE client.route_client 
                        ADD COLUMN geom_calib GEOMETRY(MultiLineStringM, 2154);
                        RAISE NOTICE 'Column geom_calib added to client.route_client';
                    EXCEPTION
                        WHEN duplicate_column THEN 
                        RAISE NOTICE 'Column geom_calib already exists in client.route_client';
                    END;
                END $$;
                """)
                
                # Create spatial index on the new column
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_route_client_geom_calib 
                ON client.route_client USING GIST (geom_calib);
                """)
                
                # Update route_client with grouped geometries from troncon_client
                logger.info("Updating client.route_client with grouped calibrated geometries...")
                update_sql = """
                UPDATE client.route_client rc
                SET geom_calib = grouped_geom.multi_geom_calib
                FROM (
                    SELECT 
                        axe,
                        ST_Collect(
                            geom_calib 
                            ORDER BY CAST(SUBSTRING(id_tronc FROM '_([0-9]+)$') AS INTEGER)
                        )::GEOMETRY(MultiLineStringM, 2154) as multi_geom_calib
                    FROM client.troncon_client
                    GROUP BY axe
                ) grouped_geom
                WHERE rc.axe = grouped_geom.axe;
                """
                
                cursor.execute(update_sql)
                rows_updated = cursor.rowcount
                
                self.connection.commit()
                logger.info(f"Successfully updated {rows_updated} routes with grouped calibrated geometries")
                
                # Verify the update
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_routes,
                    COUNT(CASE WHEN geom_calib IS NOT NULL THEN 1 END) as routes_with_calib_geom,
                    COUNT(CASE WHEN ST_GeometryType(geom_calib) = 'ST_MultiLineStringM' THEN 1 END) as routes_with_correct_type
                FROM client.route_client;
                """)
                
                verification = cursor.fetchone()
                logger.info(f"Route-level verification - Total: {verification[0]}, "
                          f"With calibrated geom: {verification[1]}, "
                          f"Correct geometry type: {verification[2]}")
                
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Failed to update route_client with calibrated geometries: {e}")
            self.connection.rollback()
            return False
    
    def get_calibration_summary(self) -> Optional[dict]:
        """
        Get comprehensive summary statistics of the calibration process
        
        Returns:
            dict: Summary statistics or None if error
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Segment-level statistics
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_segments,
                    COUNT(DISTINCT axe) as total_routes,
                    MIN(cumuld) as min_start_measure,
                    MAX(cumulf) as max_end_measure,
                    AVG(cumulf - cumuld) as avg_segment_length,
                    MIN(CAST(SUBSTRING(id_tronc FROM '_([0-9]+)$') AS INTEGER)) as min_segment_num,
                    MAX(CAST(SUBSTRING(id_tronc FROM '_([0-9]+)$') AS INTEGER)) as max_segment_num
                FROM client.troncon_client;
                """)
                
                summary = dict(cursor.fetchone())
                
                # Calibration accuracy validation
                cursor.execute("""
                WITH route_validation AS (
                    SELECT 
                        tc.axe,
                        MAX(tc.cumulf) as final_cumulf,
                        rc.longueur as declared_longueur,
                        ABS(MAX(tc.cumulf) - rc.longueur) as difference
                    FROM client.troncon_client tc
                    JOIN client.route_client rc ON tc.axe = rc.axe
                    GROUP BY tc.axe, rc.longueur
                )
                SELECT 
                    COUNT(*) as total_routes_checked,
                    COUNT(CASE WHEN difference < 0.01 THEN 1 END) as correctly_calibrated_routes,
                    AVG(difference) as avg_calibration_difference,
                    MAX(difference) as max_calibration_difference
                FROM route_validation;
                """)
                
                validation = cursor.fetchone()
                summary.update(dict(validation))
                
                # Route-level calibrated geometry statistics
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_routes_in_route_client,
                    COUNT(CASE WHEN geom_calib IS NOT NULL THEN 1 END) as routes_with_grouped_geom,
                    COUNT(CASE WHEN ST_GeometryType(geom_calib) = 'ST_MultiLineStringM' THEN 1 END) as routes_with_correct_geom_type
                FROM client.route_client;
                """)
                
                route_stats = cursor.fetchone()
                summary.update(dict(route_stats))
                
                return summary
                
        except psycopg2.Error as e:
            logger.error(f"Failed to get summary: {e}")
            return None


def main():
    """
    Main execution function
    Orchestrates the complete calibration process
    """
    
    # Database configuration for cd08_demo
    db_config = {
        'host': 'localhost',
        'database': 'cd08_demo',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    # Initialize calibrator
    calibrator = RoadCalibrator(db_config)
    
    try:
        logger.info("🚀 Starting Road Network Linear Referencing Calibration")
        logger.info("=" * 60)
        
        # Step 1: Connect to database
        if not calibrator.connect():
            logger.error("Failed to connect to database. Exiting.")
            sys.exit(1)
        
        # Step 2: Create target table for segments
        logger.info("📋 Creating/verifying segment table...")
        if not calibrator.create_troncon_table():
            logger.error("Failed to create target table. Exiting.")
            sys.exit(1)
        
        # Step 3: Perform route calibration (create segments)
        logger.info("⚙️  Starting route calibration with ratio adjustment...")
        if not calibrator.calibrate_routes():
            logger.error("Calibration failed. Exiting.")
            sys.exit(1)
        
        # Step 4: Validate calibration results
        logger.info("✅ Validating calibration results...")
        if not calibrator.validate_calibration():
            logger.warning("Validation found issues, but calibration completed")
        
        # Step 5: Update route_client with grouped calibrated geometries
        logger.info("📊 Creating route-level calibrated geometries...")
        if not calibrator.update_route_client_with_calibrated_geom():
            logger.error("Failed to update route_client with calibrated geometries")
            sys.exit(1)
        
        # Step 6: Generate final summary report
        logger.info("📈 Generating final summary...")
        summary = calibrator.get_calibration_summary()
        
        if summary:
            logger.info("=" * 60)
            logger.info("🎉 FINAL CALIBRATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"📦 SEGMENT LEVEL (client.troncon_client):")
            logger.info(f"   • Total segments created: {summary['total_segments']}")
            logger.info(f"   • Routes processed: {summary['total_routes']}")
            logger.info(f"   • Segment range: {summary['min_segment_num']} to {summary['max_segment_num']}")
            logger.info(f"   • Average segment length: {summary['avg_segment_length']:.2f}m")
            logger.info(f"   • Measure range: {summary['min_start_measure']:.2f} to {summary['max_end_measure']:.2f}m")
            
            logger.info(f"📏 CALIBRATION ACCURACY:")
            logger.info(f"   • Correctly calibrated routes: {summary['correctly_calibrated_routes']}/{summary['total_routes_checked']}")
            logger.info(f"   • Average difference: {summary['avg_calibration_difference']:.6f}m")
            logger.info(f"   • Maximum difference: {summary['max_calibration_difference']:.6f}m")
            
            logger.info(f"🛣️  ROUTE LEVEL (client.route_client.geom_calib):")
            logger.info(f"   • Routes with grouped geometries: {summary['routes_with_grouped_geom']}/{summary['total_routes_in_route_client']}")
            logger.info(f"   • Correct geometry types: {summary['routes_with_correct_geom_type']}")
        
        logger.info("=" * 60)
        logger.info("🎉 SUCCESS! Road network calibration completed!")
        logger.info("📊 Both segment-level and route-level calibrations are ready")
        logger.info("🔧 Use client.troncon_client for detailed segment analysis")  
        logger.info("🗺️  Use client.route_client.geom_calib for route-level referencing")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        calibrator.disconnect()


if __name__ == "__main__":
    main()