#!/usr/bin/env python3
"""
Map-matching program for road images to linear referencing system
Based on the SQL algorithm provided - implements the exact same logic
"""

import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MapMatcher:
    """Main class for map-matching operations following the SQL algorithm"""
    
    def __init__(self, db_config: Dict[str, str], buffer_radius: float = 24.0, min_segment_length: float = 50.0):
        """
        Initialize MapMatcher
        
        Args:
            db_config: Database connection parameters
            buffer_radius: Buffer radius in meters for segment matching (default 24m as in SQL)
            min_segment_length: Minimum valid projected segment length in meters (default 50m)
        """
        self.db_config = db_config
        self.buffer_radius = buffer_radius
        self.min_segment_length = min_segment_length
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            # Set environment variable for PostgreSQL client encoding
            import os
            os.environ['PGCLIENTENCODING'] = 'UTF8'
            
            # Add client_encoding to handle PostgreSQL 17 encoding issues
            conn_params = self.db_config.copy()
            conn_params['client_encoding'] = 'UTF8'
            
            self.conn = psycopg2.connect(**conn_params)
            
            # Set connection to autocommit temporarily to change encoding
            old_autocommit = self.conn.autocommit
            self.conn.autocommit = True
            
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SET CLIENT_ENCODING TO 'UTF8'")
            finally:
                # Restore original autocommit setting
                self.conn.autocommit = old_autocommit
            
            logger.info("Database connection established with UTF-8 encoding")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            # Try alternative connection with LATIN1 encoding
            try:
                logger.warning("Retrying connection with LATIN1 encoding...")
                conn_params = self.db_config.copy()
                conn_params['client_encoding'] = 'LATIN1'
                self.conn = psycopg2.connect(**conn_params)
                
                # Set connection to autocommit temporarily to change encoding
                self.conn.autocommit = True
                
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("SET CLIENT_ENCODING TO 'UTF8'")
                finally:
                    self.conn.autocommit = False
                
                logger.info("Database connection established with LATIN1->UTF8 encoding")
                
            except Exception as e2:
                logger.error(f"Failed to connect with alternative encoding: {e2}")
                raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def step1_update_seg_ss(self):
        """Step 1: Update seg_ss for all images (±1m segments from session)"""
        logger.info("Step 1: Updating seg_ss for all images")
        
        query = """
        UPDATE public.image t1 
        SET seg_ss = ST_Force2D(
            st_geometryN(
                ST_LocateBetween(t2.geom_calib, t1.cumuld_session-1.0, t1.cumuld_session+1.0), 1
            )
        )
        FROM public.session t2 
        WHERE t1.session_id = t2.id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            affected_rows = cur.rowcount
            logger.info(f"Updated seg_ss for {affected_rows} images")
        
        self.conn.commit()

    def step2_create_schema_and_projection_paire(self):
        """Step 2: Create projection_paire table with session-troncon matching"""
        logger.info("Step 2: Creating projection_paire table")
        
        # Create schema
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS traitement")
            cur.execute("DROP TABLE IF EXISTS traitement.projection_paire")
        
        # Create projection_paire table
        query = f"""
        CREATE TABLE traitement.projection_paire AS
        SELECT 
            t1.id as session_id, 
            t2.id_tronc,
            ST_Length(ST_Intersection(ST_Buffer(t2.geom_calib,{self.buffer_radius},'endcap=flat join=bevel'),t1.geom)) as len_ss_on_client, 
            ST_Length(t1.geom) as len_ss,
            ST_Length(ST_Intersection(t2.geom_calib,ST_Buffer(t1.geom,{self.buffer_radius},'endcap=flat join=bevel'))) as len_client_sur_ss,
            ST_Length(t2.geom_calib) as len_client,
            ST_Intersection(ST_Buffer(t2.geom_calib,{self.buffer_radius},'endcap=flat join=bevel'),t1.geom) as geom_ss_sur_client,
            ST_Intersection(t2.geom_calib,ST_Buffer(t1.geom,{self.buffer_radius},'endcap=flat join=bevel')) as geom_client_sur_session,
            degrees(st_angle(
                ST_Intersection(ST_Buffer(t2.geom_calib,{self.buffer_radius},'endcap=flat join=bevel'),t1.geom),
                ST_Intersection(t2.geom_calib,ST_Buffer(t1.geom,{self.buffer_radius},'endcap=flat join=bevel'))
            )) as angle_client_ss,
            ST_Intersection(ST_Buffer(t2.geom_calib,{self.buffer_radius},'endcap=flat join=bevel'),ST_Buffer(t1.geom,{self.buffer_radius})) as geom_intersect
        FROM public.session t1 
        JOIN client.troncon_client t2 ON ST_Distance(t2.geom_calib,t1.geom) < {self.buffer_radius + 1}
        AND (
            ST_Intersects(ST_Buffer(t2.geom_calib,{self.buffer_radius},'endcap=flat join=bevel'), t1.geom)
            OR 
            ST_Intersects(t2.geom_calib, ST_Buffer(t1.geom,{self.buffer_radius},'endcap=flat join=bevel'))
        )
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            
            # Add columns and constraints
            cur.execute("ALTER TABLE traitement.projection_paire ADD COLUMN id SERIAL")
            cur.execute("ALTER TABLE traitement.projection_paire ADD COLUMN is_paire BOOLEAN")
            cur.execute("ALTER TABLE traitement.projection_paire ADD COLUMN d_angle NUMERIC")
            cur.execute("UPDATE traitement.projection_paire SET d_angle = degrees(ST_Angle(geom_ss_sur_client,geom_client_sur_session))")
            cur.execute("ALTER TABLE traitement.projection_paire ADD PRIMARY KEY (id)")
            
            logger.info(f"Created projection_paire table with {cur.rowcount} records")
        
        self.conn.commit()

    def step3_determine_valid_pairs(self):
        """Step 3: Determine which session-troncon pairs are valid"""
        logger.info("Step 3: Determining valid session-troncon pairs")
        
        with self.conn.cursor() as cur:
            # Initialize is_paire to false
            cur.execute("UPDATE traitement.projection_paire SET is_paire = false")
            
            # Normalize angles
            cur.execute("""
                UPDATE traitement.projection_paire 
                SET d_angle = CASE 
                    WHEN abs(d_angle) BETWEEN 0 AND 180 THEN d_angle
                    WHEN abs(d_angle) BETWEEN 180 AND 360 THEN d_angle - 180
                END
            """)
            
            # Handle null angles
            cur.execute("UPDATE traitement.projection_paire SET d_angle = 0.0 WHERE d_angle IS NULL")
            
            # Set valid pairs based on criteria
            cur.execute(f"""
                UPDATE traitement.projection_paire 
                SET is_paire = true 
                WHERE NOT (
                    (CASE 
                        WHEN abs(d_angle) BETWEEN 0 AND 180 THEN d_angle
                        WHEN abs(d_angle) BETWEEN 180 AND 360 THEN d_angle - 180
                    END BETWEEN 45 AND 135 OR (len_client_sur_ss < {self.min_segment_length} OR len_ss_on_client < {self.min_segment_length}))
                )
            """)
            
            # Count valid pairs
            cur.execute("SELECT COUNT(*) FROM traitement.projection_paire WHERE is_paire = true")
            valid_pairs = cur.fetchone()[0]
            logger.info(f"Found {valid_pairs} valid session-troncon pairs")
        
        self.conn.commit()

    def step4_reset_image_projections(self):
        """Step 4: Reset image projection fields"""
        logger.info("Step 4: Resetting image projection fields")
        
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE public.image 
                SET id_tronc = NULL, axe = NULL, prj_quality = NULL, cumuld = NULL
            """)
            affected_rows = cur.rowcount
            logger.info(f"Reset projection fields for {affected_rows} images")
        
        self.conn.commit()

    def step5_create_projection_img_dist(self):
        """Step 5: Create image-distance table for valid pairs"""
        logger.info("Step 5: Creating projection_img_dist table")
        
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS traitement.projection_img_dist")
            
            # Use a smaller buffer for images (5m or 1/5 of the main buffer)
            image_buffer = min(5.0, self.buffer_radius / 5.0)
            
            query = f"""
            CREATE TABLE traitement.projection_img_dist AS     
            SELECT 
                t1.id, 
                t2.id_tronc,
                st_distance(t1.geom, t2.geom_client_sur_session) as dist 
            FROM public.image t1
            JOIN traitement.projection_paire t2 ON 
                t2.is_paire IS TRUE 
                AND t1.session_id = t2.session_id 
                AND ST_Within(t1.geom, ST_Buffer(t2.geom_ss_sur_client, {image_buffer}, 'endcap=round join=bevel'))
            """
            
            cur.execute(query)
            records = cur.rowcount
            logger.info(f"Created projection_img_dist table with {records} image-troncon distances (image buffer: {image_buffer}m)")
        
        self.conn.commit()

    def step6_assign_best_troncons(self):
        """Step 6: Assign best matching troncon to each image"""
        logger.info("Step 6: Assigning best matching troncons to images")
        
        with self.conn.cursor() as cur:
            query = """
            UPDATE public.image t1 
            SET id_tronc = r1.id_tronc, prj_quality = r1.min_dist
            FROM (
                SELECT t1.id, t1.id_tronc, r1.min_dist 
                FROM traitement.projection_img_dist t1
                JOIN (
                    SELECT id, min(dist) as min_dist 
                    FROM traitement.projection_img_dist 
                    GROUP BY id
                ) r1 ON t1.id = r1.id AND t1.dist = r1.min_dist
            ) r1
            WHERE t1.id = r1.id
            """
            
            cur.execute(query)
            affected_rows = cur.rowcount
            logger.info(f"Assigned troncons to {affected_rows} images")
        
        self.conn.commit()

    def step7_calculate_projections(self):
        """Step 7: Calculate geometric projections and cumulative distances"""
        logger.info("Step 7: Calculating geometric projections")
        
        with self.conn.cursor() as cur:
            try:
                # Calculate cumuld and geom_prj
                logger.info("Step 7a: Calculating cumuld and geom_prj")
                cur.execute("""
                    UPDATE public.image point 
                    SET 
                        cumuld = ST_M(ST_LineInterpolatePoint(line.geom_calib, ST_LineLocatePoint(line.geom_calib, point.geom))),
                        geom_prj = ST_Force2D(ST_LineInterpolatePoint(line.geom_calib, ST_LineLocatePoint(line.geom_calib, point.geom)))
                    FROM client.troncon_client line
                    WHERE point.id_tronc = line.id_tronc AND point.id_tronc IS NOT NULL
                """)
                logger.info(f"Updated cumuld and geom_prj for {cur.rowcount} images")
                
                # Update ln_prj
                logger.info("Step 7b: Calculating ln_prj")
                cur.execute("UPDATE public.image SET ln_prj = ST_Force2D(ST_MakeLine(geom, geom_prj)) WHERE geom_prj IS NOT NULL")
                logger.info(f"Updated ln_prj for {cur.rowcount} images")
                
                # Update seg_prj
                logger.info("Step 7c: Calculating seg_prj")
                cur.execute("""
                    UPDATE public.image t1 
                    SET seg_prj = ST_Force2D(
                        st_geometryN(
                            ST_LocateBetween(t2.geom_calib, t1.cumuld-1.0, t1.cumuld+1.0), 1
                        )
                    )
                    FROM client.troncon_client t2 
                    WHERE t1.id_tronc = t2.id_tronc AND t1.cumuld IS NOT NULL
                """)
                logger.info(f"Updated seg_prj for {cur.rowcount} images")
                
                # Calculate d_angle_seg
                logger.info("Step 7d: Calculating d_angle_seg")
                cur.execute("UPDATE public.image SET d_angle_seg = degrees(ST_Angle(seg_ss, seg_prj)) WHERE seg_ss IS NOT NULL AND seg_prj IS NOT NULL")
                logger.info(f"Updated d_angle_seg for {cur.rowcount} images")
                
                # Normalize angles
                logger.info("Step 7e: Normalizing angles")
                cur.execute("""
                    UPDATE public.image 
                    SET d_angle_seg = CASE 
                        WHEN abs(d_angle_seg) BETWEEN 0 AND 180 THEN d_angle_seg
                        WHEN abs(d_angle_seg) BETWEEN 180 AND 360 THEN d_angle_seg - 180
                    END
                    WHERE d_angle_seg IS NOT NULL
                """)
                logger.info(f"Normalized angles for {cur.rowcount} images")
                
                logger.info("Completed initial projections and angles")
                
            except Exception as e:
                logger.error(f"Error in step 7: {e}")
                raise
        
        self.conn.commit()

    def step8_handle_perpendicular_cases(self):
        """Step 8: Re-project perpendicular cases (45°-135°)"""
        logger.info("Step 8: Handling perpendicular projection cases")
        
        with self.conn.cursor() as cur:
            # Add columns to projection_img_dist for perpendicular handling
            cur.execute("ALTER TABLE traitement.projection_img_dist ADD COLUMN IF NOT EXISTS gid SERIAL")
            cur.execute("ALTER TABLE traitement.projection_img_dist ADD COLUMN IF NOT EXISTS d_angle_seg NUMERIC")
            cur.execute("UPDATE traitement.projection_img_dist SET d_angle_seg = 0.0")
            
            # Update angles in projection_img_dist
            cur.execute("""
                UPDATE traitement.projection_img_dist t1 
                SET d_angle_seg = t2.d_angle_seg 
                FROM public.image t2
                WHERE t1.id = t2.id AND t1.id_tronc = t2.id_tronc
            """)
            
            # Reset perpendicular projections
            cur.execute("""
                UPDATE public.image 
                SET id_tronc = NULL, prj_quality = NULL, cumuld = NULL, geom_prj = NULL, seg_prj = NULL
                WHERE d_angle_seg BETWEEN 45.0 AND 135.0
            """)
            
            # Re-assign best non-perpendicular matches
            cur.execute("""
                UPDATE public.image t1 
                SET id_tronc = r1.id_tronc, prj_quality = r1.min_dist
                FROM (
                    SELECT t1.id, t1.id_tronc, r1.min_dist 
                    FROM traitement.projection_img_dist t1
                    JOIN (
                        SELECT id, min(dist) as min_dist 
                        FROM traitement.projection_img_dist 
                        WHERE NOT d_angle_seg BETWEEN 45.0 AND 135.0 
                        GROUP BY id
                    ) r1 ON t1.id = r1.id AND t1.dist = r1.min_dist
                ) r1
                WHERE t1.id = r1.id AND t1.id_tronc IS NULL
            """)
            
            logger.info("Re-projected perpendicular cases")
        
        self.conn.commit()

    def step9_final_projections(self):
        """Step 9: Calculate final projections for re-assigned images"""
        logger.info("Step 9: Calculating final projections")
        
        with self.conn.cursor() as cur:
            try:
                # Final projection calculations for perpendicular cases
                logger.info("Step 9a: Final projections for perpendicular cases")
                cur.execute("""
                    UPDATE public.image point 
                    SET 
                        cumuld = ST_M(ST_LineInterpolatePoint(line.geom_calib, ST_LineLocatePoint(line.geom_calib, point.geom))),
                        geom_prj = ST_Force2D(ST_LineInterpolatePoint(line.geom_calib, ST_LineLocatePoint(line.geom_calib, point.geom)))
                    FROM client.troncon_client line
                    WHERE (point.d_angle_seg BETWEEN 45.0 AND 135.0) AND point.id_tronc = line.id_tronc AND point.id_tronc IS NOT NULL
                """)
                logger.info(f"Updated projections for {cur.rowcount} perpendicular cases")
                
                # Update ln_prj for perpendicular cases
                logger.info("Step 9b: Update ln_prj for perpendicular cases")
                cur.execute("UPDATE public.image SET ln_prj = ST_Force2D(ST_MakeLine(geom, geom_prj)) WHERE d_angle_seg BETWEEN 45.0 AND 135.0 AND geom_prj IS NOT NULL")
                logger.info(f"Updated ln_prj for {cur.rowcount} perpendicular cases")
                
                # Update seg_prj for perpendicular cases
                logger.info("Step 9c: Update seg_prj for perpendicular cases")
                cur.execute("""
                    UPDATE public.image t1 
                    SET seg_prj = ST_Force2D(
                        st_geometryN(
                            ST_LocateBetween(t2.geom_calib, t1.cumuld-1.0, t1.cumuld+1.0), 1
                        )
                    )
                    FROM client.troncon_client t2 
                    WHERE (t1.d_angle_seg BETWEEN 45.0 AND 135.0) AND t1.id_tronc = t2.id_tronc AND t1.cumuld IS NOT NULL
                """)
                logger.info(f"Updated seg_prj for {cur.rowcount} perpendicular cases")
                
                # Update angles for perpendicular cases
                logger.info("Step 9d: Update angles for perpendicular cases")
                cur.execute("UPDATE public.image SET d_angle_seg = degrees(ST_Angle(seg_ss, seg_prj)) WHERE d_angle_seg BETWEEN 45.0 AND 135.0 AND seg_ss IS NOT NULL AND seg_prj IS NOT NULL")
                logger.info(f"Updated angles for {cur.rowcount} perpendicular cases")
                
                # Final update for all seg_prj
                logger.info("Step 9e: Final update for all seg_prj")
                cur.execute("""
                    UPDATE public.image t1 
                    SET seg_prj = ST_Force2D(
                        st_geometryN(
                            ST_LocateBetween(t2.geom_calib, t1.cumuld-1.0, t1.cumuld+1.0), 1
                        )
                    )
                    FROM client.troncon_client t2 
                    WHERE t1.id_tronc = t2.id_tronc AND t1.cumuld IS NOT NULL
                """)
                logger.info(f"Final seg_prj update for {cur.rowcount} images")
                
                # Final angle calculation and normalization
                logger.info("Step 9f: Final angle calculations")
                cur.execute("UPDATE public.image SET d_angle_seg = degrees(ST_Angle(seg_ss, seg_prj)) WHERE seg_ss IS NOT NULL AND seg_prj IS NOT NULL")
                cur.execute("""
                    UPDATE public.image 
                    SET d_angle_seg = CASE 
                        WHEN abs(d_angle_seg) BETWEEN 0 AND 180 THEN d_angle_seg
                        WHEN abs(d_angle_seg) BETWEEN 180 AND 360 THEN d_angle_seg - 180
                    END
                    WHERE d_angle_seg IS NOT NULL
                """)
                logger.info(f"Final angle normalization for {cur.rowcount} images")
                
                logger.info("Completed final projections")
                
            except Exception as e:
                logger.error(f"Error in step 9: {e}")
                raise
        
        self.conn.commit()

    def step10_update_axe_values(self):
        """Step 10: Update axe values from troncon_client"""
        logger.info("Step 10: Updating axe values")
        
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE public.image t1 
                SET axe = t2.axe
                FROM client.troncon_client t2 
                WHERE t1.id_tronc = t2.id_tronc AND t1.axe IS NULL
            """)
            affected_rows = cur.rowcount
            logger.info(f"Updated axe values for {affected_rows} images")
        
        self.conn.commit()

    def get_statistics(self) -> Dict[str, int]:
        """Get processing statistics"""
        with self.conn.cursor() as cur:
            # Total images
            cur.execute("SELECT COUNT(*) FROM public.image")
            total_images = cur.fetchone()[0]
            
            # Matched images
            cur.execute("SELECT COUNT(*) FROM public.image WHERE id_tronc IS NOT NULL")
            matched_images = cur.fetchone()[0]
            
            # Valid pairs
            cur.execute("SELECT COUNT(*) FROM traitement.projection_paire WHERE is_paire = true")
            valid_pairs = cur.fetchone()[0]
            
            # Unique sessions processed
            cur.execute("SELECT COUNT(DISTINCT session_id) FROM traitement.projection_paire WHERE is_paire = true")
            sessions_processed = cur.fetchone()[0]
            
            return {
                'total_images': total_images,
                'matched_images': matched_images,
                'match_rate_percent': round((matched_images / total_images * 100) if total_images > 0 else 0, 2),
                'valid_pairs': valid_pairs,
                'sessions_processed': sessions_processed
            }

    def run(self, perpendicular_iterations: int = 2) -> Dict[str, int]:
        """Run the complete map-matching process following SQL algorithm
        
        Args:
            perpendicular_iterations: Number of times to run step8 (default: 2)
        """
        logger.info(f"Starting map-matching process (SQL algorithm implementation)")
        logger.info(f"Database: {self.db_config['database']}")
        logger.info(f"Buffer radius: {self.buffer_radius}m")
        logger.info(f"Min segment length: {self.min_segment_length}m")
        logger.info(f"Perpendicular iterations: {perpendicular_iterations}")
        
        try:
            self.connect()
            
            # Execute all steps in sequence
            self.step1_update_seg_ss()
            self.step2_create_schema_and_projection_paire()
            self.step3_determine_valid_pairs()
            self.step4_reset_image_projections()
            self.step5_create_projection_img_dist()
            self.step6_assign_best_troncons()
            self.step7_calculate_projections()
            
            # Run step8 multiple times as specified
            for iteration in range(perpendicular_iterations):
                logger.info(f"Running perpendicular handling iteration {iteration + 1}/{perpendicular_iterations}")
                self.step8_handle_perpendicular_cases()
            
            self.step9_final_projections()
            self.step10_update_axe_values()
            
            # Get final statistics
            results = self.get_statistics()
            logger.info(f"Map-matching completed successfully: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Fatal error in map-matching process: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()


def main(perpendicular_iterations: int = 2, buffer_radius: float = 24.0, min_segment_length: float = 50.0, database: str = 'cd08_demo'):
    """Main function to run the map-matching program
    
    Args:
        perpendicular_iterations: Number of times to run perpendicular handling (default: 2)
        buffer_radius: Buffer radius in meters for segment matching (default: 24.0)
        min_segment_length: Minimum valid projected segment length in meters (default: 50.0)
        database: Database name (default: 'cd08_demo')
    """
    
    # Database configuration
    db_config = {
        'host': 'localhost',          # Update with your host
        'database': database,         # Configurable database name
        'user': 'diagway',           # Update with your username
        'password': 'diagway',       # Update with your password
        'port': 5433                  # Update with your port
    }
    
    # Create and run map matcher
    matcher = MapMatcher(db_config, buffer_radius, min_segment_length)
    
    try:
        results = matcher.run(perpendicular_iterations)
        print(f"\n=== Map-matching Results ===")
        print(f"Database: {database}")
        print(f"Buffer radius: {buffer_radius}m")
        print(f"Min segment length: {min_segment_length}m")
        print(f"Perpendicular iterations: {perpendicular_iterations}")
        print(f"Total images: {results['total_images']}")
        print(f"Successfully matched: {results['matched_images']}")
        print(f"Match rate: {results['match_rate_percent']}%")
        print(f"Valid session-troncon pairs: {results['valid_pairs']}")
        print(f"Sessions processed: {results['sessions_processed']}")
        print(f"Map-matching completed successfully!")
        
    except Exception as e:
        print(f"Map-matching failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(
        description='Map-matching program for road images to linear referencing system',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-i', '--perpendicular-iterations',
        type=int,
        default=2,
        help='Number of times to run perpendicular handling (step8)'
    )
    
    parser.add_argument(
        '-b', '--buffer-radius',
        type=float,
        default=14.0,
        help='Buffer radius in meters for segment matching'
    )
    
    parser.add_argument(
        '-s', '--min-segment-length',
        type=float,
        default=20.0,
        help='Minimum valid projected segment length in meters'
    )
    
    parser.add_argument(
        '-d', '--database',
        type=str,
        default='rcp_pontarlier',
        help='Database name to connect to'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate arguments
    if args.perpendicular_iterations < 1:
        parser.error("perpendicular_iterations must be >= 1")
    
    if args.buffer_radius <= 0:
        parser.error("buffer_radius must be > 0")
    
    if args.min_segment_length <= 0:
        parser.error("min_segment_length must be > 0")
    
    if not args.database.strip():
        parser.error("database name cannot be empty")
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    print(f"Starting map-matching with:")
    print(f"  - Database: {args.database}")
    print(f"  - Perpendicular iterations: {args.perpendicular_iterations}")
    print(f"  - Buffer radius: {args.buffer_radius}m")
    print(f"  - Min segment length: {args.min_segment_length}m")
    print(f"  - Verbose logging: {'enabled' if args.verbose else 'disabled'}")
    print()
    
    exit(main(args.perpendicular_iterations, args.buffer_radius, args.min_segment_length, args.database))