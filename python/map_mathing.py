#!/usr/bin/env python3
"""
Map-matching program for road images to linear referencing system
Matches images from public.image to reference roads in client.troncon_client
"""

import psycopg2
import psycopg2.extras
import math
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    """Result of map-matching operation"""
    id_tronc: int
    axe: str
    prj_quality: float
    cumuld: float
    geom_prj: str
    ln_prj: str
    seg_prj: str
    d_angle_seg: float

class MapMatcher:
    """Main class for map-matching operations"""
    
    def __init__(self, db_config: Dict[str, str], buffer_radius: float = 50.0, max_angle_diff: float = 45.0):
        """
        Initialize MapMatcher
        
        Args:
            db_config: Database connection parameters
            buffer_radius: Buffer radius in meters for segment matching
            max_angle_diff: Maximum angle difference in degrees for segment matching
        """
        self.db_config = db_config
        self.buffer_radius = buffer_radius
        self.max_angle_diff = max_angle_diff
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def get_session_segments(self) -> List[Dict]:
        """Get all session segments that need processing"""
        query = """
        SELECT DISTINCT 
            s.id as session_id,
            s.geom_calib,
            ST_Length(s.geom) as session_length
        FROM public.session s
        JOIN public.image i ON i.session_id = s.id
        WHERE s.geom_calib IS NOT NULL
        AND s.state != 'processed'  -- Adjust based on your state management
        ORDER BY s.acquisition_date DESC
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()
    
    def get_candidate_troncons(self, session_geom: str) -> List[Dict]:
        """Get candidate troncon segments that intersect with session buffer"""
        query = """
        SELECT 
            tc.id,
            tc.axe,
            tc.id_tronc,
            tc.geom_calib,
            tc.cumuld,
            tc.cumulf,
            ST_Length(tc.geom_calib) as troncon_length,
            ST_Distance(tc.geom_calib, ST_GeomFromText(%s, 2154)) as min_distance
        FROM client.troncon_client tc
        WHERE ST_DWithin(tc.geom_calib, ST_GeomFromText(%s, 2154), %s)
        ORDER BY min_distance
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (session_geom, session_geom, self.buffer_radius))
            return cur.fetchall()
    
    def calculate_segment_angle_difference(self, session_geom: str, troncon_geom: str) -> float:
        """Calculate angle difference between two line segments"""
        query = """
        SELECT 
            ABS(
                degrees(ST_Azimuth(ST_StartPoint(%s), ST_EndPoint(%s))) - 
                degrees(ST_Azimuth(ST_StartPoint(%s), ST_EndPoint(%s)))
            ) as angle_diff
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (session_geom, session_geom, troncon_geom, troncon_geom))
            result = cur.fetchone()
            angle_diff = result[0] if result[0] else 180
            
            # Normalize angle difference to 0-180 range
            if angle_diff > 180:
                angle_diff = 360 - angle_diff
                
            return angle_diff
    
    def find_matching_troncons(self, session_id: str, session_geom: str) -> List[Dict]:
        """Find troncon segments that match the session segment"""
        candidates = self.get_candidate_troncons(session_geom)
        matching_troncons = []
        
        logger.info(f"Found {len(candidates)} candidate troncons for session {session_id}")
        
        for candidate in candidates:
            # Calculate angle difference
            angle_diff = self.calculate_segment_angle_difference(session_geom, candidate['geom_calib'])
            
            # Check if segments are not perpendicular
            if angle_diff <= self.max_angle_diff:
                candidate['angle_diff'] = angle_diff
                matching_troncons.append(candidate)
                logger.debug(f"Troncon {candidate['id_tronc']} matches with angle diff: {angle_diff:.2f}°")
        
        logger.info(f"Found {len(matching_troncons)} matching troncons for session {session_id}")
        return matching_troncons
    
    def get_session_images(self, session_id: str) -> List[Dict]:
        """Get all images for a specific session"""
        query = """
        SELECT 
            id,
            geom,
            geom_calib,
            cumuld_session,
            session_id,
            index
        FROM public.image 
        WHERE session_id = %s
        AND geom IS NOT NULL
        ORDER BY index
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (session_id,))
            return cur.fetchall()
    
    def find_best_match_for_image(self, image: Dict, candidate_troncons: List[Dict]) -> Optional[MatchResult]:
        """Find the best matching troncon for a single image"""
        if not candidate_troncons:
            return None
        
        best_match = None
        best_score = float('inf')
        
        image_geom = image['geom']
        
        for troncon in candidate_troncons:
            # Calculate projection quality (distance from image to troncon)
            distance_query = """
            SELECT ST_Distance(
                ST_GeomFromText(%s, 2154),
                %s
            ) as distance
            """
            
            with self.conn.cursor() as cur:
                cur.execute(distance_query, (image_geom, troncon['geom_calib']))
                distance = cur.fetchone()[0]
            
            # Calculate projected point and cumulative distance
            projection_query = """
            SELECT 
                ST_LineLocatePoint(%s, ST_GeomFromText(%s, 2154)) as line_position,
                ST_AsText(ST_ClosestPoint(%s, ST_GeomFromText(%s, 2154))) as proj_point,
                ST_AsText(ST_LineSubstring(%s, 
                    GREATEST(0, ST_LineLocatePoint(%s, ST_GeomFromText(%s, 2154)) - 0.01),
                    LEAST(1, ST_LineLocatePoint(%s, ST_GeomFromText(%s, 2154)) + 0.01)
                )) as ln_prj,
                ST_AsText(ST_LineSubstring(%s, 
                    GREATEST(0, ST_LineLocatePoint(%s, ST_GeomFromText(%s, 2154)) - 0.01),
                    LEAST(1, ST_LineLocatePoint(%s, ST_GeomFromText(%s, 2154)) + 0.01)
                )) as seg_prj
            """
            
            with self.conn.cursor() as cur:
                cur.execute(projection_query, (
                    troncon['geom_calib'], image_geom,
                    troncon['geom_calib'], image_geom,
                    troncon['geom_calib'], troncon['geom_calib'], image_geom,
                    troncon['geom_calib'], image_geom,
                    troncon['geom_calib'], troncon['geom_calib'], image_geom,
                    troncon['geom_calib'], image_geom
                ))
                proj_result = cur.fetchone()
            
            if proj_result and proj_result[0] is not None:
                line_position = proj_result[0]
                proj_point = proj_result[1]
                ln_prj = proj_result[2]
                seg_prj = proj_result[3]
                
                # Calculate cumulative distance on troncon
                troncon_length = troncon['cumulf'] - troncon['cumuld']
                cumuld_on_troncon = troncon['cumuld'] + (line_position * troncon_length)
                
                # Calculate angle between session segment and troncon segment
                d_angle_seg = troncon.get('angle_diff', 0)
                
                # Combine distance and angle for scoring (you can adjust weights)
                score = distance + (d_angle_seg / 90.0) * 10  # Weight angle component
                
                if score < best_score:
                    best_score = score
                    best_match = MatchResult(
                        id_tronc=troncon['id_tronc'],
                        axe=troncon['axe'],
                        prj_quality=distance,
                        cumuld=cumuld_on_troncon,
                        geom_prj=proj_point,
                        ln_prj=ln_prj,
                        seg_prj=seg_prj,
                        d_angle_seg=d_angle_seg
                    )
        
        return best_match
    
    def update_image_match(self, image_id: str, match: MatchResult):
        """Update image with map-matching results"""
        update_query = """
        UPDATE public.image 
        SET 
            id_tronc = %s,
            axe = %s,
            prj_quality = %s,
            cumuld_client = %s,
            geom_prj = ST_GeomFromText(%s, 2154),
            ln_prj = ST_GeomFromText(%s, 2154),
            seg_prj = ST_GeomFromText(%s, 2154),
            d_angle_seg = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        with self.conn.cursor() as cur:
            cur.execute(update_query, (
                match.id_tronc,
                match.axe,
                match.prj_quality,
                match.cumuld,
                match.geom_prj,
                match.ln_prj,
                match.seg_prj,
                match.d_angle_seg,
                image_id
            ))
    
    def process_session(self, session: Dict) -> int:
        """Process a single session for map-matching"""
        session_id = session['id']
        session_geom = session['geom_calib']
        
        logger.info(f"Processing session {session_id}")
        
        # Find matching troncon segments
        matching_troncons = self.find_matching_troncons(session_id, session_geom)
        
        if not matching_troncons:
            logger.warning(f"No matching troncons found for session {session_id}")
            return 0
        
        # Get all images for this session
        images = self.get_session_images(session_id)
        logger.info(f"Processing {len(images)} images for session {session_id}")
        
        processed_count = 0
        
        for image in images:
            try:
                # Find best match for this image
                best_match = self.find_best_match_for_image(image, matching_troncons)
                
                if best_match:
                    # Update the image with matching results
                    self.update_image_match(image['id'], best_match)
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count} images for session {session_id}")
                        self.conn.commit()  # Commit periodically
                else:
                    logger.warning(f"No match found for image {image['id']}")
                    
            except Exception as e:
                logger.error(f"Error processing image {image['id']}: {e}")
                continue
        
        self.conn.commit()  # Final commit for this session
        logger.info(f"Completed session {session_id}: {processed_count} images processed")
        return processed_count
    
    def run(self) -> Dict[str, int]:
        """Run the complete map-matching process"""
        logger.info("Starting map-matching process")
        
        try:
            self.connect()
            
            # Get all sessions to process
            sessions = self.get_session_segments()
            logger.info(f"Found {len(sessions)} sessions to process")
            
            total_processed = 0
            total_sessions = 0
            
            for session in sessions:
                try:
                    processed_count = self.process_session(session)
                    total_processed += processed_count
                    total_sessions += 1
                    
                except Exception as e:
                    logger.error(f"Error processing session {session['id']}: {e}")
                    self.conn.rollback()
                    continue
            
            results = {
                'total_sessions_processed': total_sessions,
                'total_images_processed': total_processed,
                'average_images_per_session': total_processed / total_sessions if total_sessions > 0 else 0
            }
            
            logger.info(f"Map-matching completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Fatal error in map-matching process: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()


def main():
    """Main function to run the map-matching program"""
    
    # Database configuration
    db_config = {
        'host': 'localhost',          # Update with your host
        'database': 'cd08_demo',     # Update with your database name
        'user': 'diagway',           # Update with your username
        'password': 'diagway',       # Update with your password
        'port': 5433                  # Update with your port
    }
    
    # Map-matching parameters
    buffer_radius = 50.0  # meters
    max_angle_diff = 45.0  # degrees
    
    # Create and run map matcher
    matcher = MapMatcher(db_config, buffer_radius, max_angle_diff)
    
    try:
        results = matcher.run()
        print(f"Map-matching completed successfully!")
        print(f"Sessions processed: {results['total_sessions_processed']}")
        print(f"Images processed: {results['total_images_processed']}")
        print(f"Average images per session: {results['average_images_per_session']:.1f}")
        
    except Exception as e:
        print(f"Map-matching failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())