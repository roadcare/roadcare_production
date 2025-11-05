import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
from datetime import datetime
import logging
from typing import List, Set, Tuple, Dict
from multiprocessing import Pool, cpu_count
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_axe_worker(args: Tuple) -> Set[str]:
    """
    Worker function to process a single axe in parallel.
    This runs in a separate process.
    
    Args:
        args: Tuple of (axe_name, records_data, distance_threshold)
        
    Returns:
        Set of IDs to mark as obsolete
    """
    axe_name, records_data, distance_threshold = args
    
    if len(records_data) < 2:
        return set()
    
    # Convert to structured numpy array for faster processing
    dtype = [
        ('id', 'U36'),
        ('session_id', 'U36'),
        ('cumuld', 'f8'),
        ('sens', 'U10'),
        ('index', 'i4'),
        ('captureDate', 'datetime64[s]'),
        ('note_globale', 'f8'),
        ('cumuld_session', 'f8')
    ]
    
    # Prepare data for numpy
    np_data = []
    for rec in records_data:
        np_data.append((
            str(rec[0]) if rec[0] else '',
            str(rec[1]) if rec[1] else '',
            float(rec[2]) if rec[2] is not None else np.nan,
            str(rec[3]) if rec[3] else '',
            int(rec[4]) if rec[4] is not None else -1,
            np.datetime64(rec[5]) if rec[5] is not None else np.datetime64('NaT'),
            float(rec[6]) if rec[6] is not None else 0.0,
            float(rec[7]) if rec[7] is not None else np.nan
        ))
    
    records = np.array(np_data, dtype=dtype)
    
    # Sort by cumuld for efficient processing
    valid_cumuld = ~np.isnan(records['cumuld'])
    records = records[valid_cumuld]
    sort_idx = np.argsort(records['cumuld'])
    records = records[sort_idx]
    
    ids_to_mark = set()
    n = len(records)
    
    # Vectorized distance calculation for optimization
    cumuld_values = records['cumuld']
    
    for i in range(n):
        img1 = records[i]
        
        if np.isnan(img1['cumuld']):
            continue
        
        # Find all records within ±distance_threshold range using vectorized operations
        # IMPORTANT: Only check j > i to avoid comparing pairs twice
        lower_bound = img1['cumuld'] - distance_threshold
        upper_bound = img1['cumuld'] + distance_threshold
        
        # Vectorized search for candidates (only indices greater than i)
        candidates_mask = (cumuld_values >= lower_bound) & (cumuld_values <= upper_bound) & (np.arange(n) > i)
        candidate_indices = np.where(candidates_mask)[0]
        
        for j in candidate_indices:
            img2 = records[j]
            
            obsolete_id = apply_business_rules_numpy(img1, img2)
            if obsolete_id:
                ids_to_mark.add(obsolete_id)
    
    logger.info(f"Axe '{axe_name}': found {len(ids_to_mark)} records to mark obsolete")
    return ids_to_mark


def apply_business_rules_numpy(img1: np.void, img2: np.void) -> str:
    """
    Apply business rules using numpy record types.
    
    Returns the ID to mark as obsolete, or None if no action needed.
    """
    session1 = img1['session_id']
    session2 = img2['session_id']
    
    # Same session
    if session1 == session2:
        # NEW RULE: Check if cumuld_session difference is > 100m
        cumuld_sess1 = img1['cumuld_session']
        cumuld_sess2 = img2['cumuld_session']
        
        # If both have valid cumuld_session values, check the difference
        if not np.isnan(cumuld_sess1) and not np.isnan(cumuld_sess2):
            cumuld_session_diff = abs(cumuld_sess1 - cumuld_sess2)
            if cumuld_session_diff <= 100:
                # Don't mark as obsolete if difference is <= 100m
                return None
        
        # Continue with existing rules if difference > 100m (or if cumuld_session is null)
        sens1 = img1['sens']
        sens2 = img2['sens']
        
        # Different sens: mark '+' as obsolete
        if sens1 != sens2:
            if sens1 == '+':
                return img1['id']
            elif sens2 == '+':
                return img2['id']
        # Same sens: mark the one with smaller index
        elif sens1 == sens2:
            idx1 = img1['index']
            idx2 = img2['index']
            if idx1 >= 0 and idx2 >= 0:
                if idx1 < idx2:
                    return img1['id']
                else:
                    return img2['id']
    
    # Different session
    else:
        sens1 = img1['sens']
        sens2 = img2['sens']
        
        # Different sens: mark '+' as obsolete
        if sens1 != sens2:
            if sens1 == '+':
                return img1['id']
            elif sens2 == '+':
                return img2['id']
        # Same sens
        elif sens1 == sens2:
            date1 = img1['captureDate']
            date2 = img2['captureDate']
            
            if not np.isnat(date1) and not np.isnat(date2):
                # Calculate date difference in days
                date_diff = abs((date1 - date2).astype('timedelta64[D]').astype(int))
                
                # If more than 30 days: mark older one
                if date_diff > 30:
                    if date1 < date2:
                        return img1['id']
                    else:
                        return img2['id']
                # If <= 30 days: mark the one with higher note_globale
                else:
                    note1 = img1['note_globale']
                    note2 = img2['note_globale']
                    if note1 > note2:
                        return img1['id']
                    elif note2 > note1:
                        return img2['id']
    
    return None


class ImageObsoleteUpdater:
    def __init__(self, db_config: dict, num_processes: int = None, distance_threshold: float = 6):
        """
        Initialize the updater with database configuration.
        
        Args:
            db_config: Dictionary with keys: host, database, user, password, port
            num_processes: Number of processes to use (default: cpu_count())
            distance_threshold: Distance threshold for comparing images (default: 6)
        """
        self.db_config = db_config
        self.num_processes = num_processes or cpu_count()
        self.distance_threshold = distance_threshold
        self.has_primary_key = False
        logger.info(f"Using {self.num_processes} processes for parallel processing")
        logger.info(f"Distance threshold set to: {self.distance_threshold}")
    
    def check_primary_key(self):
        """
        Check if id column is a primary key.
        """
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        try:
            # Check for primary key constraint
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'public'
                    AND tc.table_name = 'image'
                    AND tc.constraint_type = 'PRIMARY KEY'
                    AND kcu.column_name = 'id'
            """)
            
            has_pk = cursor.fetchone()[0] > 0
            self.has_primary_key = has_pk
            
            if has_pk:
                logger.info("✓ Column 'id' is a PRIMARY KEY - no duplicates expected")
            else:
                logger.warning("✗ Column 'id' is NOT a PRIMARY KEY - checking for duplicates...")
                
                # Check for actual duplicates
                cursor.execute("""
                    SELECT COUNT(*) as duplicate_count
                    FROM (
                        SELECT id
                        FROM public.image
                        GROUP BY id
                        HAVING COUNT(*) > 1
                    ) dups
                """)
                
                dup_count = cursor.fetchone()[0]
                if dup_count > 0:
                    logger.warning(f"Found {dup_count} duplicate IDs in the table!")
                    
                    # Show some examples
                    cursor.execute("""
                        SELECT id, COUNT(*) as cnt
                        FROM public.image
                        GROUP BY id
                        HAVING COUNT(*) > 1
                        ORDER BY cnt DESC
                        LIMIT 5
                    """)
                    
                    examples = cursor.fetchall()
                    logger.warning(f"Examples of duplicates: {examples}")
                else:
                    logger.info("No duplicate IDs found in the table")
            
        finally:
            cursor.close()
            conn.close()
        
        return self.has_primary_key
        
    def get_all_data_by_axe(self, axe_list: List[str] = None) -> Dict[str, List[tuple]]:
        """
        Fetch all non-obsolete records grouped by axe.
        Uses DISTINCT ON to handle duplicate IDs if primary key doesn't exist.
        
        Args:
            axe_list: Optional list of specific axes to process. If None, process all.
        
        Returns:
            Dictionary mapping axe -> list of records
        """
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        # If no primary key, use DISTINCT ON to get only one record per ID
        # Prioritize: most recent captureDate, then smallest index
        if not self.has_primary_key:
            logger.info("Using DISTINCT ON to handle potential duplicate IDs...")
            distinct_clause = 'DISTINCT ON (id)'
            order_clause = 'id, "captureDate" DESC NULLS LAST, index ASC NULLS LAST, axe, cumuld'
        else:
            distinct_clause = ''
            order_clause = 'axe, cumuld'
        
        if axe_list:
            logger.info(f"Fetching data for specific axes: {axe_list}")
            query = f"""
                SELECT {distinct_clause} axe, id, session_id, cumuld, sens, index, "captureDate", note_globale, cumuld_session
                FROM public.image
                WHERE obsolette = false
                AND axe = ANY(%s)
                ORDER BY {order_clause}
            """
            cursor.execute(query, (axe_list,))
        else:
            logger.info("Fetching all data from database...")
            query = f"""
                SELECT {distinct_clause} axe, id, session_id, cumuld, sens, index, "captureDate", note_globale, cumuld_session
                FROM public.image
                WHERE obsolette = false
                AND axe IS NOT NULL
                ORDER BY {order_clause}
            """
            cursor.execute(query)
        
        # Group records by axe
        axe_data = {}
        current_axe = None
        current_records = []
        
        for row in cursor:
            axe = row[0]
            record = row[1:]  # Everything except axe
            
            if axe != current_axe:
                if current_axe is not None:
                    axe_data[current_axe] = current_records
                current_axe = axe
                current_records = [record]
            else:
                current_records.append(record)
        
        # Don't forget the last axe
        if current_axe is not None:
            axe_data[current_axe] = current_records
        
        cursor.close()
        conn.close()
        
        logger.info(f"Loaded {len(axe_data)} axes with total records")
        return axe_data
    
    def batch_update_obsolete(self, ids_to_update: Set[str], batch_size: int = 5000):
        """
        Update obsolette flag for given IDs in batches.
        Note: If there are duplicate IDs in the table, all records with that ID will be updated.
        """
        if not ids_to_update:
            logger.info("No records to update")
            return 0
        
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        ids_list = list(ids_to_update)
        
        logger.info(f"Updating records for {len(ids_list)} unique IDs in batches of {batch_size}...")
        
        total_updated = 0
        
        try:
            for i in range(0, len(ids_list), batch_size):
                batch = ids_list[i:i + batch_size]
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['%s'] * len(batch))
                query = f"""
                    UPDATE public.image 
                    SET obsolette = true 
                    WHERE id IN ({placeholders})
                """
                
                cursor.execute(query, batch)
                updated = cursor.rowcount
                total_updated += updated
                
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"Batch {i//batch_size + 1}: Updated {updated} rows from {len(batch)} IDs")
            
            conn.commit()
            
            # Summary
            if total_updated == len(ids_list):
                logger.info(f"✓ Successfully updated {total_updated} records (1:1 ratio - no duplicates)")
            else:
                logger.info(f"✓ Successfully updated {total_updated} records from {len(ids_list)} unique IDs (ratio: {total_updated/len(ids_list):.2f}:1)")
                if total_updated > len(ids_list):
                    logger.warning(f"Note: {total_updated - len(ids_list)} duplicate records were also marked obsolete")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during update: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        return total_updated
    
    def reset_all_obsolete_flags(self, axe_list: List[str] = None):
        """
        Reset all obsolete flags to false before processing.
        
        Args:
            axe_list: Optional list of specific axes to reset. If None, reset all.
        """
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        try:
            if axe_list:
                logger.info(f"Resetting obsolete flags to false for axes: {axe_list}")
                query = """
                    UPDATE public.image 
                    SET obsolette = false
                    WHERE axe = ANY(%s)
                """
                cursor.execute(query, (axe_list,))
            else:
                logger.info("Resetting all obsolete flags to false for every record...")
                query = """
                    UPDATE public.image 
                    SET obsolette = false
                """
                cursor.execute(query)
            
            updated = cursor.rowcount
            conn.commit()
            logger.info(f"Reset {updated} records to obsolette = false")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error resetting obsolete flags: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def process_all_axes_parallel(self, axe_list: List[str] = None):
        """
        Main processing function using multiprocessing.
        
        Args:
            axe_list: Optional list of specific axes to process. If None, process all axes.
        """
        start_time = datetime.now()
        
        # Step -1: Check if id is primary key and look for duplicates
        logger.info("Step -1: Checking primary key and duplicates...")
        self.check_primary_key()
        
        # Step 0: Reset all obsolete flags to false
        logger.info("Step 0: Resetting obsolete flags...")
        self.reset_all_obsolete_flags(axe_list)
        
        # Step 1: Load all data grouped by axe
        logger.info("Step 1: Loading data from database...")
        axe_data = self.get_all_data_by_axe(axe_list)
        
        # Step 2: Prepare work items for parallel processing
        logger.info("Step 2: Preparing parallel processing tasks...")
        work_items = [(axe, records, self.distance_threshold) for axe, records in axe_data.items() if len(records) >= 2]
        logger.info(f"Processing {len(work_items)} axes in parallel...")
        
        # Step 3: Process in parallel using multiprocessing pool
        logger.info("Step 3: Processing data in parallel...")
        all_ids_to_mark = set()
        
        with Pool(processes=self.num_processes) as pool:
            results = pool.map(process_axe_worker, work_items)
            
            # Combine results from all workers
            for result_set in results:
                all_ids_to_mark.update(result_set)
        
        logger.info(f"Found {len(all_ids_to_mark)} total records to mark obsolete")
        
        # Step 4: Batch update all records
        logger.info("Step 4: Updating database...")
        total_updated = self.batch_update_obsolete(all_ids_to_mark)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Processing complete! Total time: {duration}")
        logger.info(f"Records marked obsolete: {total_updated}")
        
        return total_updated


def main():
    """Main execution function."""
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'cd12_demo',
        'user': 'diagway',
        'password': 'diagway',
        'port': 5433
    }
    
    # Create updater instance with multiprocessing
    # Use None to auto-detect CPU count, or specify a number
    updater = ImageObsoleteUpdater(db_config, num_processes=None)
    
    # Process all axes in parallel
    logger.info("Starting obsolete flag update process with parallel processing...")
    
    # Option 1: Process ALL axes
    updater.process_all_axes_parallel()
    
    # Option 2: Process specific list of axes (uncomment to use)
    # specific_axes = ['D1']
    # updater.process_all_axes_parallel(axe_list=specific_axes)


if __name__ == "__main__":
    main()