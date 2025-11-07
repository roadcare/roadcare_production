import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Tuple, Set
import sys

class UnionFind:
    """Union-Find data structure for finding connected components"""
    def __init__(self):
        self.parent = {}
    
    def add(self, x):
        """Add an element to the structure"""
        if x not in self.parent:
            self.parent[x] = x
    
    def find(self, x):
        """Find root of element x with path compression"""
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        """Unite two sets containing x and y"""
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            self.parent[root_x] = root_y
    
    def get_groups(self):
        """Return dictionary mapping each element to its group id"""
        roots = {}
        group_id = 1
        result = {}
        
        for item in self.parent.keys():
            root = self.find(item)
            if root not in roots:
                roots[root] = group_id
                group_id += 1
            result[item] = roots[root]
        
        return result


def update_group_ids(
    host: str = "localhost",
    database: str = "your_database",
    user: str = "your_user",
    password: str = "your_password",
    port: int = 5432,
    distance_threshold: float = 1.5,
    excluded_codifications: List[str] = None,
    codification_thresholds: Dict[str, float] = None
):
    """
    Update group_id for signalisation_h_intens table
    
    Rules:
    - Each is_linaire=true record gets unique group_id
    - Records with codification in excluded_codifications list get unique group_id (not grouped)
    - Other is_linaire=false records with same codification and ST_Distance(geom_center) < threshold
      are grouped together (transitive clustering)
    - All records must have a group_id (not null)
    
    Args:
        host: Database host
        database: Database name
        user: Database user
        password: Database password
        port: Database port
        distance_threshold: Default maximum distance (in meters) for grouping non-linear records
        excluded_codifications: List of codifications that should NOT be grouped together
        codification_thresholds: Dict of codification -> specific distance threshold
                                 Example: {'ZEBRA': 3.5, 'B14': 2.0}
    """
    
    if excluded_codifications is None:
        excluded_codifications = []
    
    if codification_thresholds is None:
        codification_thresholds = {}
    
    print("=" * 60)
    print("Group ID Update Process")
    print("=" * 60)
    print(f"\nDefault distance threshold: {distance_threshold}m")
    
    if codification_thresholds:
        print(f"\nSpecific thresholds by codification:")
        for codif, threshold in codification_thresholds.items():
            print(f"  - '{codif}': {threshold}m")
    
    if excluded_codifications:
        print(f"\nExcluded codifications (will NOT be grouped):")
        for codif in excluded_codifications:
            print(f"  - {codif}")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()
        print(f"\n✓ Connected to database: {database}")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)
    
    try:
        # Step 0: Reset all group_id to NULL
        print("\n[Step 0/7] Resetting all group_id to NULL...")
        cur.execute("""
            UPDATE offroad.signalisation_h_intens
            SET group_id = NULL
        """)
        rows_reset = cur.rowcount
        print(f"  ✓ Reset {rows_reset} records")
        
        current_group_id = 1
        
        # Step 1: Get all linear records (is_linaire = true)
        print("\n[Step 1/7] Fetching linear records (is_linaire = true)...")
        cur.execute("""
            SELECT id
            FROM offroad.signalisation_h_intens
            WHERE is_linaire = true
            ORDER BY id
        """)
        linear_records = cur.fetchall()
        print(f"  Found {len(linear_records)} linear records")
        
        # Assign unique group_id to each linear record
        linear_updates = []
        for record_id, in linear_records:
            linear_updates.append((current_group_id, record_id))
            current_group_id += 1
        
        # Step 2: Get non-linear records with excluded codifications
        print("\n[Step 2/7] Fetching non-linear records with excluded codifications...")
        if excluded_codifications:
            cur.execute("""
                SELECT id, codification
                FROM offroad.signalisation_h_intens
                WHERE is_linaire = false
                    AND codification = ANY(%s)
                ORDER BY id
            """, (excluded_codifications,))
            excluded_records = cur.fetchall()
            print(f"  Found {len(excluded_records)} records with excluded codifications")
            
            # Assign unique group_id to each excluded record
            excluded_updates = []
            for record_id, codif in excluded_records:
                excluded_updates.append((current_group_id, record_id))
                current_group_id += 1
        else:
            excluded_records = []
            excluded_updates = []
            print(f"  No excluded codifications specified")
        
        # Step 3: Get all other non-linear records (to be grouped)
        print("\n[Step 3/7] Fetching non-linear records for grouping...")
        if excluded_codifications:
            cur.execute("""
                SELECT 
                    id,
                    codification
                FROM offroad.signalisation_h_intens
                WHERE is_linaire = false
                    AND (codification IS NULL OR codification <> ALL(%s))
            """, (excluded_codifications,))
        else:
            cur.execute("""
                SELECT 
                    id,
                    codification
                FROM offroad.signalisation_h_intens
                WHERE is_linaire = false
            """)
        
        nonlinear_records = cur.fetchall()
        print(f"  Found {len(nonlinear_records)} non-linear records to process")
        
        if not nonlinear_records:
            print("  No non-linear records to process")
            nonlinear_updates = []
        else:
            # Step 4: Group by codification
            print("\n[Step 4/7] Grouping by codification...")
            by_codification: Dict[str, List[int]] = {}
            records_with_null_codif = []
            
            for record_id, codif in nonlinear_records:
                if codif is None:
                    records_with_null_codif.append(record_id)
                else:
                    if codif not in by_codification:
                        by_codification[codif] = []
                    by_codification[codif].append(record_id)
            
            print(f"  Found {len(by_codification)} unique codifications")
            if records_with_null_codif:
                print(f"  Found {len(records_with_null_codif)} records with NULL codification")
            
            # Step 5: Find connected components using Union-Find
            print("\n[Step 5/7] Finding connected components...")
            print(f"  Using ST_Distance on geom_center (Point)")
            
            uf = UnionFind()
            
            # IMPORTANT: Add ALL non-linear records to Union-Find first
            # This ensures even isolated records get a group_id
            print("  Initializing all records in Union-Find...")
            for record_id, codif in nonlinear_records:
                uf.add(record_id)
            
            total_connections = 0
            
            # For each codification group, find pairs within distance threshold
            for codif, record_ids in by_codification.items():
                if len(record_ids) <= 1:
                    # Single records are already initialized, no connections needed
                    continue
                
                # Get threshold for this codification (specific or default)
                threshold = codification_thresholds.get(codif, distance_threshold)
                
                print(f"    Processing '{codif}': {len(record_ids)} records (threshold: {threshold}m)...")
                
                # Find all pairs within distance threshold using PostgreSQL
                cur.execute("""
                    SELECT 
                        t1.id as id1,
                        t2.id as id2,
                        ST_Distance(t1.geom_center, t2.geom_center) as distance
                    FROM offroad.signalisation_h_intens t1
                    JOIN offroad.signalisation_h_intens t2 ON t1.id < t2.id
                    WHERE t1.id = ANY(%s)
                        AND t2.id = ANY(%s)
                        AND t1.is_linaire = false
                        AND t2.is_linaire = false
                        AND t1.codification = %s
                        AND t2.codification = %s
                        AND t1.geom_center IS NOT NULL
                        AND t2.geom_center IS NOT NULL
                        AND ST_Distance(t1.geom_center, t2.geom_center) < %s
                """, (record_ids, record_ids, codif, codif, threshold))
                
                pairs = cur.fetchall()
                
                # Add connections to Union-Find
                for id1, id2, distance in pairs:
                    uf.union(id1, id2)
                    total_connections += 1
                
                if pairs:
                    print(f"      Found {len(pairs)} connections")
            
            print(f"  Total connections found: {total_connections}")
            
            # Get group assignments for ALL non-linear records
            groups = uf.get_groups()
            unique_groups = len(set(groups.values())) if groups else 0
            print(f"  Created {unique_groups} groups (including isolated records)")
            
            # Assign group_ids starting after linear and excluded records
            nonlinear_updates = [
                (group_id + current_group_id - 1, record_id) 
                for record_id, group_id in groups.items()
            ]
            
            # Verify all non-linear records have been assigned
            assigned_ids = set(record_id for _, record_id in nonlinear_updates)
            all_ids = set(record_id for record_id, _ in nonlinear_records)
            missing_ids = all_ids - assigned_ids
            
            if missing_ids:
                print(f"  ⚠ Warning: {len(missing_ids)} records were not assigned a group_id")
                print(f"    Missing IDs: {list(missing_ids)[:10]}...")
        
        # Step 6: Update database
        print("\n[Step 6/7] Updating database...")
        
        # Update linear records
        if linear_updates:
            execute_batch(cur, """
                UPDATE offroad.signalisation_h_intens
                SET group_id = %s
                WHERE id = %s
            """, linear_updates, page_size=1000)
            print(f"  ✓ Updated {len(linear_updates)} linear records")
        
        # Update excluded records (not grouped)
        if excluded_updates:
            execute_batch(cur, """
                UPDATE offroad.signalisation_h_intens
                SET group_id = %s
                WHERE id = %s
            """, excluded_updates, page_size=1000)
            print(f"  ✓ Updated {len(excluded_updates)} excluded records (not grouped)")
        
        # Update non-linear records (grouped)
        if nonlinear_updates:
            execute_batch(cur, """
                UPDATE offroad.signalisation_h_intens
                SET group_id = %s
                WHERE id = %s
            """, nonlinear_updates, page_size=1000)
            print(f"  ✓ Updated {len(nonlinear_updates)} non-linear records (grouped)")
        
        # Step 7: Commit changes
        print("\n[Step 7/7] Committing changes...")
        conn.commit()
        print("  ✓ Successfully committed all changes")
        
        # Verify no NULL group_id remains
        print("\n[Verification] Checking for NULL group_id...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM offroad.signalisation_h_intens
            WHERE group_id IS NULL
        """)
        null_count = cur.fetchone()[0]
        
        if null_count > 0:
            print(f"  ⚠ WARNING: {null_count} records still have NULL group_id!")
            
            # Show details of records with NULL group_id
            cur.execute("""
                SELECT id, is_linaire, codification, 
                       geom_center IS NOT NULL as has_geom_center
                FROM offroad.signalisation_h_intens
                WHERE group_id IS NULL
                LIMIT 10
            """)
            null_records = cur.fetchall()
            print("\n  Sample of records with NULL group_id:")
            print(f"  {'ID':<10} {'is_linaire':<12} {'Codification':<20} {'Has geom_center'}")
            print("  " + "-" * 60)
            for rec_id, is_lin, codif, has_geom in null_records:
                print(f"  {rec_id:<10} {str(is_lin):<12} {codif or 'NULL':<20} {has_geom}")
        else:
            print(f"  ✓ All records have a group_id assigned")
        
        # Show summary statistics
        print("\n" + "=" * 60)
        print("Summary Statistics")
        print("=" * 60)
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT group_id) as total_groups,
                SUM(CASE WHEN is_linaire = true THEN 1 ELSE 0 END) as linear_count,
                SUM(CASE WHEN is_linaire = false THEN 1 ELSE 0 END) as nonlinear_count,
                COUNT(CASE WHEN group_id IS NULL THEN 1 END) as null_group_count
            FROM offroad.signalisation_h_intens
        """)
        stats = cur.fetchone()
        
        print(f"Total records in table:    {stats[0]}")
        print(f"Records with group_id:     {stats[0] - stats[4]}")
        print(f"Records without group_id:  {stats[4]}")
        print(f"Total unique groups:       {stats[1]}")
        print(f"  - Linear records:        {len(linear_updates)}")
        print(f"  - Excluded (not grouped):{len(excluded_updates)}")
        print(f"  - Other groups:          {stats[1] - len(linear_updates) - len(excluded_updates) if stats[1] else 0}")
        
        # Show specific codification thresholds summary
        if codification_thresholds:
            print("\n" + "=" * 60)
            print("Codifications with Specific Thresholds")
            print("=" * 60)
            
            for codif, threshold in codification_thresholds.items():
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT group_id) as num_groups
                    FROM offroad.signalisation_h_intens
                    WHERE codification = %s AND group_id IS NOT NULL
                """, (codif,))
                result = cur.fetchone()
                if result[0] > 0:
                    avg_per_group = result[0] / result[1] if result[1] > 0 else 0
                    print(f"  '{codif}' (threshold: {threshold}m):")
                    print(f"    - Total records: {result[0]}")
                    print(f"    - Number of groups: {result[1]}")
                    print(f"    - Avg records/group: {avg_per_group:.1f}")
        
        # Show excluded codifications summary
        if excluded_codifications:
            print("\n" + "=" * 60)
            print("Excluded Codifications Summary")
            print("=" * 60)
            
            for codif in excluded_codifications:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM offroad.signalisation_h_intens
                    WHERE codification = %s AND group_id IS NOT NULL
                """, (codif,))
                count = cur.fetchone()[0]
                print(f"  '{codif}': {count} records (each with unique group_id)")
        
        # Show some examples
        print("\n" + "=" * 60)
        print("Sample Groups")
        print("=" * 60)
        
        cur.execute("""
            SELECT 
                group_id,
                COUNT(*) as count,
                bool_and(is_linaire) as all_linear,
                array_agg(DISTINCT codification) as codifications
            FROM offroad.signalisation_h_intens
            WHERE group_id IS NOT NULL
            GROUP BY group_id
            ORDER BY count DESC, group_id
            LIMIT 10
        """)
        
        examples = cur.fetchall()
        if examples:
            print(f"{'Group ID':<12} {'Count':<8} {'Type':<15} {'Codifications'}")
            print("-" * 60)
            for group_id, count, all_linear, codifs in examples:
                if all_linear:
                    group_type = "Linear"
                elif count == 1:
                    group_type = "Isolated"
                else:
                    group_type = "Clustered"
                codif_str = ", ".join([c for c in codifs if c]) if codifs else "None"
                print(f"{group_id:<12} {count:<8} {group_type:<15} {codif_str[:30]}")
        
        # Show distribution of group sizes
        print("\n" + "=" * 60)
        print("Group Size Distribution")
        print("=" * 60)
        
        cur.execute("""
            SELECT 
                CASE 
                    WHEN count = 1 THEN '1 record'
                    WHEN count = 2 THEN '2 records'
                    WHEN count BETWEEN 3 AND 5 THEN '3-5 records'
                    WHEN count BETWEEN 6 AND 10 THEN '6-10 records'
                    ELSE '10+ records'
                END as size_range,
                COUNT(*) as num_groups,
                SUM(count) as total_records
            FROM (
                SELECT group_id, COUNT(*) as count
                FROM offroad.signalisation_h_intens
                WHERE group_id IS NOT NULL
                GROUP BY group_id
            ) subq
            GROUP BY 
                CASE 
                    WHEN count = 1 THEN '1 record'
                    WHEN count = 2 THEN '2 records'
                    WHEN count BETWEEN 3 AND 5 THEN '3-5 records'
                    WHEN count BETWEEN 6 AND 10 THEN '6-10 records'
                    ELSE '10+ records'
                END
            ORDER BY MIN(count)
        """)
        
        distribution = cur.fetchall()
        if distribution:
            print(f"{'Size Range':<15} {'# Groups':<12} {'Total Records'}")
            print("-" * 60)
            for size_range, num_groups, total_records in distribution:
                print(f"{size_range:<15} {num_groups:<12} {total_records}")
        
        print("\n✓ Process completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error occurred: {e}")
        print("  All changes have been rolled back")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        cur.close()
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    # Configure your database connection parameters
    DB_CONFIG = {
        "host": "localhost",
        "database": 'CD93_2023',
        "user": "diagway",
        "password": "diagway",
        "port": 5433,
        "distance_threshold": 1.5,  # Default distance in meters
        
        # List of codifications that should NOT be grouped
        # Each record with these codifications will get a unique group_id
       	"excluded_codifications": [ "PIC_VELO", 'FD_D','FD_G','FD_TD','FD_TD','FD','FR_D','FR_G'],
        
        # Dictionary of codifications with specific distance thresholds
        # Format: "codification": distance_in_meters
        "codification_thresholds": {
            # Example: ZEBRA crossings might need a larger distance threshold
            "ZEBRA": 3.5
            # "PASSAGE_PIETON": 3.0,
            # "STOP": 2.0,
        }
    }
    
    # Run the update
    update_group_ids(**DB_CONFIG)