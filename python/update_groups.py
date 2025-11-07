import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Tuple
import sys

class UnionFind:
    """Union-Find data structure for finding connected components"""
    def __init__(self):
        self.parent = {}
    
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


def calculate_distance(x1: float, y1: float, x2: float, y2: float) -> float:
    """Calculate Euclidean distance between two points"""
    return ((x2 - x1)**2 + (y2 - y1)**2)**0.5


def update_group_ids(
    host: str = "localhost",
    database: str = "your_database",
    user: str = "your_user",
    password: str = "your_password",
    port: int = 5432,
    distance_threshold: float = 1.5
):
    """
    Update group_id for signalisation_h_intens table
    
    Rules:
    - Each is_linaire=true record gets unique group_id
    - is_linaire=false records with same codification and within distance_threshold
      are grouped together (transitive clustering)
    
    Args:
        host: Database host
        database: Database name
        user: Database user
        password: Database password
        port: Database port
        distance_threshold: Maximum distance (in meters) for grouping non-linear records
    """
    
    print("=" * 60)
    print("Group ID Update Process")
    print("=" * 60)
    
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
        print(f"✓ Connected to database: {database}")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)
    
    try:
        # Step 1: Get all linear records (is_linaire = true)
        print("\n[Step 1/5] Fetching linear records (is_linaire = true)...")
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
        for idx, (record_id,) in enumerate(linear_records, start=1):
            linear_updates.append((idx, record_id))
        
        # Step 2: Get all non-linear records with spatial data
        print("\n[Step 2/5] Fetching non-linear records (is_linaire = false)...")
        cur.execute("""
            SELECT 
                id,
                codification,
                ST_X(geom) as x,
                ST_Y(geom) as y
            FROM offroad.signalisation_h_intens
            WHERE is_linaire = false
                AND codification IS NOT NULL
                AND geom IS NOT NULL
        """)
        nonlinear_records = cur.fetchall()
        print(f"  Found {len(nonlinear_records)} non-linear records")
        
        if not nonlinear_records:
            print("  No non-linear records to process")
            nonlinear_updates = []
        else:
            # Step 3: Group by codification
            print("\n[Step 3/5] Grouping by codification...")
            by_codification: Dict[str, List[Tuple]] = {}
            for record in nonlinear_records:
                record_id, codif, x, y = record
                if codif not in by_codification:
                    by_codification[codif] = []
                by_codification[codif].append((record_id, x, y))
            
            print(f"  Found {len(by_codification)} unique codifications")
            
            # Step 4: Find connected components using Union-Find
            print("\n[Step 4/5] Finding connected components...")
            print(f"  Distance threshold: {distance_threshold}m")
            
            uf = UnionFind()
            total_connections = 0
            
            # For each codification group, find pairs within distance threshold
            for codif, records in by_codification.items():
                connections_in_group = 0
                
                # Compare each pair within this codification
                for i in range(len(records)):
                    record_id1, x1, y1 = records[i]
                    for j in range(i + 1, len(records)):
                        record_id2, x2, y2 = records[j]
                        
                        # Calculate distance
                        distance = calculate_distance(x1, y1, x2, y2)
                        
                        if distance < distance_threshold:
                            uf.union(record_id1, record_id2)
                            connections_in_group += 1
                            total_connections += 1
                
                if len(records) > 1:
                    print(f"    '{codif}': {len(records)} records, {connections_in_group} connections")
            
            print(f"  Total connections found: {total_connections}")
            
            # Get group assignments for non-linear records
            groups = uf.get_groups()
            unique_groups = len(set(groups.values()))
            print(f"  Created {unique_groups} non-linear groups")
            
            # Offset group_id to start after linear records
            max_linear_group = len(linear_records)
            nonlinear_updates = [
                (group_id + max_linear_group, record_id) 
                for record_id, group_id in groups.items()
            ]
        
        # Step 5: Update database
        print("\n[Step 5/5] Updating database...")
        
        # Update linear records
        if linear_updates:
            execute_batch(cur, """
                UPDATE offroad.signalisation_h_intens
                SET group_id = %s
                WHERE id = %s
            """, linear_updates, page_size=1000)
            print(f"  ✓ Updated {len(linear_updates)} linear records")
        
        # Update non-linear records
        if nonlinear_updates:
            execute_batch(cur, """
                UPDATE offroad.signalisation_h_intens
                SET group_id = %s
                WHERE id = %s
            """, nonlinear_updates, page_size=1000)
            print(f"  ✓ Updated {len(nonlinear_updates)} non-linear records")
        
        # Commit changes
        conn.commit()
        print("\n✓ Successfully committed all changes")
        
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
        print(f"  - Linear groups:         {stats[2]}")
        print(f"  - Non-linear groups:     {stats[1] - stats[2] if stats[1] else 0}")
        
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
                group_type = "Linear" if all_linear else "Non-linear"
                codif_str = ", ".join([c for c in codifs if c]) if codifs else "None"
                print(f"{group_id:<12} {count:<8} {group_type:<15} {codif_str[:30]}")
        
        print("\n✓ Process completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error occurred: {e}")
        print("  All changes have been rolled back")
        raise
    
    finally:
        cur.close()
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    # Configure your database connection parameters
    DB_CONFIG = {
        "host": "localhost",
        "database": "CD93_2023",
        "user": "diagway",
        "password": "diagway",
        "port": 5433,
        "distance_threshold": 0.7  # Distance in meters
    }
    
    # Run the update
    update_group_ids(**DB_CONFIG)