"""
Homogeneous Zone Calculator - PostgreSQL Version
Converts road/infrastructure data into homogeneous zones based on attribute values.
"""

import psycopg2
from psycopg2 import sql
from dataclasses import dataclass
from typing import List, Optional, Tuple
import math


@dataclass
class ZHomogene:
    """Represents a homogeneous zone"""
    id: int
    section_id: str
    cumuld: float  # Start cumulative distance
    cumulf: float  # End cumulative distance
    mean_val: float
    min_val: float
    max_val: float
    prd: Optional[str] = None
    abd: Optional[str] = None
    prf: Optional[str] = None
    abf: Optional[str] = None


class HomogeneousZoneCalculator:
    """Calculate homogeneous zones from PostgreSQL database"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "your_db", user: str = "postgres", 
                 password: str = "", schema_name: str = "mdb_largeur"):
        """
        Initialize calculator
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
            schema_name: PostgreSQL schema containing input/output tables
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema_name = schema_name
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            
    def get_value_themes(self) -> List[Tuple[str, int]]:
        """
        Get list of tables with value attributes in the schema
        
        Returns:
            List of (theme_name, location_type) tuples
            location_type: 0=Ponctuelle, 1=Linéaire
        """
        cursor = self.connection.cursor()
        
        # Get all tables in schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
        """, (self.schema_name,))
        
        tables = [row[0] for row in cursor.fetchall()]
        
        # Default tables to skip
        default_tables = ['plo', 'gps', 'camera_1', 'camera_2', 'camera_3', 
                         'camera_4', 'camera_5', 'camera_6']
        
        default_fields = ['id', 'section_id', 'cumuld', 'cumulf', 'cumul']
        
        value_themes = []
        
        for table_name in tables:
            if table_name.lower() in default_tables:
                continue
                
            try:
                # Get table columns
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                """, (self.schema_name, table_name))
                
                col_info = cursor.fetchall()
                col_names = [name.lower() for name, _ in col_info]
                
                # Check if it's a data table (has id, section_id, and cumul*)
                has_id = 'id' in col_names
                has_section = 'section_id' in col_names
                has_cumul = any(c in ['cumul', 'cumuld', 'cumulf'] for c in col_names)
                
                if not (has_id and has_section and has_cumul):
                    continue
                
                # Check if it has value columns (numeric, non-default)
                has_cumulf = 'cumulf' in col_names
                has_value = any(
                    name not in default_fields and 
                    dtype in ['integer', 'bigint', 'smallint', 'numeric', 
                             'decimal', 'real', 'double precision']
                    for name, dtype in col_info
                )
                
                if has_value:
                    location_type = 1 if has_cumulf else 0  # 1=Linéaire, 0=Ponctuelle
                    value_themes.append((table_name, location_type))
                    
            except Exception as e:
                print(f"Warning: Could not analyze table {table_name}: {e}")
                continue
        
        cursor.close()
        return value_themes
    
    def get_value_attributes(self, theme_name: str) -> List[str]:
        """
        Get list of numeric value attributes for a theme
        
        Args:
            theme_name: Name of the theme table
            
        Returns:
            List of attribute names
        """
        cursor = self.connection.cursor()
        default_fields = ['id', 'section_id', 'cumuld', 'cumulf', 'cumul']
        
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (self.schema_name, theme_name))
        
        value_attrs = []
        for col_name, dtype in cursor.fetchall():
            if (col_name.lower() not in default_fields and 
                dtype in ['integer', 'bigint', 'smallint', 'numeric', 
                         'decimal', 'real', 'double precision']):
                value_attrs.append(col_name)
        
        cursor.close()
        return value_attrs
    
    def calculate_zh(self, theme_name: str, attribute_name: str, 
                     u_threshold: float, min_length: float = 0.0,
                     refine: bool = True) -> str:
        """
        Calculate homogeneous zones
        
        Args:
            theme_name: Name of the source table
            attribute_name: Attribute to analyze
            u_threshold: Maximum allowed difference from mean (U parameter)
            min_length: Minimum zone length (zones shorter will be merged)
            refine: If True, apply U threshold fusion after min_length fusion
            
        Returns:
            Name of the result table
        """
        cursor = self.connection.cursor()
        
        # Check if table has PRD, ABD, PRF, ABF fields
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (self.schema_name, theme_name))
        
        columns = [row[0].lower() for row in cursor.fetchall()]
        has_pr_abs = all(f in columns for f in ['prd', 'abd', 'prf', 'abf'])
        
        # Load data
        query = sql.SQL("""
            SELECT id, section_id, cumuld, cumulf, {attr}
            {pr_abs_fields}
            FROM {schema}.{table}
            ORDER BY section_id, cumuld
        """).format(
            attr=sql.Identifier(attribute_name),
            pr_abs_fields=sql.SQL(", prd, abd, prf, abf") if has_pr_abs else sql.SQL(""),
            schema=sql.Identifier(self.schema_name),
            table=sql.Identifier(theme_name)
        )
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            cursor.close()
            raise ValueError(f"No data found in table {self.schema_name}.{theme_name}")
        
        # Create initial zones (each data point is a zone)
        # Cast all numeric values to float to avoid Decimal type issues
        current_zh_list = []
        skipped_zero_length = 0
        for row in rows:
            cumuld_val = float(row[2]) if row[2] is not None else 0.0
            cumulf_val = float(row[3]) if row[3] is not None else 0.0
            
            # Skip zero-length zones
            if abs(cumulf_val - cumuld_val) < 1e-6:
                skipped_zero_length += 1
                continue
            
            zh = ZHomogene(
                id=int(row[0]),
                section_id=str(row[1]),
                cumuld=cumuld_val,
                cumulf=cumulf_val,
                mean_val=float(row[4]) if row[4] is not None else 0.0,
                min_val=float(row[4]) if row[4] is not None else 0.0,
                max_val=float(row[4]) if row[4] is not None else 0.0
            )
            if has_pr_abs:
                zh.prd = str(row[5]) if row[5] is not None else ""
                zh.abd = str(row[6]) if row[6] is not None else ""
                zh.prf = str(row[7]) if row[7] is not None else ""
                zh.abf = str(row[8]) if row[8] is not None else ""
            current_zh_list.append(zh)
        
        if skipped_zero_length > 0:
            print(f"Skipped {skipped_zero_length} zero-length zones")
        
        cursor.close()
        print(f"Initial zones: {len(current_zh_list)}")
        
        # Step 1: Fusion by U threshold
        current_zh_list = self._fusion_by_u_threshold(
            current_zh_list, u_threshold, has_pr_abs
        )
        print(f"After U-threshold fusion: {len(current_zh_list)}")
        
        # Step 2: Force fusion of zones shorter than min_length
        if min_length > 0:
            current_zh_list = self._fusion_by_min_length(
                current_zh_list, min_length, has_pr_abs
            )
            print(f"After min-length fusion: {len(current_zh_list)}")
        
        # Step 3: Optional refinement with U threshold
        if refine:
            current_zh_list = self._fusion_by_u_threshold(
                current_zh_list, u_threshold, has_pr_abs
            )
            print(f"After refinement: {len(current_zh_list)}")
        
        # Write results to database
        result_table = self._write_results(
            current_zh_list, theme_name, attribute_name, has_pr_abs
        )
        
        return result_table
    
    def _fusion_by_u_threshold(self, zh_list: List[ZHomogene], 
                               u_threshold: float, has_pr_abs: bool) -> List[ZHomogene]:
        """Merge adjacent zones based on U threshold"""
        prev_count = len(zh_list)
        
        while True:
            next_list = []
            zh_idx = 0
            in_fusion = False
            fusion_zh = None
            
            while zh_idx < len(zh_list) - 1:
                if not in_fusion:
                    fusion_zh = zh_list[zh_idx]
                    in_fusion = True
                else:
                    current_zh = zh_list[zh_idx + 1]
                    
                    # Calculate weighted mean and new min/max
                    len_fusion = abs(fusion_zh.cumulf - fusion_zh.cumuld)
                    len_current = abs(current_zh.cumulf - current_zh.cumuld)
                    total_len = len_fusion + len_current
                    
                    # Handle edge case of zero-length zones
                    if total_len < 1e-6:
                        new_mean = (fusion_zh.mean_val + current_zh.mean_val) / 2.0
                    else:
                        new_mean = ((current_zh.mean_val * len_current + 
                                   fusion_zh.mean_val * len_fusion) / total_len)
                    
                    new_min = min(fusion_zh.min_val, current_zh.min_val)
                    new_max = max(fusion_zh.max_val, current_zh.max_val)
                    
                    # Check fusion conditions
                    can_merge = (
                        round(current_zh.cumuld) == round(fusion_zh.cumulf) and
                        (new_mean - new_min) <= u_threshold and
                        (new_max - new_mean) <= u_threshold and
                        current_zh.section_id == fusion_zh.section_id
                    )
                    
                    if not can_merge:
                        next_list.append(fusion_zh)
                        in_fusion = False
                        zh_idx += 1
                    else:
                        # Merge
                        fusion_zh.cumulf = current_zh.cumulf
                        if has_pr_abs:
                            fusion_zh.prf = current_zh.prf
                            fusion_zh.abf = current_zh.abf
                        fusion_zh.mean_val = new_mean
                        fusion_zh.min_val = new_min
                        fusion_zh.max_val = new_max
                        zh_idx += 1
            
            # Handle last zone
            if in_fusion:
                if zh_idx < len(zh_list):
                    current_zh = zh_list[zh_idx]
                    len_fusion = abs(fusion_zh.cumulf - fusion_zh.cumuld)
                    len_current = abs(current_zh.cumulf - current_zh.cumuld)
                    total_len = len_fusion + len_current
                    
                    # Handle edge case of zero-length zones
                    if total_len < 1e-6:
                        new_mean = (fusion_zh.mean_val + current_zh.mean_val) / 2.0
                    else:
                        new_mean = ((current_zh.mean_val * len_current + 
                                   fusion_zh.mean_val * len_fusion) / total_len)
                    
                    new_min = min(fusion_zh.min_val, current_zh.min_val)
                    new_max = max(fusion_zh.max_val, current_zh.max_val)
                    
                    can_merge = (
                        round(current_zh.cumuld) == round(fusion_zh.cumulf) and
                        (new_mean - new_min) <= u_threshold and
                        (new_max - new_mean) <= u_threshold and
                        current_zh.section_id == fusion_zh.section_id
                    )
                    
                    if can_merge:
                        fusion_zh.cumulf = current_zh.cumulf
                        if has_pr_abs:
                            fusion_zh.prf = current_zh.prf
                            fusion_zh.abf = current_zh.abf
                        fusion_zh.mean_val = new_mean
                        fusion_zh.min_val = new_min
                        fusion_zh.max_val = new_max
                        next_list.append(fusion_zh)
                    else:
                        next_list.append(fusion_zh)
                        next_list.append(current_zh)
                else:
                    next_list.append(fusion_zh)
            elif zh_idx < len(zh_list):
                next_list.append(zh_list[zh_idx])
            
            # Check if converged
            if len(next_list) == prev_count:
                break
                
            zh_list = next_list
            prev_count = len(zh_list)
        
        return zh_list
    
    def _fusion_by_min_length(self, zh_list: List[ZHomogene], 
                             min_length: float, has_pr_abs: bool) -> List[ZHomogene]:
        """Force merge zones shorter than min_length"""
        prev_count = len(zh_list)
        
        while True:
            next_list = []
            zh_idx = 0
            
            while zh_idx < len(zh_list):
                current_zh = zh_list[zh_idx]
                zh_length = abs(current_zh.cumulf - current_zh.cumuld)
                
                if zh_length >= min_length:
                    next_list.append(current_zh)
                    zh_idx += 1
                else:
                    # Zone too short, merge with neighbor
                    has_next = zh_idx + 1 < len(zh_list)
                    has_prev = len(next_list) > 0
                    
                    if not has_next:
                        # Last zone, merge with previous if possible
                        if has_prev:
                            prev_zh = next_list[-1]
                            if (prev_zh.section_id == current_zh.section_id and
                                round(prev_zh.cumulf) == round(current_zh.cumuld)):
                                next_list[-1] = self._merge_zones(
                                    prev_zh, current_zh, has_pr_abs
                                )
                            else:
                                next_list.append(current_zh)
                        else:
                            next_list.append(current_zh)
                        zh_idx += 1
                    elif not has_prev:
                        # First zone, merge with next if possible
                        next_zh = zh_list[zh_idx + 1]
                        if (next_zh.section_id == current_zh.section_id and
                            round(next_zh.cumuld) == round(current_zh.cumulf)):
                            merged = self._merge_zones(current_zh, next_zh, has_pr_abs)
                            next_list.append(merged)
                            zh_idx += 2
                        else:
                            next_list.append(current_zh)
                            zh_idx += 1
                    else:
                        # Has both neighbors, merge with closest mean
                        prev_zh = next_list[-1]
                        next_zh = zh_list[zh_idx + 1]
                        
                        if (next_zh.section_id == current_zh.section_id and
                            prev_zh.section_id == current_zh.section_id):
                            # Choose neighbor with closest mean
                            diff_prev = abs(prev_zh.mean_val - current_zh.mean_val)
                            diff_next = abs(next_zh.mean_val - current_zh.mean_val)
                            
                            if (diff_prev < diff_next and
                                round(prev_zh.cumulf) == round(current_zh.cumuld)):
                                next_list[-1] = self._merge_zones(
                                    prev_zh, current_zh, has_pr_abs
                                )
                                zh_idx += 1
                            elif round(next_zh.cumuld) == round(current_zh.cumulf):
                                merged = self._merge_zones(
                                    current_zh, next_zh, has_pr_abs
                                )
                                next_list.append(merged)
                                zh_idx += 2
                            else:
                                next_list.append(current_zh)
                                zh_idx += 1
                        elif (next_zh.section_id == current_zh.section_id and
                              round(next_zh.cumuld) == round(current_zh.cumulf)):
                            merged = self._merge_zones(current_zh, next_zh, has_pr_abs)
                            next_list.append(merged)
                            zh_idx += 2
                        elif (prev_zh.section_id == current_zh.section_id and
                              round(prev_zh.cumulf) == round(current_zh.cumuld)):
                            next_list[-1] = self._merge_zones(
                                prev_zh, current_zh, has_pr_abs
                            )
                            zh_idx += 1
                        else:
                            next_list.append(current_zh)
                            zh_idx += 1
            
            # Check if converged
            if len(next_list) == prev_count:
                break
                
            zh_list = next_list
            prev_count = len(zh_list)
        
        return zh_list
    
    def _merge_zones(self, zh1: ZHomogene, zh2: ZHomogene, 
                    has_pr_abs: bool) -> ZHomogene:
        """Merge two adjacent zones"""
        len1 = abs(zh1.cumulf - zh1.cumuld)
        len2 = abs(zh2.cumulf - zh2.cumuld)
        total_len = len1 + len2
        
        # Handle edge case of zero-length zones
        if total_len < 1e-6:
            # Use simple average if both zones have zero length
            mean_val = (zh1.mean_val + zh2.mean_val) / 2.0
        else:
            # Calculate weighted mean
            mean_val = ((zh1.mean_val * len1 + zh2.mean_val * len2) / total_len)
        
        merged = ZHomogene(
            id=zh1.id,
            section_id=zh1.section_id,
            cumuld=zh1.cumuld,
            cumulf=zh2.cumulf,
            mean_val=mean_val,
            min_val=min(zh1.min_val, zh2.min_val),
            max_val=max(zh1.max_val, zh2.max_val)
        )
        
        if has_pr_abs:
            merged.prd = zh1.prd
            merged.abd = zh1.abd
            merged.prf = zh2.prf
            merged.abf = zh2.abf
        
        return merged
    
    def _write_results(self, zh_list: List[ZHomogene], theme_name: str,
                      attribute_name: str, has_pr_abs: bool) -> str:
        """Write results to database table"""
        cursor = self.connection.cursor()
        
        # Create result table name
        attr_normalized = attribute_name.strip().replace(" ", "_")
        result_table = f"zh_{theme_name}_{attr_normalized}"
        
        # Drop existing table
        try:
            drop_query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                schema=sql.Identifier(self.schema_name),
                table=sql.Identifier(result_table)
            )
            cursor.execute(drop_query)
            self.connection.commit()
        except Exception as e:
            print(f"Warning dropping table: {e}")
            self.connection.rollback()
        
        # Create new table
        if has_pr_abs:
            create_query = sql.SQL("""
                CREATE TABLE {schema}.{table} (
                    id SERIAL PRIMARY KEY,
                    section_id TEXT,
                    cumuld DOUBLE PRECISION,
                    cumulf DOUBLE PRECISION,
                    {mean_col} DOUBLE PRECISION,
                    prd TEXT,
                    abd TEXT,
                    prf TEXT,
                    abf TEXT
                )
            """).format(
                schema=sql.Identifier(self.schema_name),
                table=sql.Identifier(result_table),
                mean_col=sql.Identifier(f"mean_{attr_normalized}")
            )
        else:
            create_query = sql.SQL("""
                CREATE TABLE {schema}.{table} (
                    id SERIAL PRIMARY KEY,
                    section_id TEXT,
                    cumuld DOUBLE PRECISION,
                    cumulf DOUBLE PRECISION,
                    {mean_col} DOUBLE PRECISION
                )
            """).format(
                schema=sql.Identifier(self.schema_name),
                table=sql.Identifier(result_table),
                mean_col=sql.Identifier(f"mean_{attr_normalized}")
            )
        
        cursor.execute(create_query)
        self.connection.commit()
        
        # Insert data
        if has_pr_abs:
            insert_query = sql.SQL("""
                INSERT INTO {schema}.{table} 
                (section_id, cumuld, cumulf, {mean_col}, prd, abd, prf, abf)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """).format(
                schema=sql.Identifier(self.schema_name),
                table=sql.Identifier(result_table),
                mean_col=sql.Identifier(f"mean_{attr_normalized}")
            )
            for zh in zh_list:
                cursor.execute(insert_query, (
                    zh.section_id, zh.cumuld, zh.cumulf, zh.mean_val,
                    zh.prd, zh.abd, zh.prf, zh.abf
                ))
        else:
            insert_query = sql.SQL("""
                INSERT INTO {schema}.{table} 
                (section_id, cumuld, cumulf, {mean_col})
                VALUES (%s, %s, %s, %s)
            """).format(
                schema=sql.Identifier(self.schema_name),
                table=sql.Identifier(result_table),
                mean_col=sql.Identifier(f"mean_{attr_normalized}")
            )
            for zh in zh_list:
                cursor.execute(insert_query, (
                    zh.section_id, zh.cumuld, zh.cumulf, zh.mean_val
                ))
        
        self.connection.commit()
        cursor.close()
        return result_table


# Example usage
if __name__ == "__main__":
    # Configuration
    HOST = "localhost"
    PORT = 5433
    DATABASE = "cd12_demo"
    USER = "diagway"
    PASSWORD = "diagway"
    SCHEMA_NAME = "mdb"
    
    THEME_NAME = "note_num"
    ATTRIBUTE_NAME = "note_num"
    U_THRESHOLD = 0.2  # Maximum difference from mean
    MIN_LENGTH = 200.0  # Minimum zone length
    
    # Calculate homogeneous zones
    calculator = HomogeneousZoneCalculator(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        schema_name=SCHEMA_NAME
    )
    
    try:
        calculator.connect()
        
        # List available themes
        themes = calculator.get_value_themes()
        print("Available themes:")
        for theme, loc_type in themes:
            print(f"  - {theme} ({'Linéaire' if loc_type == 1 else 'Ponctuelle'})")
        
        # Get attributes for a theme
        if themes:
            attributes = calculator.get_value_attributes(THEME_NAME)
            print(f"\nAttributes for {THEME_NAME}:")
            for attr in attributes:
                print(f"  - {attr}")
        
        # Calculate zones
        result_table = calculator.calculate_zh(
            theme_name=THEME_NAME,
            attribute_name=ATTRIBUTE_NAME,
            u_threshold=U_THRESHOLD,
            min_length=MIN_LENGTH,
            refine=True
        )
        
        print(f"\nResults written to table: {SCHEMA_NAME}.{result_table}")
        
    finally:
        calculator.close()