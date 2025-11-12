#!/usr/bin/env python3
"""
RoadCare Session Export Script (with .env support)
Exports organization and session data to CSV file
"""

import psycopg2
import csv
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'roadcare'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': int(os.getenv('DB_PORT', 5432)),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

# Output directory
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './exports')


def ensure_output_directory():
    """
    Create output directory if it doesn't exist
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"✓ Created output directory: {OUTPUT_DIR}")


def connect_to_database():
    """
    Establish connection to PostgreSQL database
    """
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Successfully connected to database: {DB_CONFIG['database']}")
        return connection
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise


def export_sessions_to_csv(connection, output_file):
    """
    Query database and export sessions to CSV file
    """
    query = """
        SELECT 
            o.id as organization_id,
            o.name as organization_name,
            s.id as session_id,
            s.name as session_name,
            sm.acquisition_date
        FROM public.session s
        INNER JOIN public.organization o ON s."organizationId" = o.id
        INNER JOIN public.session_metadata sm ON s."metadataId" = sm.id
        WHERE s.state IS DISTINCT FROM 'toDelete'
        ORDER BY o.name, sm.acquisition_date DESC, s.name
    """
    
    try:
        cursor = connection.cursor()
        
        print("Executing query...")
        cursor.execute(query)
        
        # Fetch all results
        rows = cursor.fetchall()
        row_count = len(rows)
        
        if row_count == 0:
            print("⚠ No data found matching the criteria")
            cursor.close()
            return None
        
        print(f"✓ Retrieved {row_count} sessions from database")
        
        # Write to CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            
            # Write header
            csv_writer.writerow([
                'organization_id',
                'organization_name',
                'session_id',
                'session_name',
                'acquisition_date'
            ])
            
            # Write data rows
            csv_writer.writerows(rows)
        
        file_size = os.path.getsize(output_file)
        print(f"✓ Successfully exported to: {output_file}")
        print(f"  File size: {file_size:,} bytes ({file_size / 1024:.2f} KB)")
        
        cursor.close()
        return row_count
        
    except psycopg2.Error as e:
        print(f"✗ Database error: {e}")
        raise
    except IOError as e:
        print(f"✗ File writing error: {e}")
        raise


def get_session_statistics(connection):
    """
    Get statistics about sessions in the database
    """
    stats_query = """
        SELECT 
            COUNT(*) as total_sessions,
            COUNT(DISTINCT s."organizationId") as total_organizations,
            COUNT(CASE WHEN s.state = 'toDelete' THEN 1 END) as sessions_to_delete,
            COUNT(CASE WHEN s.state IS DISTINCT FROM 'toDelete' THEN 1 END) as active_sessions
        FROM public.session s
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(stats_query)
        stats = cursor.fetchone()
        cursor.close()
        
        print("\nDatabase Statistics:")
        print(f"  Total sessions: {stats[0]}")
        print(f"  Total organizations: {stats[1]}")
        print(f"  Sessions marked 'toDelete': {stats[2]}")
        print(f"  Active sessions (exported): {stats[3]}")
        
    except psycopg2.Error as e:
        print(f"⚠ Could not retrieve statistics: {e}")


def main():
    """
    Main execution function
    """
    print("=" * 60)
    print("RoadCare Session Export Tool")
    print("=" * 60)
    
    connection = None
    
    try:
        # Ensure output directory exists
        ensure_output_directory()
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(OUTPUT_DIR, f'roadcare_sessions_{timestamp}.csv')
        
        # Connect to database
        connection = connect_to_database()
        
        # Show statistics
        get_session_statistics(connection)
        
        print("\nExporting data...")
        
        # Export data to CSV
        row_count = export_sessions_to_csv(connection, output_file)
        
        if row_count:
            print("=" * 60)
            print("✓ Export completed successfully!")
            print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Export failed: {e}")
        return 1
    
    finally:
        # Close database connection
        if connection:
            connection.close()
            print("\n✓ Database connection closed")
    
    return 0


if __name__ == "__main__":
    exit(main())
