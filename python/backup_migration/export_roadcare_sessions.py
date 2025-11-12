#!/usr/bin/env python3
"""
RoadCare Session Export Script
Exports organization and session data to CSV file
"""

import psycopg2
import csv
from datetime import datetime
import os

# Database connection parameters
DB_CONFIG = {
    'host': 'your_host.postgres.database.azure.com',
    'database': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'port': 5432,
    'sslmode': 'require'  # Azure PostgreSQL requires SSL
}

# Output file name
OUTPUT_FILE = f'roadcare_sessions_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'


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
        cursor.execute(query)
        
        # Fetch all results
        rows = cursor.fetchall()
        row_count = len(rows)
        
        if row_count == 0:
            print("⚠ No data found matching the criteria")
            return
        
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
        
        print(f"✓ Successfully exported {row_count} sessions to: {output_file}")
        print(f"  File size: {os.path.getsize(output_file)} bytes")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"✗ Database error: {e}")
        raise
    except IOError as e:
        print(f"✗ File writing error: {e}")
        raise


def main():
    """
    Main execution function
    """
    print("=" * 60)
    print("RoadCare Session Export Tool")
    print("=" * 60)
    
    connection = None
    
    try:
        # Connect to database
        connection = connect_to_database()
        
        # Export data to CSV
        export_sessions_to_csv(connection, OUTPUT_FILE)
        
        print("=" * 60)
        print("Export completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Export failed: {e}")
        return 1
    
    finally:
        # Close database connection
        if connection:
            connection.close()
            print("✓ Database connection closed")
    
    return 0


if __name__ == "__main__":
    exit(main())
