# RoadCare Session Export Tool

Python script to export session data from the RoadCare PostgreSQL database to CSV format.

## Features

- Exports organization and session data with acquisition dates
- Filters out sessions marked as 'toDelete'
- Generates timestamped CSV files
- Supports Azure PostgreSQL with SSL
- Two versions: basic and environment-variable based

## Prerequisites

- Python 3.7 or higher
- Access to RoadCare PostgreSQL database
- Network access to Azure PostgreSQL instance

## Installation

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Basic Script (Hardcoded Configuration)

1. Edit `export_roadcare_sessions.py` and update the `DB_CONFIG` dictionary with your database credentials:

```python
DB_CONFIG = {
    'host': 'your_host.postgres.database.azure.com',
    'database': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'port': 5432,
    'sslmode': 'require'
}
```

2. Run the script:

```bash
python export_roadcare_sessions.py
```

### Option 2: Environment Variables (Recommended)

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Edit `.env` and fill in your database credentials:

```env
DB_HOST=your_host.postgres.database.azure.com
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432
DB_SSLMODE=require
OUTPUT_DIR=./exports
```

3. Run the script:

```bash
python export_roadcare_sessions_env.py
```

## Output

The script generates a CSV file with the following columns:

| Column Name         | Description                           |
|---------------------|---------------------------------------|
| organization_id     | UUID of the organization              |
| organization_name   | Name of the organization              |
| session_id          | UUID of the session                   |
| session_name        | Name of the session                   |
| acquisition_date    | Date when the session was acquired    |

### Output File Format

- Filename: `roadcare_sessions_YYYYMMDD_HHMMSS.csv`
- Location: Current directory (basic) or `./exports/` directory (env version)
- Encoding: UTF-8
- Separator: Comma

### Example Output

```csv
organization_id,organization_name,session_id,session_name,acquisition_date
550e8400-e29b-41d4-a716-446655440000,City Roads Dept,660e8400-e29b-41d4-a716-446655440001,Main Street Survey,2024-11-10 14:30:00+00:00
550e8400-e29b-41d4-a716-446655440000,City Roads Dept,660e8400-e29b-41d4-a716-446655440002,Highway 101 North,2024-11-08 09:15:00+00:00
```

## Query Details

The script executes the following SQL query:

```sql
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
```

### Filters Applied

- Only sessions where `state` is NOT 'toDelete'
- Also includes sessions where `state` is NULL

### Sorting

Results are sorted by:
1. Organization name (alphabetically)
2. Acquisition date (most recent first)
3. Session name (alphabetically)

## Database Schema

The script queries the following tables:

- `public.organization` - Organization information
- `public.session` - Session records
- `public.session_metadata` - Metadata including acquisition dates

## Error Handling

The script includes error handling for:

- Database connection failures
- SQL query errors
- File writing errors
- Missing data

## Security Notes

- **Never commit the `.env` file** to version control
- Add `.env` to your `.gitignore` file
- Use the environment variable version for production
- Ensure database credentials have read-only access if possible

## Troubleshooting

### Connection Failed

- Verify database credentials are correct
- Check network connectivity to Azure
- Ensure firewall rules allow your IP address
- Verify SSL/TLS settings

### No Data Exported

- Check if there are sessions with `state != 'toDelete'`
- Verify sessions have associated metadata records
- Check database permissions

### Import Errors

- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check Python version: `python --version`

## Dependencies

- `psycopg2-binary`: PostgreSQL database adapter
- `python-dotenv`: Environment variable loader (for env version)

## License

Internal tool for RoadCare application.

## Support

For issues or questions, contact the RoadCare development team.
