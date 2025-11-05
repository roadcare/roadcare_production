# ImageObsoleteUpdater

## Overview

The `ImageObsoleteUpdater` is a high-performance Python tool designed to identify and mark obsolete images in a road network acquisition system. It processes images captured along road networks and determines which images should be marked as obsolete based on spatial proximity, temporal factors, and quality metrics.

## Purpose

In a road network image acquisition system, multiple acquisition sessions may capture images of the same road segments at different times. This tool helps maintain data quality by identifying redundant or outdated images based on:
- **Location**: Images at the same position on the road network (using linear referencing)
- **Time**: Newer captures supersede older ones
- **Quality**: Better quality images are preferred over lower quality ones
- **Direction**: Images captured in different directions (sens +/-)

## Key Concepts

### Linear Referencing
The system uses **linear referencing** to position images along road segments:
- Each image has a `cumuld` value representing its position along a route (axe)
- Routes are defined in `client.route_client` and calibrated in `client.troncon_client`
- Images are grouped by `axe` (route name) for processing

### Distance Threshold
The `distance_threshold` parameter (default: 6 meters) defines how close images must be to be considered "at the same location" and compared for obsolescence.

## Business Rules

The tool applies sophisticated business rules to determine which images should be marked as obsolete:

### Rule 1: Same Session Images
When two images belong to the same acquisition session AND are within `distance_threshold` of each other:

**Sub-rule 1a**: If `cumuld_session` difference ≤ 100m:
- Images are considered part of a continuous sequence
- **No action taken** (both images kept)

**Sub-rule 1b**: If `cumuld_session` difference > 100m (or null):
- Images are considered separate occurrences of the same location
- **Different `sens`**: Mark images with `sens='+'` as obsolete
- **Same `sens`**: Mark the image with the smaller `index` as obsolete

### Rule 2: Different Session Images
When two images belong to different sessions AND are within `distance_threshold`:

**Different `sens`**: 
- Mark images with `sens='+'` as obsolete

**Same `sens`**:
- **If capture dates differ by > 30 days**: Mark the older image as obsolete
- **If capture dates differ by ≤ 30 days**: Mark the image with higher `note_globale` (worse quality) as obsolete

## Architecture

### Multi-Processing Design
The tool uses Python's `multiprocessing` module to process multiple routes (axes) in parallel:
- Each route is processed independently by a worker process
- Default: Uses all available CPU cores
- Configurable via `num_processes` parameter

### Database Schema
Works with PostgreSQL/PostGIS database containing:

**Schema: public**
- `image`: Main table with captured images and their linear referencing attributes
- `session`: Acquisition sessions (itineraries)

**Key fields in `image` table:**
- `id`: Unique identifier (UUID)
- `axe`: Route name from linear referencing
- `cumuld`: Position on route (linear measure)
- `cumuld_session`: Distance from session start
- `session_id`: Acquisition session identifier
- `sens`: Direction indicator (+/-)
- `index`: Image sequence number in session
- `captureDate`: When the image was captured
- `note_globale`: Quality score (higher = worse)
- `obsolette`: Boolean flag (target field to update)

## Processing Steps

### Step -1: Primary Key Check
- Verifies if the `id` column is a PRIMARY KEY
- Checks for duplicate IDs in the table
- Logs warnings if duplicates are found

### Step 0: Reset Obsolete Flags
- Sets all `obsolette` flags to `false` for the specified routes (or all routes)
- Ensures clean state before processing

### Step 1: Data Loading
- Loads all non-obsolete images from the database
- Groups images by `axe` (route)
- Orders by `axe`, `cumuld` for efficient processing
- Optionally filters by specific route list

### Step 2: Parallel Processing Preparation
- Creates work items (tuples) for each route with ≥2 images
- Each work item contains: `(axe_name, records_data, distance_threshold)`
- Distributes work across worker processes

### Step 3: Parallel Processing
- Each worker processes one route at a time
- Uses vectorized NumPy operations for performance
- For each image, finds nearby images within `distance_threshold`
- Applies business rules to determine obsolete images
- Returns set of image IDs to mark as obsolete

### Step 4: Batch Update
- Collects obsolete IDs from all workers
- Updates database in batches (default: 5000 records per batch)
- Sets `obsolette = true` for identified images
- Commits changes to database

## Installation

### Prerequisites
```bash
pip install psycopg2-binary numpy pandas --break-system-packages
```

### Required Python Packages
- `psycopg2`: PostgreSQL database adapter
- `numpy`: Numerical operations and vectorization
- `pandas`: Data manipulation (imported but not heavily used)
- `multiprocessing`: Standard library (no installation needed)
- `logging`: Standard library (no installation needed)

## Usage

### Basic Usage

```python
from imageobsoleteupdater import ImageObsoleteUpdater

# Database configuration
db_config = {
    'host': 'localhost',
    'database': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'port': 5432
}

# Create updater instance with default settings
updater = ImageObsoleteUpdater(db_config)

# Process all routes
updater.process_all_axes_parallel()
```

### Custom Distance Threshold

```python
# Use a larger distance threshold (10 meters instead of default 6)
updater = ImageObsoleteUpdater(
    db_config=db_config,
    distance_threshold=10
)

updater.process_all_axes_parallel()
```

### Specific Routes Only

```python
# Process only specific routes
updater = ImageObsoleteUpdater(db_config)

# Define routes to process
specific_routes = ['D1', 'D2', 'N7']

# Process only these routes
updater.process_all_axes_parallel(axe_list=specific_routes)
```

### Custom Number of Processes

```python
# Use 4 worker processes instead of all CPU cores
updater = ImageObsoleteUpdater(
    db_config=db_config,
    num_processes=4,
    distance_threshold=6
)

updater.process_all_axes_parallel()
```

### Complete Example with All Options

```python
import logging
from imageobsoleteupdater import ImageObsoleteUpdater

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration
db_config = {
    'host': 'localhost',
    'database': 'roadare_sig_prod',
    'user': 'postgres',
    'password': 'secure_password',
    'port': 5432
}

# Create updater with custom settings
updater = ImageObsoleteUpdater(
    db_config=db_config,
    num_processes=8,           # Use 8 parallel processes
    distance_threshold=8       # Images within 8 meters are compared
)

# Option 1: Process all routes
total_updated = updater.process_all_axes_parallel()
print(f"Total images marked as obsolete: {total_updated}")

# Option 2: Process specific routes
specific_routes = ['A1', 'A2', 'A3']
total_updated = updater.process_all_axes_parallel(axe_list=specific_routes)
print(f"Total images marked as obsolete for routes {specific_routes}: {total_updated}")
```

## Configuration Parameters

### `db_config` (dict, required)
Database connection parameters:
- `host`: Database server hostname
- `database`: Database name
- `user`: Database username
- `password`: Database password
- `port`: Database port (default: 5432)

### `num_processes` (int, optional)
Number of parallel worker processes:
- **Default**: Auto-detect (uses `cpu_count()`)
- **Recommended**: Leave as default for optimal performance
- **Custom**: Set to specific number if you want to limit CPU usage

### `distance_threshold` (float, optional)
Distance threshold in meters for comparing images:
- **Default**: 6 meters
- **Purpose**: Images within this distance are considered "at the same location"
- **Impact**: 
  - Smaller values: More strict, fewer images marked obsolete
  - Larger values: More lenient, more images marked obsolete
- **Typical range**: 4-10 meters depending on GPS accuracy and road geometry

### `axe_list` (list, optional)
List of specific route names to process:
- **Default**: `None` (processes all routes)
- **Format**: List of strings, e.g., `['D1', 'D2', 'N7']`
- **Use case**: Process specific routes after new acquisitions

## Performance Optimization

### Vectorized Operations
The tool uses NumPy's vectorized operations for significant performance gains:
- Batch comparison of distances
- Efficient filtering using boolean masks
- Minimal Python loop overhead

### Multiprocessing
- Processes multiple routes simultaneously
- Scales linearly with CPU cores
- Each route is independent, avoiding data sharing overhead

### Batch Updates
- Updates database in batches (5000 records default)
- Reduces database round trips
- Single commit per processing run

### Memory Efficiency
- Loads data grouped by route
- Converts to structured NumPy arrays for compact storage
- Releases memory after each route is processed

## Expected Performance

For a typical dataset:
- **Small**: <100K images, <50 routes → ~30 seconds
- **Medium**: 100K-1M images, 50-200 routes → 2-5 minutes
- **Large**: >1M images, >200 routes → 5-15 minutes

Performance depends on:
- Number of CPU cores
- Database response time
- Number of images per route
- Distance threshold (affects comparison count)

## Logging

The tool provides detailed logging at each step:

```
2025-01-15 10:30:00 - INFO - Using 12 processes for parallel processing
2025-01-15 10:30:00 - INFO - Distance threshold set to: 6
2025-01-15 10:30:00 - INFO - Step -1: Checking primary key and duplicates...
2025-01-15 10:30:01 - INFO - ✓ Column 'id' is a PRIMARY KEY - no duplicates expected
2025-01-15 10:30:01 - INFO - Step 0: Resetting obsolete flags...
2025-01-15 10:30:02 - INFO - Reset 150000 records to obsolette = false
2025-01-15 10:30:02 - INFO - Step 1: Loading data from database...
2025-01-15 10:30:05 - INFO - Loaded 120 axes with total records
2025-01-15 10:30:05 - INFO - Step 2: Preparing parallel processing tasks...
2025-01-15 10:30:05 - INFO - Processing 118 axes in parallel...
2025-01-15 10:30:05 - INFO - Step 3: Processing data in parallel...
2025-01-15 10:30:15 - INFO - Axe 'D1': found 1234 records to mark obsolete
2025-01-15 10:30:16 - INFO - Axe 'N7': found 856 records to mark obsolete
...
2025-01-15 10:30:25 - INFO - Found 45678 total records to mark obsolete
2025-01-15 10:30:25 - INFO - Step 4: Updating database...
2025-01-15 10:30:30 - INFO - ✓ Successfully updated 45678 records (1:1 ratio - no duplicates)
2025-01-15 10:30:30 - INFO - Processing complete! Total time: 0:00:30
2025-01-15 10:30:30 - INFO - Records marked obsolete: 45678
```

## Troubleshooting

### Issue: Duplicate ID warnings
**Symptom**: Log shows "✗ Column 'id' is NOT a PRIMARY KEY"

**Solution**: 
- Add PRIMARY KEY constraint: `ALTER TABLE public.image ADD PRIMARY KEY (id);`
- Or investigate and remove duplicate IDs

### Issue: Out of memory
**Symptom**: Process crashes with memory error

**Solutions**:
- Reduce `num_processes` to use less memory
- Process routes in smaller batches using `axe_list`
- Increase system RAM or use swap space

### Issue: Slow performance
**Symptom**: Processing takes much longer than expected

**Solutions**:
- Ensure database has indexes on: `axe`, `cumuld`, `session_id`, `obsolette`
- Check database connection latency
- Verify CPU usage is high (multiprocessing working)
- Reduce `distance_threshold` to compare fewer image pairs

### Issue: Database connection errors
**Symptom**: Connection timeout or authentication failure

**Solutions**:
- Verify database credentials in `db_config`
- Check network connectivity to database server
- Ensure PostgreSQL accepts connections from your host
- Check PostgreSQL `pg_hba.conf` settings

## Database Indexes (Recommended)

For optimal performance, create these indexes:

```sql
-- Index on axe for grouping and filtering
CREATE INDEX IF NOT EXISTS idx_image_axe ON public.image(axe);

-- Index on obsolette for filtering non-obsolete records
CREATE INDEX IF NOT EXISTS idx_image_obsolette ON public.image(obsolette);

-- Composite index for main query
CREATE INDEX IF NOT EXISTS idx_image_axe_obsolette_cumuld 
    ON public.image(axe, obsolette, cumuld);

-- Index on session_id for rule processing
CREATE INDEX IF NOT EXISTS idx_image_session_id ON public.image(session_id);
```

## Safety Features

### Idempotent Operation
- Always resets `obsolette` flags before processing
- Can be run multiple times safely
- Results are deterministic

### Transaction Management
- Uses database transactions for all updates
- Rolls back on error
- Ensures data consistency

### Primary Key Validation
- Checks for duplicate IDs before processing
- Warns user if duplicates exist
- Logs ratio of IDs to rows updated

## Advanced Usage

### Running as a Scheduled Task

```python
# schedule_obsolete_check.py
import schedule
import time
from imageobsoleteupdater import ImageObsoleteUpdater

def run_obsolete_check():
    db_config = {
        'host': 'localhost',
        'database': 'roadare_sig_prod',
        'user': 'postgres',
        'password': 'password',
        'port': 5432
    }
    
    updater = ImageObsoleteUpdater(db_config)
    updater.process_all_axes_parallel()

# Run every day at 2:00 AM
schedule.every().day.at("02:00").do(run_obsolete_check)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Processing Recent Acquisitions Only

```python
# Process only routes that have new acquisitions
from datetime import datetime, timedelta

# Get routes with acquisitions in last 7 days
# (implement custom query to get recent routes)
recent_routes = get_routes_with_recent_acquisitions(days=7)

updater = ImageObsoleteUpdater(db_config)
updater.process_all_axes_parallel(axe_list=recent_routes)
```

### Custom Reset Strategy

```python
# Don't reset all flags, only specific routes
updater = ImageObsoleteUpdater(db_config)

specific_routes = ['D1', 'D2']

# Manual reset for specific routes
updater.reset_all_obsolete_flags(axe_list=specific_routes)

# Then process
updater.process_all_axes_parallel(axe_list=specific_routes)
```

## Contributing

When modifying the tool:
1. Maintain the business rules logic in `apply_business_rules_numpy()`
2. Keep vectorized operations for performance
3. Update tests when changing distance calculations
4. Document any new parameters or behavior

## License

[Specify your license here]

## Support

For issues or questions:
- Check troubleshooting section above
- Review logs for detailed error messages
- Verify database connection and schema
- Ensure all required fields are populated in `image` table

## Version History

### v1.1.0
- Added configurable `distance_threshold` parameter
- Improved logging with threshold information
- Enhanced documentation

### v1.0.0
- Initial release
- Multi-processing support
- Vectorized NumPy operations
- Comprehensive business rules implementation
