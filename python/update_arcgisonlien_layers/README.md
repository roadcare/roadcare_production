# ArcGIS Online Feature Layer Update Script

This Python script updates the `note_classe` field in two ArcGIS Online feature layers based on numeric rating values.

## What it does

### Layer 1: image_note (Layer 0)
- Updates field: `note_classe`
- Based on field: `note_globale`

### Layer 2: zh_u02_l200 (Layer 2)
- Updates field: `note_classe`
- Based on field: `note_num`

### Classification Logic

Both layers use the same classification:
- **1-Bon**: note >= 0.8
- **2-Moyen+**: note >= 0.6 and < 0.8
- **3-Moyen-**: note >= 0.4 and < 0.6
- **4-Mauvais**: note < 0.4

## Installation

1. Install Python 3.7 or higher
2. Install required packages:
```bash
pip install -r requirements.txt
```

Or install directly:
```bash
pip install arcgis
```

## Troubleshooting Field Names

If you get an error about invalid field names, first run the diagnostic script to see what fields actually exist:

```bash
python inspect_layers.py
```

This will show you:
- All available fields in each layer
- Field types and properties
- Sample data from the first feature

Use this information to verify the correct field names (they are case-sensitive!).

## Usage

Simply run the script:
```bash
python update_arcgis_layers.py
```

The script will:
1. Connect to ArcGIS Online
2. Query both feature layers
3. Calculate the classification for each feature
4. Update only the features that have changed
5. Display progress and summary statistics

## Output

The script provides detailed feedback:
- Authentication confirmation
- Number of features found
- Number of features updated vs skipped
- Batch processing progress
- Success/error messages

## Security Note

⚠️ **Important**: The script contains hardcoded credentials. For production use:
- Store credentials in environment variables
- Use a configuration file (not committed to version control)
- Consider using OAuth authentication

## Troubleshooting

### Common Issues

1. **"Invalid query parameters" or "'outFields' parameter is invalid"**: 
   - The field names might not match exactly (they are case-sensitive!)
   - Run `python inspect_layers.py` to see the actual field names in your layers
   - Update the field names in `update_arcgis_layers.py` if needed
2. **Authentication Error**: Verify username and password are correct
3. **Connection Error**: Check internet connection and firewall settings
4. **Permission Error**: Ensure your account has edit permissions on the layers
5. **Field Not Found**: The script will now show you which fields exist and warn you if a field is missing

## Requirements

- Python 3.7+
- arcgis Python API
- Internet connection
- ArcGIS Online account with edit permissions
