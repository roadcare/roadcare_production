# Quick Start Guide - RoadCare Export Tool

## 🚀 Fast Setup (5 minutes)

### Step 1: Install Dependencies
```bash
pip install psycopg2-binary python-dotenv
```

### Step 2: Configure Database Connection

**Option A - Using .env file (Recommended)**
```bash
# Create .env file
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use any text editor
```

**Option B - Direct edit**
Edit `export_roadcare_sessions.py` lines 15-21 with your database info.

### Step 3: Run the Script

```bash
# Using environment variables:
python export_roadcare_sessions_env.py

# OR using hardcoded values:
python export_roadcare_sessions.py
```

### Step 4: Find Your CSV

The exported file will be in:
- Basic version: Current directory
- Env version: `./exports/` directory

Filename format: `roadcare_sessions_YYYYMMDD_HHMMSS.csv`

---

## 📊 What Gets Exported

**CSV Columns:**
1. `organization_id` - Organization UUID
2. `organization_name` - Organization name  
3. `session_id` - Session UUID
4. `session_name` - Session name
5. `acquisition_date` - When session was acquired

**Filters:**
- ✅ Includes: All sessions except those marked 'toDelete'
- ❌ Excludes: Sessions with state = 'toDelete'

---

## 💡 Example

```csv
organization_id,organization_name,session_id,session_name,acquisition_date
550e8400-e29b...,City Roads Dept,660e8400-e29b...,Main St,2024-11-10 14:30:00+00:00
```

---

## 🔧 Troubleshooting

**"Connection failed"**
- Check database host, username, password
- Verify firewall allows your IP
- Ensure SSL is configured correctly

**"No data found"**
- Check if sessions exist with state != 'toDelete'
- Verify sessions have metadata records

**"Module not found"**
```bash
pip install psycopg2-binary python-dotenv
```

---

## 📝 Your Database Info

Remember to update these in `.env`:
```
DB_HOST=your_host.postgres.database.azure.com
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

---

Need help? Check the full README.md for detailed information.
