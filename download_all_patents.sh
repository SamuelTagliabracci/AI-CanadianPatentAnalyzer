#!/bin/bash

# Full Canadian Patents Download Script
# This script runs the patent fetcher to download ALL available patents

echo "🍁 Starting Full Canadian Patents Download Process"
echo "=========================================="
echo "⚠️  WARNING: This will download ALL patents from the Canadian government database"
echo "   This could take several hours and result in a database of 100MB+ in size"
echo "   Make sure you have enough disk space and a stable internet connection"
echo ""

# Check if database exists
if [ -f "cnd_patents.db" ]; then
    current_count=$(python3 -c "import sqlite3; conn=sqlite3.connect('cnd_patents.db'); print(conn.execute('SELECT COUNT(*) FROM patents').fetchone()[0]); conn.close()" 2>/dev/null || echo "0")
    echo "📊 Current patents in database: ${current_count}"
else
    echo "📊 Starting with empty database"
fi

echo ""
read -p "Do you want to continue with the full download? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Download cancelled"
    exit 1
fi

echo ""
echo "🚀 Starting download process..."
echo "📝 This will automatically:"
echo "   1. Connect to Government of Canada CKAN API"
echo "   2. Download all patent ZIP files"  
echo "   3. Extract and parse all CSV data"
echo "   4. Import all records into the database"
echo ""
echo "⏱️  Estimated time: 2-6 hours depending on internet speed"
echo "💾 Expected final database size: 100-500MB"
echo ""

# Activate virtual environment and run the patent fetcher
echo "🐍 Activating virtual environment..."
source venv/bin/activate

echo "📥 Starting patent data fetch (press Ctrl+C to stop)..."

# Run the fetcher automatically with option 1 (fetch data)
python3 pull_patents.py << 'EOF'
1
6
EOF

# Check final count
if [ -f "cnd_patents.db" ]; then
    final_count=$(python3 -c "import sqlite3; conn=sqlite3.connect('cnd_patents.db'); print(conn.execute('SELECT COUNT(*) FROM patents').fetchone()[0]); conn.close()" 2>/dev/null || echo "unknown")
    echo ""
    echo "✅ Download process completed!"
    echo "📊 Total patents in database: ${final_count}"
    
    # Show database size
    db_size=$(ls -lh cnd_patents.db | awk '{print $5}')
    echo "💾 Database file size: ${db_size}"
else
    echo "❌ Something went wrong - database not found"
fi

echo ""
echo "🎉 Full patent download process finished!"
echo "   You can now start the web application with: ./start.sh"