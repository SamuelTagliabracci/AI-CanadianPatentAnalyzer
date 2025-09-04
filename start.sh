#!/bin/bash

# Canadian Patents Browser - Startup Script
# This script activates the virtual environment and starts the Flask application

echo "🏁 Starting Canadian Patents Browser..."
echo "📍 Working directory: $(pwd)"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please create one first:"
    echo "   python3 -m venv venv"
    echo "   source venv/bin/activate"
    echo "   pip install -r requirements.txt"
    exit 1
fi

# Check if database exists
if [ ! -f "cnd_patents.db" ]; then
    echo "📄 Database file not found: cnd_patents.db"
    echo "🔨 Creating database schema automatically..."
    echo "   (Flask app will initialize the database on startup)"
fi

# Activate virtual environment and start Flask app
echo "🐍 Activating virtual environment..."
source venv/bin/activate

echo "🚀 Starting Flask application..."
echo "📊 Database: cnd_patents.db"
echo "🌐 Access the app at: http://localhost:5000"
echo "🌐 Network access at: http://$(hostname -I | awk '{print $1}'):5000"
echo ""
echo "Press Ctrl+C to stop the application"
echo "=================================="

python3 app.py
