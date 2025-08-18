#!/bin/bash

# Dutch Parliament Transcript Scraper Runner
# This script sets up and runs the scraper with proper logging

echo "🏛️  Dutch Parliament Transcript Scraper"
echo "======================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

# Check if pip is available  
if ! command -v pip &> /dev/null; then
    echo "❌ pip is required but not installed."
    exit 1
fi

# Install dependencies if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies."
        exit 1
    fi
else
    echo "⚠️  requirements.txt not found. Make sure dependencies are installed manually."
fi

# Create output directory if it doesn't exist
if [ ! -d "output" ]; then
    echo "📁 Creating output directory..."
    mkdir -p output
fi

# Check available disk space (warn if less than 1GB)
available_space=$(df . | awk 'NR==2 {print $4}')
if [ "$available_space" -lt 1048576 ]; then
    echo "⚠️  Warning: Less than 1GB of disk space available."
    echo "   The full dataset may require several GB of storage."
    read -p "   Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "🚀 Starting scraper..."
echo "   - Full dataset: ~1,460 meetings, ~3,559 reports"
echo "   - Output directory: $(pwd)/output"
echo "   - Use Ctrl+C to stop gracefully"
echo ""

# Run the scraper with debug mode for better visibility
python3 scrape.py --debug

echo ""
echo "✅ Scraping completed!"
echo "   Check the output directory for JSON files: $(pwd)/output"