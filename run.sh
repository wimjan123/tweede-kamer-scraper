#!/bin/bash

# Dutch Parliament Transcript Scraper Runner
# This script sets up and runs the scraper with proper logging

echo "🏛️  Dutch Parliament Transcript Scraper"
echo "======================================="

usage() {
  cat <<USAGE
Usage:
  ./run.sh scrape                # Run full scraper (default)
  ./run.sh link <MEETING_ID>     # Print report URL for a meeting
  ./run.sh links [OUTPUT.json]   # Dump all meeting->report URLs

Examples:
  ./run.sh scrape
  ./run.sh link 1e4227e0-7c1f-4a8e-8c96-5d0e35fb8b9a
  ./run.sh links links.json
USAGE
}

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
    pip install -r requirements.txt || { echo "❌ Failed to install dependencies."; exit 1; }
else
    echo "⚠️  requirements.txt not found. Make sure dependencies are installed manually."
fi

CMD="$1"
case "$CMD" in
  ""|"scrape")
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

    python3 scrape.py --debug

    echo ""
    echo "✅ Scraping completed!"
    echo "   Check the output directory for JSON files: $(pwd)/output"
    ;;
  "link")
    MEETING_ID="$2"
    if [ -z "$MEETING_ID" ]; then
      echo "❌ Missing MEETING_ID."
      usage
      exit 1
    fi
    echo "🔗 Extracting report link for meeting: $MEETING_ID"
    python3 extract_link.py --meeting-id "$MEETING_ID"
    ;;
  "links")
    OUTPUT_FILE="$2"
    if [ -n "$OUTPUT_FILE" ]; then
      echo "🧾 Dumping all meeting->report links to: $OUTPUT_FILE"
      python3 extract_link.py --all --output "$OUTPUT_FILE"
    else
      echo "🧾 Dumping all meeting->report links to stdout"
      python3 extract_link.py --all
    fi
    ;;
  "help"|"-h"|"--help")
    usage
    ;;
  *)
    echo "❌ Unknown command: $CMD"
    usage
    exit 1
    ;;
esac
