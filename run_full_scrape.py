#!/usr/bin/env python3
"""
Script to run a full scrape of all available parliament data.
This will discover and download all available transcripts.
"""

import subprocess
import time
from pathlib import Path

def count_existing_files():
    """Count existing JSON files."""
    output_dir = Path("output")
    if not output_dir.exists():
        return 0
    return len(list(output_dir.glob("*.json")))

def main():
    """Run full scrape with status updates."""
    print("🏛️  Starting full Dutch Parliament transcript scrape...")
    print("=" * 60)
    
    initial_count = count_existing_files()
    print(f"📁 Starting with {initial_count} existing files")
    
    print("\n🔍 Phase 1: Discovery (finding all available meetings)...")
    # First, do a quick discovery run to see total scope
    discovery = subprocess.run([
        "python", "scrape.py", 
        "--delay", "0.1", 
        "--max-pages", "50"
    ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
    
    if discovery.returncode != 0:
        print(f"❌ Discovery failed: {discovery.stderr}")
        return
    
    # Extract totals from output
    for line in discovery.stdout.split('\n'):
        if "Total found:" in line and "plenary meetings" in line:
            print(f"📊 {line}")
        elif "Total found:" in line and "report mappings" in line:
            print(f"📊 {line}")
    
    print(f"\n📥 Phase 2: Full download (unlimited pages)...")
    print("⏱️  This may take a while. Progress will be shown...")
    print("⏹️  Press Ctrl+C to stop gracefully")
    
    try:
        # Run full scrape with no page limit
        subprocess.run([
            "python", "scrape.py",
            "--delay", "0.5"  # Be respectful to server
        ])
        
        final_count = count_existing_files()
        print(f"\n✅ Scrape completed!")
        print(f"📁 Downloaded {final_count - initial_count} new files")
        print(f"📁 Total files: {final_count}")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Scrape interrupted by user")
        final_count = count_existing_files()
        print(f"📁 Downloaded {final_count - initial_count} new files before stopping")
        print(f"📁 Total files: {final_count}")
        print("💡 You can resume by running the scraper again - it skips existing files")

if __name__ == "__main__":
    main()