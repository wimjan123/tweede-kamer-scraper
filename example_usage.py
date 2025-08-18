#!/usr/bin/env python3
"""
Example usage of the Dutch Parliament Transcript Scraper
This demonstrates how to use the scraper programmatically
"""

import json
import os
from scrape import DutchParliamentScraper

def example_basic_usage():
    """Basic usage example - scrape a few meetings with debug output"""
    print("🏛️  Basic Usage Example")
    print("=" * 50)
    
    # Initialize scraper with debug mode
    scraper = DutchParliamentScraper(output_dir="example_output", debug=True)
    
    # This will scrape all meetings - you can modify the script to limit this
    print("Note: This will attempt to scrape all 1,460+ meetings!")
    print("Consider modifying scrape.py to limit the number for testing.")
    
    scraper.run()

def example_analyze_output():
    """Example of analyzing the scraped JSON output"""
    print("\n📊 Output Analysis Example")
    print("=" * 50)
    
    output_dir = "output"
    if not os.path.exists(output_dir):
        print("No output directory found. Run the scraper first!")
        return
    
    # Count files and analyze content
    json_files = [f for f in os.listdir(output_dir) if f.endswith('.json')]
    
    if not json_files:
        print("No JSON files found. Run the scraper first!")
        return
    
    print(f"Found {len(json_files)} scraped meeting files")
    
    # Analyze first few files
    for i, filename in enumerate(json_files[:3]):
        filepath = os.path.join(output_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"\n📄 File {i+1}: {filename}")
            print(f"   Title: {data.get('title', 'N/A')}")
            print(f"   Date: {data.get('date', 'N/A')}")
            print(f"   Segments: {len(data.get('segments', []))}")
            
            # Show speaker info from first segment
            segments = data.get('segments', [])
            if segments:
                first_speaker = segments[0].get('speaker', {})
                print(f"   First speaker: {first_speaker.get('name', 'N/A')} ({first_speaker.get('party', 'N/A')})")
                print(f"   First text: {segments[0].get('text', '')[:100]}...")
            
        except Exception as e:
            print(f"   Error reading {filename}: {e}")

def example_custom_scraper():
    """Example of using the scraper with custom settings"""
    print("\n⚙️  Custom Scraper Example")
    print("=" * 50)
    
    # Custom scraper with different output directory
    scraper = DutchParliamentScraper(
        output_dir="custom_output",
        debug=False  # Less verbose output
    )
    
    # You could modify the scraper class to add filtering
    # For example, only scrape meetings from a specific date range
    print("This example shows how to customize the scraper settings.")
    print("You can modify the DutchParliamentScraper class to add:")
    print("- Date range filtering")
    print("- Speaker name filtering") 
    print("- Party-specific analysis")
    print("- Custom output formats")

def example_data_analysis():
    """Example of basic data analysis on scraped transcripts"""
    print("\n🔍 Data Analysis Example")
    print("=" * 50)
    
    output_dir = "output"
    if not os.path.exists(output_dir):
        print("No output directory found. Run the scraper first!")
        return
    
    all_speakers = {}
    all_parties = {}
    total_segments = 0
    
    json_files = [f for f in os.listdir(output_dir) if f.endswith('.json')]
    
    for filename in json_files[:10]:  # Analyze first 10 files as example
        filepath = os.path.join(output_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for segment in data.get('segments', []):
                speaker_info = segment.get('speaker', {})
                name = speaker_info.get('name', 'Unknown')
                party = speaker_info.get('party', 'Unknown')
                
                all_speakers[name] = all_speakers.get(name, 0) + 1
                all_parties[party] = all_parties.get(party, 0) + 1
                total_segments += 1
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    print(f"Analysis of first {len(json_files[:10])} meetings:")
    print(f"Total segments: {total_segments}")
    print(f"Unique speakers: {len(all_speakers)}")
    print(f"Unique parties: {len(all_parties)}")
    
    # Top speakers
    top_speakers = sorted(all_speakers.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\nTop speakers:")
    for name, count in top_speakers:
        print(f"  {name}: {count} segments")
    
    # Top parties  
    top_parties = sorted(all_parties.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\nTop parties:")
    for party, count in top_parties:
        print(f"  {party}: {count} segments")

if __name__ == "__main__":
    print("🏛️  Dutch Parliament Scraper - Usage Examples")
    print("=" * 60)
    
    print("\n1. Basic usage:")
    print("   python scrape.py --debug")
    print("   or")  
    print("   ./run.sh")
    
    print("\n2. Programmatic usage:")
    print("   Run the functions in this file to see different examples")
    
    # Run analysis if output exists
    example_analyze_output()
    example_data_analysis()
    
    print("\n✅ Examples completed!")
    print("   Check the README.md for more detailed instructions.")