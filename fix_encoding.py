#!/usr/bin/env python3
"""
Utility script to fix encoding issues in existing JSON files.
This script will:
1. Read corrupted JSON files
2. Fix character encoding issues 
3. Re-save them with proper UTF-8 encoding
"""

import json
import os
import re
from pathlib import Path

def fix_encoding_issues(text):
    """Fix common UTF-8 encoding issues."""
    if not isinstance(text, str):
        return text
    
    # Common encoding fixes for Dutch characters
    replacements = {
        'Ã©': 'é',
        'Ã¨': 'è', 
        'Ã¡': 'á',
        'Ã ': 'à',
        'Ã³': 'ó',
        'Ã²': 'ò',
        'Ã­': 'í',
        'Ã¬': 'ì',
        'Ãº': 'ú',
        'Ã¹': 'ù',
        'Ã¼': 'ü',
        'Ã«': 'ë',
        'Ã¶': 'ö',
        'Ã¤': 'ä',
        'Ã§': 'ç',
        'Ã±': 'ñ',
        'CaluwÃ©': 'Caluwé',
        'NeppÃ©rus': 'Neppérus',
        'ÃztÃ¼rk': 'Öztürk',
        'YÃ¼cel': 'Yücel',
        'YeÅilgÃ¶z': 'Yeşilgöz',
        'â¦': '…',  # ellipsis
    }
    
    # Apply replacements
    for wrong, correct in replacements.items():
        text = text.replace(wrong, correct)
    
    return text

def fix_json_encoding(obj):
    """Recursively fix encoding in JSON objects."""
    if isinstance(obj, dict):
        return {k: fix_json_encoding(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fix_json_encoding(item) for item in obj]
    elif isinstance(obj, str):
        return fix_encoding_issues(obj)
    else:
        return obj

def main():
    """Main function to process all JSON files."""
    output_dir = Path("output")
    if not output_dir.exists():
        print("Output directory not found!")
        return
    
    json_files = list(output_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files to process")
    
    fixed_count = 0
    
    for json_file in json_files:
        try:
            # Read the original file
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Fix encoding issues
            fixed_data = fix_json_encoding(data)
            
            # Check if any changes were made by comparing string representations
            if json.dumps(data, sort_keys=True) != json.dumps(fixed_data, sort_keys=True):
                # Write back the fixed data
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(fixed_data, f, indent=2, ensure_ascii=False)
                
                print(f"Fixed encoding in {json_file.name}")
                fixed_count += 1
            
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
    
    print(f"Fixed encoding issues in {fixed_count} files")

if __name__ == "__main__":
    main()