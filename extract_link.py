#!/usr/bin/env python3
"""
Extract the report XML link(s) from the Verslag feed.

This script provides a lightweight way to fetch the meeting report link
without running the full scraper. You can either:
 - Look up a single meeting ID and print its report URL
 - Dump all meeting->report URL mappings as JSON

Usage examples:
  # Single meeting
  python extract_link.py --meeting-id 1e4227e0-7c1f-4a8e-8c96-5d0e35fb8b9a

  # Dump all mappings to JSON
  python extract_link.py --all --output links.json
"""

import argparse
import json
import sys

from scrape import DutchParliamentScraper
from typing import Optional, Dict


def find_single_link(meeting_id: str, debug: bool = False) -> Optional[str]:
    """Return the report XML URL for a given meeting ID, or None if not found.

    Note: This uses the existing reports mapping method which scans the feed.
    """
    scraper = DutchParliamentScraper(debug=debug)
    mapping = scraper.fetch_reports_mapping()
    return mapping.get(meeting_id)


def dump_all_links(debug: bool = False) -> Dict[str, str]:
    """Return a mapping of meeting_id -> report XML URL for all available entries."""
    scraper = DutchParliamentScraper(debug=debug)
    return scraper.fetch_reports_mapping()


def main():
    parser = argparse.ArgumentParser(description="Extract report XML link(s) from the Verslag feed")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--meeting-id", help="Meeting ID to look up the report URL for")
    grp.add_argument("--all", action="store_true", help="Dump all meeting->report URL mappings as JSON")
    parser.add_argument("--output", "-o", help="Optional output file (JSON for --all). Defaults to stdout")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.all:
        mapping = dump_all_links(debug=args.debug)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
        else:
            json.dump(mapping, sys.stdout, ensure_ascii=False, indent=2)
            print()
        return

    # Single meeting-id mode
    link = find_single_link(args.meeting_id, debug=args.debug)
    if not link:
        print(f"No report link found for meeting {args.meeting_id}")
        sys.exit(1)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(link + "\n")
    else:
        print(link)


if __name__ == "__main__":
    main()
