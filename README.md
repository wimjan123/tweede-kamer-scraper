# Dutch Parliament Transcript Scraper

A comprehensive Python scraper for extracting plenary debate transcripts from the Dutch Parliament's (Tweede Kamer) Open Data API.

## Features

- 🏛️ **Complete Dataset**: Scrapes all 1,460+ plenary meetings from the official SyncFeed API
- 📄 **Rich Metadata**: Extracts speaker information, party affiliations, timestamps, and complete dialogue flow  
- 🔄 **Smart Pagination**: Automatically handles API pagination to fetch the entire historical archive
- 📊 **Structured Output**: Saves each meeting as a structured JSON file with detailed speaker segments
- 🛡️ **Robust Parsing**: Handles XML encoding issues, namespaces, and complex VLOS document structures
- ⚡ **Progress Tracking**: Shows real-time progress with detailed logging and progress bars

## Dataset Size

- **1,460+ plenary meetings** available for scraping
- **3,559+ reports** with full transcript data
- **Historical coverage** spanning multiple years of parliamentary proceedings
- **Rich speaker data** including names, parties, roles, and timestamps

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/tweede-kamer-scraper.git
cd tweede-kamer-scraper
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage
```bash
python scrape.py
```

### Debug Mode (Recommended for first run)
```bash
python scrape.py --debug
```

### Overwrite Existing JSON
If you previously scraped without embedded XML and want to regenerate files with the new embedded `raw_xml` and `metadata` fields, use:
```bash
python scrape.py --overwrite
```
You can also use `--force` as an alias.

### Command Line Options
- `--debug`: Enable detailed debug output to monitor progress and diagnose issues

### Extract Only the Report Link
Use the separate helper script to extract just the meeting report XML link(s) without running the full scraper:

```bash
# For a single meeting ID
python extract_link.py --meeting-id <MEETING_ID>

# Dump all meeting->report URL mappings to a JSON file
python extract_link.py --all --output links.json
```

### Makefile Shortcuts
Convenient targets are provided via `Makefile`:

```bash
# Install dependencies
make install

# Run full scraper (pass ARGS=--debug to enable debug)
make scrape ARGS=--debug

# Get single link (required: MEETING_ID)
make link MEETING_ID=<MEETING_ID>

# Dump all links (optional: OUTPUT=links.json)
make links OUTPUT=links.json
```

### Run Script Shortcuts
`run.sh` supports subcommands:

```bash
./run.sh scrape                 # Run full scraper (default)
./run.sh link <MEETING_ID>      # Print the report URL for one meeting
./run.sh links [OUTPUT.json]    # Dump all meeting->report URLs
./run.sh extract-xmls           # Extract XMLs from existing JSONs
```

## Output Format

The scraper creates JSON files for each meeting in the `output/` directory:

```json
{
  "meeting_id": "2019D12345",
  "title": "62e vergadering, woensdag 13 maart 2019",
  "date": "2019-03-13T00:00:00",
  "url": "https://gegevensmagazijn.tweedekamer.nl/SyncFeed/2.0/Resources/...",
  "segments": [
    {
      "speaker": {
        "name": "Arib",
        "party": "PvdA", 
        "role": "lid Tweede Kamer"
      },
      "text": "De vergadering wordt geopend om 10.15 uur.",
      "start_timestamp": "2019-03-13T10:15:00",
      "end_timestamp": "2019-03-13T10:15:30"
    }
  ],
  "raw_xml": "<?xml version=\"1.0\" encoding=\"utf-8\"?>...", 
  "metadata": {
    "meeting_id": "2019D12345",
    "title": "62e vergadering, woensdag 13 maart 2019",
    "date": "2019-03-13T00:00:00",
    "url": "https://gegevensmagazijn.tweedekamer.nl/SyncFeed/2.0/Resources/...",
    "segments_count": 432
  }
}
```

In addition, the scraper writes the original report XML into `output/xml/<MEETING_ID>.xml` and a sidecar metadata file `output/xml/<MEETING_ID>.metadata.json` containing the JSON metadata (title, date, url, segments_count, etc.). The XML file also embeds the metadata as a base64-encoded JSON comment right after the XML declaration, so the XML is self-describing without needing the sidecar file.

Example of embedded comment at the top of the XML:

```
<?xml version="1.0" encoding="utf-8"?>
<!-- scraper-metadata:base64:eyJtZWV0aW5nX2lkIjoiLi4uIiwidGl0bGUiOiIuLi4ifQ== -->
<vlos:vergaderverslag ...>
  ...
</vlos:vergaderverslag>
```

To decode the embedded metadata in Python:

```python
import base64, json, re
from pathlib import Path

xml_text = Path('output/xml/<MEETING_ID>.xml').read_text(encoding='utf-8')
m = re.search(r'<!--\s*scraper-metadata:base64:([^\s]+)\s*-->', xml_text)
if m:
    meta = json.loads(base64.b64decode(m.group(1)).decode('utf-8'))
    print(meta)
```

Use `--overwrite` to regenerate XMLs/metadata if needed.

## Technical Details

### API Integration
- Uses the official Dutch Parliament SyncFeed 2.0 API
- Processes both `Vergadering` (meetings) and `Verslag` (reports) feeds
- Handles ATOM feed pagination automatically

### XML Processing
- Parses complex VLOS (Vergaderverslag Ondersteuning Systeem) documents
- Handles multiple XML namespaces and encoding issues
- Resolves BOM (Byte Order Mark) encoding problems

### Data Flow
1. **Fetch Meetings**: Retrieves all plenary meetings from Vergadering feed
2. **Map Reports**: Creates mapping between meetings and their transcript reports  
3. **Parse Transcripts**: Downloads and parses detailed VLOS XML documents
4. **Extract Segments**: Identifies speakers, text content, and timestamps
5. **Save JSON**: Outputs structured data for each meeting

## Error Handling

The scraper includes comprehensive error handling:
- **Network Issues**: Automatic retries and timeout handling
- **XML Parsing**: Graceful handling of malformed or incomplete documents  
- **Missing Data**: Continues processing even if individual meetings fail
- **Progress Resumption**: Skips already-processed files on restart

## Performance

- **Concurrent Processing**: Efficient HTTP requests with session management
- **Memory Efficient**: Processes one meeting at a time to minimize memory usage
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Incremental**: Can be stopped and resumed - skips existing files

## Example Run

```
Starting Dutch Parliament transcript scraper...
Fetching plenary meetings from Vergadering feed...
Fetching page 1...
Found 47 plenary meetings on page 1 (total: 47)
Fetching page 2...
Found 44 plenary meetings on page 2 (total: 91)
...
Total found: 1460 plenary meetings across 49 pages

Fetching reports from Verslag feed...
Fetching reports page 1...
Found 250 report mappings on page 1 (total: 110)
...
Total found: 3559 report mappings across 50 pages

Processing 1460 plenary meetings...
Processing meetings: 100%|██████████| 1460/1460 [2:45:30<00:00, 6.78it/s]

Scraping completed!
Successfully processed: 1247 reports
Failed to process: 213 reports  
Output directory: /path/to/tweede-kamer-scraper/output
```

## Requirements

- Python 3.7+
- Dependencies listed in `requirements.txt`:
  - `requests` - HTTP client for API calls
  - `lxml` - XML parsing and processing
  - `tqdm` - Progress bars and logging

## Data Sources

This scraper uses the official Dutch Parliament Open Data API:
- **Base URL**: `https://gegevensmagazijn.tweedekamer.nl/SyncFeed/2.0/Feed`
- **Documentation**: [Tweede Kamer Open Data](https://opendata.tweedekamer.nl/)
- **API Format**: ATOM feeds with embedded XML content

## License

MIT License - Feel free to use this scraper for research, journalism, or civic engagement projects.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Disclaimer

This tool is designed for legitimate research and transparency purposes. Please be respectful of the API and avoid excessive request rates that could impact service availability.
