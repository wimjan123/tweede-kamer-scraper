#!/usr/bin/env python3
"""
Standalone Python script that scrapes all plenary debate reports from the Dutch Parliament's Open Data API.
Extracts rich speaker information, complete dialogue flow, and start/end timestamps for each spoken part.
"""

import os
import json
import requests
from datetime import datetime
from urllib.parse import urljoin
from lxml import etree
from tqdm import tqdm


class DutchParliamentScraper:
    """Scraper for Dutch Parliament plenary debate transcripts with timestamps."""
    
    BASE_URL = "https://gegevensmagazijn.tweedekamer.nl/SyncFeed/2.0/Feed"
    
    def __init__(self, output_dir="output", debug=False):
        """Initialize the scraper with output directory."""
        self.output_dir = output_dir
        self.debug = debug
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Dutch Parliament Transcript Scraper 1.0'
        })
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
    def make_request(self, url, timeout=30):
        """Make HTTP request with error handling and retries."""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def parse_xml_feed(self, xml_content):
        """Parse XML feed and return root element."""
        try:
            # Remove BOM if present
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]
            elif xml_content.startswith('ï»¿'):  # UTF-8 BOM as seen in response
                xml_content = xml_content[3:]
            
            return etree.fromstring(xml_content.encode('utf-8'))
        except etree.XMLSyntaxError as e:
            print(f"XML parsing error: {e}")
            return None
    
    def fetch_plenary_meetings(self):
        """Fetch all plenary meetings from the Vergadering feed with pagination."""
        print("Fetching plenary meetings from Vergadering feed...")
        
        plenary_meetings = []
        page_count = 0
        next_url = f"{self.BASE_URL}?category=Vergadering"
        
        while next_url and page_count < 50:  # Limit to 50 pages to prevent infinite loops
            page_count += 1
            print(f"Fetching page {page_count}...")
            
            response = self.make_request(next_url)
            if not response:
                break
            
            root = self.parse_xml_feed(response.text)
            if root is None:
                break
            
            # Debug: Print namespace and structure for first page only
            if self.debug and page_count == 1:
                print("Root tag:", root.tag)
                print("Root nsmap:", root.nsmap)
                print("First few children:", [child.tag for child in root[:3]])
            
            # Define namespaces properly
            namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
            
            # Find all entries in the feed
            entries = root.xpath('//atom:entry', namespaces=namespaces)
            if self.debug:
                print(f"Found {len(entries)} entries on page {page_count}")
            
            page_meetings = []
            for entry in entries:
                # Extract the meeting details from the entry content
                content = entry.find('.//{http://www.w3.org/2005/Atom}content')
                if content is not None:
                    # Parse the content XML to find Soort
                    try:
                        # Handle both direct content and nested CDATA
                        content_text = content.text
                        if content_text is None and len(content) > 0:
                            # Get content from the element itself including children
                            for child in content:
                                content_text = etree.tostring(child, encoding='unicode')
                                break
                        
                        if content_text and content_text.strip():
                            # Debug first few content texts
                            content_xml = etree.fromstring(content_text)
                            
                            # Define the namespace for the content XML
                            tk_ns = {'ns1': 'http://www.tweedekamer.nl/xsd/tkData/v1-0'}
                            soort_elem = content_xml.find('.//ns1:soort', tk_ns)
                        else:
                            continue
                        
                        if soort_elem is not None and soort_elem.text == "Plenair":
                            # Extract meeting ID - it's in the root element's id attribute
                            meeting_id = content_xml.get('id')
                            
                            # Extract date
                            datum_elem = content_xml.find('.//ns1:datum', tk_ns)
                            
                            if meeting_id is not None:
                                meeting_info = {
                                    'id': meeting_id,
                                    'date': datum_elem.text if datum_elem is not None else None
                                }
                                page_meetings.append(meeting_info)
                                
                    except etree.XMLSyntaxError:
                        continue
            
            # Add page meetings to total
            plenary_meetings.extend(page_meetings)
            print(f"Found {len(page_meetings)} plenary meetings on page {page_count} (total: {len(plenary_meetings)})")
            
            # Look for next page link
            next_url = None
            next_links = root.xpath('//atom:link[@rel="next"]', namespaces=namespaces)
            if next_links:
                next_url = next_links[0].get('href')
                if self.debug:
                    print(f"Next page URL: {next_url}")
            
            # If no meetings found on this page, we might be at the end
            if len(page_meetings) == 0:
                print("No more plenary meetings found, stopping pagination")
                break
        
        print(f"Total found: {len(plenary_meetings)} plenary meetings across {page_count} pages")
        return plenary_meetings
    
    def fetch_reports_mapping(self):
        """Fetch all reports and create mapping of meeting ID to report URL with pagination."""
        print("Fetching reports from Verslag feed...")
        
        reports_mapping = {}
        page_count = 0
        next_url = f"{self.BASE_URL}?category=Verslag"
        
        while next_url and page_count < 50:  # Limit to 50 pages to prevent infinite loops
            page_count += 1
            print(f"Fetching reports page {page_count}...")
            
            response = self.make_request(next_url)
            if not response:
                break
                
            root = self.parse_xml_feed(response.text)
            if root is None:
                break
            
            # Define namespaces properly
            namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
            
            # Find all entries in the feed
            entries = root.xpath('//atom:entry', namespaces=namespaces)
            if self.debug:
                print(f"Found {len(entries)} report entries on page {page_count}")
            
            page_mappings = 0
            for entry in entries:
                # Debug the first entry structure  
                if self.debug and len(reports_mapping) < 1 and page_count == 1:
                    print(f"Entry tag: {entry.tag}")
                    print(f"Entry children: {[child.tag for child in entry]}")
                
                    # Check all link elements in this entry
                    all_links = entry.findall('.//{http://www.w3.org/2005/Atom}link')
                    print(f"All links in entry: {len(all_links)}")
                    for i, link in enumerate(all_links):
                        print(f"  Link {i}: type={link.get('type')}, rel={link.get('rel')}, href={link.get('href')}")
                
                # Get the enclosure link (the actual resource)
                link_elem = entry.find('.//{http://www.w3.org/2005/Atom}link[@rel="enclosure"]')
                content_elem = entry.find('.//{http://www.w3.org/2005/Atom}content')
                
                if self.debug and len(reports_mapping) < 1 and page_count == 1:
                    print(f"Link elem: {link_elem}")
                    print(f"Content elem: {content_elem}")
                
                if link_elem is not None and content_elem is not None:
                    report_xml_url = link_elem.get('href')
                    
                    # Parse content to find Vergadering_Id
                    try:
                        # Handle both direct content and nested CDATA
                        content_text = content_elem.text
                        if content_text is None and len(content_elem) > 0:
                            # Try to get content from nested elements
                            content_text = etree.tostring(content_elem, encoding='unicode')
                        
                        if content_text:
                            content_xml = etree.fromstring(content_text)
                            
                            # Debug the reports XML structure
                            if self.debug and len(reports_mapping) < 2 and page_count == 1:
                                print(f"Reports content sample: {content_text[:500]}...")
                                print(f"Reports XML root tag: {content_xml.tag}")
                                print(f"Reports XML children: {[child.tag for child in content_xml[:5]]}")
                            
                            # Define the namespace for the content XML
                            tk_ns = {'ns1': 'http://www.tweedekamer.nl/xsd/tkData/v1-0'}
                            
                            # Look for vergadering element and extract its ID
                            vergadering_elem = content_xml.find('.//ns1:vergadering', tk_ns)
                            
                            if self.debug and len(reports_mapping) < 2 and page_count == 1:
                                print(f"Vergadering element: {vergadering_elem}")
                                if vergadering_elem is not None:
                                    print(f"Vergadering attributes: {vergadering_elem.attrib}")
                                    # Look for xsi:type and extract the ID from the href
                                    xsi_type = vergadering_elem.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                                    if xsi_type and 'referentie' in xsi_type:
                                        # Extract from href attribute
                                        href = vergadering_elem.get('href')
                                        print(f"Vergadering href: {href}")
                        else:
                            continue
                        
                        if vergadering_elem is not None:
                            # Extract meeting ID from ref attribute
                            meeting_id = vergadering_elem.get('ref')
                            if meeting_id:
                                reports_mapping[meeting_id] = report_xml_url
                                page_mappings += 1
                                
                                if self.debug and len(reports_mapping) <= 2:
                                    print(f"Mapped meeting {meeting_id} to {report_xml_url}")
                            
                    except etree.XMLSyntaxError:
                        continue
            
            print(f"Found {page_mappings} report mappings on page {page_count} (total: {len(reports_mapping)})")
            
            # Look for next page link
            next_url = None
            next_links = root.xpath('//atom:link[@rel="next"]', namespaces=namespaces)
            if next_links:
                next_url = next_links[0].get('href')
                if self.debug:
                    print(f"Next reports page URL: {next_url}")
            
            # If no mappings found on this page, we might be at the end
            if page_mappings == 0:
                print("No more report mappings found, stopping pagination")
                break
        
        print(f"Total found: {len(reports_mapping)} report mappings across {page_count} pages")
        return reports_mapping
    
    def extract_vlos_speaker_info(self, spreker_elem, vlos_ns):
        """Extract speaker information from VLOS spreker XML element."""
        if spreker_elem is None:
            return {"name": "Unknown", "party": None, "role": None}
        
        # Look for speaker name in VLOS structure
        name_elem = spreker_elem.find('.//vlos:verslagnaam', vlos_ns)
        party_elem = spreker_elem.find('.//vlos:fractie', vlos_ns)
        role_elem = spreker_elem.find('.//vlos:functie', vlos_ns)
        
        # Also try other possible name fields
        if name_elem is None:
            name_elem = spreker_elem.find('.//vlos:weergavenaam', vlos_ns)
        
        return {
            "name": name_elem.text if name_elem is not None else "Unknown",
            "party": party_elem.text if party_elem is not None else None,
            "role": role_elem.text if role_elem is not None else None
        }
    
    def extract_speaker_info(self, spreker_elem):
        """Extract speaker information from spreker XML element (legacy method)."""
        if spreker_elem is None:
            return {"name": "Unknown", "party": None, "role": None}
        
        name_elem = spreker_elem.find('.//Verslagnaam')
        party_elem = spreker_elem.find('.//Fractie')
        role_elem = spreker_elem.find('.//Functie')
        
        return {
            "name": name_elem.text if name_elem is not None else "Unknown",
            "party": party_elem.text if party_elem is not None else None,
            "role": role_elem.text if role_elem is not None else None
        }
    
    def extract_text_content(self, tekst_elem):
        """Extract and combine text from all alineaitem tags."""
        if tekst_elem is None:
            return ""
        
        text_parts = []
        for alinea_item in tekst_elem.findall('.//Alineaitem'):
            if alinea_item.text:
                text_parts.append(alinea_item.text.strip())
        
        return " ".join(text_parts)
    
    def parse_timestamp(self, timestamp_text):
        """Parse and format timestamp."""
        if not timestamp_text:
            return None
        
        try:
            # Try to parse the timestamp - format may vary
            # Common formats: '2019-05-28T14:00:33' or '2019-05-28T14:00:33.000'
            if 'T' in timestamp_text:
                return timestamp_text.split('.')[0]  # Remove microseconds if present
            return timestamp_text
        except:
            return timestamp_text
    
    def parse_report_xml(self, report_xml_url, meeting_id):
        """Parse detailed report XML and extract transcript segments."""
        print(f"Processing report for meeting {meeting_id}...")
        
        response = self.make_request(report_xml_url)
        if not response:
            return None
        
        # Debug the response content type and first part of content
        if self.debug:
            print(f"Response content type: {response.headers.get('content-type')}")
            print(f"Response content sample: {response.text[:200]}...")
        
        root = self.parse_xml_feed(response.text)
        if root is None:
            return None
        
        # Debug the XML structure
        if self.debug:
            print(f"XML root tag: {root.tag}")
            print(f"XML root nsmap: {root.nsmap}")
            print(f"First few children: {[child.tag for child in root[:10]]}")
        
        # Extract basic report information
        report_data = {
            "title": "",
            "date": "",
            "url": report_xml_url,
            "segments": []
        }
        
        # Define the VLOS namespace
        vlos_ns = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}
        
        # This appears to be a VLOS document, extract title and date
        title_elem = root.find('.//vlos:titel', vlos_ns)
        if title_elem is not None:
            report_data["title"] = title_elem.text
        
        datum_elem = root.find('.//vlos:datum', vlos_ns)
        if datum_elem is not None:
            report_data["date"] = datum_elem.text
        
        if self.debug:
            print(f"Found title: {report_data['title']}")
            print(f"Found date: {report_data['date']}")
        
        # Process VLOS activiteit elements (these contain speaker segments)
        activiteiten = root.findall('.//vlos:activiteit', vlos_ns)
        if self.debug:
            print(f"Found {len(activiteiten)} activiteit elements")
        
        for act_idx, activiteit in enumerate(activiteiten):
            # Find speaker information within this activiteit
            sprekers = activiteit.findall('.//vlos:spreker', vlos_ns)
            
            # Also look for all alinea elements in this activiteit
            all_alineas = activiteit.findall('.//vlos:alinea', vlos_ns)
            
            if self.debug and act_idx < 2:
                print(f"Activiteit {act_idx}: Found {len(sprekers)} speakers and {len(all_alineas)} alinea elements")
                if len(all_alineas) > 0:
                    print(f"  Sample alinea texts: {[al.text[:50] if al.text else 'None' for al in all_alineas[:3]]}")
            
            for i, spreker in enumerate(sprekers):
                # Extract speaker information
                spreker_info = self.extract_vlos_speaker_info(spreker, vlos_ns)
                
                # Debug speaker info for first few speakers
                if self.debug and i < 2:
                    print(f"Speaker {i}: {spreker_info}")
                    print(f"Speaker element: {spreker.tag}")
                    print(f"Speaker children: {[child.tag for child in spreker]}")
                
                # Find all text segments (alinea elements) for this speaker
                alineas = spreker.findall('.//vlos:alinea', vlos_ns)
                
                if self.debug and i < 2:
                    print(f"Found {len(alineas)} alinea elements for speaker {i}")
                    for j, alinea in enumerate(alineas[:3]):
                        print(f"  Alinea {j}: '{alinea.text}'" if alinea.text else f"  Alinea {j}: No text")
                
                # Extract timestamps for this speaker's segment
                start_time_elem = spreker.find('.//vlos:markeertijdbegin', vlos_ns)
                end_time_elem = spreker.find('.//vlos:markeertijdeind', vlos_ns)
                
                start_timestamp = self.parse_timestamp(start_time_elem.text if start_time_elem is not None else None)
                end_timestamp = self.parse_timestamp(end_time_elem.text if end_time_elem is not None else None)
                
                # Combine all text from alinea elements
                text_parts = []
                for alinea in alineas:
                    if alinea.text and alinea.text.strip():
                        text_parts.append(alinea.text.strip())
                
                text_content = " ".join(text_parts)
                
                if self.debug and i < 2:
                    print(f"Combined text for speaker {i}: '{text_content[:200]}...'")
                
                if text_content.strip():  # Only add non-empty segments
                    segment = {
                        "speaker": spreker_info,
                        "text": text_content,
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp
                    }
                    report_data["segments"].append(segment)
                    
                    if self.debug and len(report_data["segments"]) <= 3:
                        print(f"Added segment: {spreker_info['name']} - {text_content[:100]}...")
        
        print(f"Extracted {len(report_data['segments'])} segments from report")
        return report_data
    
    def save_report_json(self, report_data, meeting_id):
        """Save report data as JSON file."""
        filename = f"{meeting_id}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            print(f"Saved report to {filepath}")
            return True
        except Exception as e:
            print(f"Error saving report {filename}: {e}")
            return False
    
    def run(self):
        """Main execution method."""
        print("Starting Dutch Parliament transcript scraper...")
        
        # Step 1: Fetch all plenary meetings
        plenary_meetings = self.fetch_plenary_meetings()
        if not plenary_meetings:
            print("No plenary meetings found. Exiting.")
            return
        
        # Step 2: Fetch reports mapping
        reports_mapping = self.fetch_reports_mapping()
        if not reports_mapping:
            print("No reports mapping found. Exiting.")
            return
        
        # Step 3: Process each plenary meeting
        successful_downloads = 0
        failed_downloads = 0
        
        print(f"\nProcessing {len(plenary_meetings)} plenary meetings...")
        
        for meeting in tqdm(plenary_meetings, desc="Processing meetings"):
            meeting_id = meeting['id']
            
            # Check if we have a report for this meeting
            if meeting_id not in reports_mapping:
                print(f"No report found for meeting {meeting_id}")
                failed_downloads += 1
                continue
            
            report_xml_url = reports_mapping[meeting_id]
            
            # Check if file already exists
            filename = f"{meeting_id}.json"
            filepath = os.path.join(self.output_dir, filename)
            if os.path.exists(filepath):
                print(f"Report {filename} already exists, skipping...")
                successful_downloads += 1
                continue
            
            # Parse and save the report
            report_data = self.parse_report_xml(report_xml_url, meeting_id)
            if report_data and self.save_report_json(report_data, meeting_id):
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        # Summary
        print(f"\nScraping completed!")
        print(f"Successfully processed: {successful_downloads} reports")
        print(f"Failed to process: {failed_downloads} reports")
        print(f"Total reports found: {len(plenary_meetings)}")
        print(f"Output directory: {os.path.abspath(self.output_dir)}")


def main():
    """Main entry point."""
    import sys
    debug = '--debug' in sys.argv
    scraper = DutchParliamentScraper(debug=debug)
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()