#!/usr/bin/env python3
"""
Canadian Patent Data Fetcher and Analyzer
Connects to the Government of Canada CKAN API to fetch patent data,
stores it in a SQLite database, and provides analysis capabilities.
"""

import sqlite3
import requests
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import time
import os
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CanadianPatentFetcher:
    def __init__(self, db_path: str = "cnd_patents.db", cache_dir: str = "patent_cache"):
        """
        Initialize the patent data fetcher.
        
        Args:
            db_path: Path to the SQLite database file
            cache_dir: Directory to cache downloaded files
        """
        self.db_path = db_path
        self.cache_dir = cache_dir
        self.base_url = "https://open.canada.ca/data/api/3"
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Canadian Patent Fetcher/1.0'
        })
        
        # Disable SSL verification for CIPO servers (they have certificate issues)
        self.session.verify = False
        
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Initialize database
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with comprehensive patent schema."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            logger.info("Creating comprehensive patent database schema...")
            
            # Main patents table (from PT_main)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patents_main (
                    patent_number TEXT PRIMARY KEY,
                    filing_date TEXT,
                    grant_date TEXT,
                    application_status_code TEXT,
                    application_type_code TEXT,
                    title_english TEXT,
                    title_french TEXT,
                    bibliographic_extract_date TEXT,
                    country_publication_code TEXT,
                    document_kind_type TEXT,
                    examination_request_date TEXT,
                    filing_country_code TEXT,
                    language_filing_code TEXT,
                    license_sale_indicator INTEGER,
                    pct_application_number TEXT,
                    pct_publication_number TEXT,
                    pct_publication_date TEXT,
                    parent_application_number TEXT,
                    pct_article_22_39_date TEXT,
                    pct_section_371_date TEXT,
                    pct_publication_country_code TEXT,
                    publication_kind_type TEXT,
                    printed_amended_country_code TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Patent abstracts table (from PT_abstract)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_abstracts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    sequence_number INTEGER,
                    filing_language_code TEXT,
                    abstract_language_code TEXT,
                    abstract_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # Patent claims table (from PT_claim)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    sequence_number INTEGER,
                    filing_language_code TEXT,
                    claims_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # Patent disclosure table (from PT_disclosure)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_disclosures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    sequence_number INTEGER,
                    filing_language_code TEXT,
                    disclosure_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # Interested parties table (from PT_interested_party)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_interested_parties (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    agent_type_code TEXT,
                    applicant_type_code TEXT,
                    interested_party_type_code TEXT,
                    interested_party_type TEXT,
                    owner_enable_date TEXT,
                    ownership_end_date TEXT,
                    party_name TEXT,
                    party_address_line1 TEXT,
                    party_address_line2 TEXT,
                    party_address_line3 TEXT,
                    party_address_line4 TEXT,
                    party_address_line5 TEXT,
                    party_city TEXT,
                    party_province_code TEXT,
                    party_province TEXT,
                    party_postal_code TEXT,
                    party_country_code TEXT,
                    party_country TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # IPC Classification table (from PT_IPC_classification)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_ipc_classifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    sequence_number INTEGER,
                    ipc_version_date TEXT,
                    classification_level TEXT,
                    classification_status_code TEXT,
                    classification_status TEXT,
                    ipc_section_code TEXT,
                    ipc_section TEXT,
                    ipc_class_code TEXT,
                    ipc_class TEXT,
                    ipc_subclass_code TEXT,
                    ipc_subclass TEXT,
                    ipc_main_group_code TEXT,
                    ipc_group TEXT,
                    ipc_subgroup_code TEXT,
                    ipc_subgroup TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # Priority claims table (from PT_priority_claim)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patent_priority_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patent_number TEXT NOT NULL,
                    foreign_application_number TEXT,
                    priority_claim_kind_code TEXT,
                    priority_claim_country_code TEXT,
                    priority_claim_country TEXT,
                    priority_claim_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (patent_number) REFERENCES patents_main (patent_number)
                )
            ''')
            
            # Create datasets table to track which datasets we've processed
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    title TEXT,
                    description TEXT,
                    last_updated TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create cache table to track downloaded files
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_cache (
                    url TEXT PRIMARY KEY,
                    local_path TEXT,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_size INTEGER,
                    processed BOOLEAN DEFAULT FALSE
                )
            ''')
            
            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_patents_main_filing_date ON patents_main (filing_date)",
                "CREATE INDEX IF NOT EXISTS idx_patents_main_status ON patents_main (application_status_code)",
                "CREATE INDEX IF NOT EXISTS idx_patents_main_type ON patents_main (application_type_code)",
                "CREATE INDEX IF NOT EXISTS idx_abstracts_patent_number ON patent_abstracts (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_claims_patent_number ON patent_claims (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_disclosures_patent_number ON patent_disclosures (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_parties_patent_number ON patent_interested_parties (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_parties_type ON patent_interested_parties (interested_party_type)",
                "CREATE INDEX IF NOT EXISTS idx_parties_name ON patent_interested_parties (party_name)",
                "CREATE INDEX IF NOT EXISTS idx_ipc_patent_number ON patent_ipc_classifications (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_ipc_section ON patent_ipc_classifications (ipc_section_code)",
                "CREATE INDEX IF NOT EXISTS idx_ipc_class ON patent_ipc_classifications (ipc_class_code)",
                "CREATE INDEX IF NOT EXISTS idx_priority_patent_number ON patent_priority_claims (patent_number)",
                "CREATE INDEX IF NOT EXISTS idx_priority_country ON patent_priority_claims (priority_claim_country_code)"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            
            conn.commit()
            conn.close()
            logger.info(f"Comprehensive patent database schema initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def get_cached_file_path(self, url: str) -> str:
        """Generate a cache file path for a given URL."""
        # Create a hash of the URL for the filename
        url_hash = hashlib.md5(url.encode()).hexdigest()
        filename = os.path.basename(url)
        if not filename or not filename.endswith(('.zip', '.csv', '.xml')):
            filename = f"{url_hash}.zip"
        return os.path.join(self.cache_dir, filename)
    
    def is_file_cached(self, url: str) -> Optional[str]:
        """Check if file is already cached locally."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT local_path, processed FROM file_cache WHERE url = ?", (url,))
            result = cursor.fetchone()
            conn.close()
            
            if result and os.path.exists(result[0]):
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Error checking cache: {e}")
            return None
    
    def is_file_processed(self, url: str) -> bool:
        """Check if file has already been processed."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT processed FROM file_cache WHERE url = ?", (url,))
            result = cursor.fetchone()
            conn.close()
            
            return result and result[0] == 1  # processed = TRUE
        except Exception as e:
            logger.error(f"Error checking if file processed: {e}")
            return False
    
    def cache_file(self, url: str, content: bytes) -> str:
        """Cache a downloaded file locally."""
        local_path = self.get_cached_file_path(url)
        
        try:
            with open(local_path, 'wb') as f:
                f.write(content)
            
            # Record in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO file_cache 
                (url, local_path, downloaded_at, file_size, processed)
                VALUES (?, ?, ?, ?, ?)
            ''', (url, local_path, datetime.now().isoformat(), len(content), False))
            conn.commit()
            conn.close()
            
            logger.info(f"Cached file: {local_path} ({len(content)} bytes)")
            return local_path
            
        except Exception as e:
            logger.error(f"Error caching file: {e}")
            return ""
    
    def search_patent_datasets(self, query: str = "patent") -> List[Dict]:
        """
        Search for patent-related datasets in CKAN.
        
        Args:
            query: Search query for finding patent datasets
            
        Returns:
            List of dataset dictionaries
        """
        try:
            # First, try to get the specific CIPO patent dataset
            specific_patent_datasets = [
                "fe1dfbb9-0fc3-42ca-b2a9-6ca4c05dbac9",  # Patent data from CIPO (main dataset)
            ]
            
            found_datasets = []
            
            # Try to fetch specific datasets directly
            for dataset_id in specific_patent_datasets:
                try:
                    url = f"{self.base_url}/action/package_show"
                    params = {'id': dataset_id}
                    
                    response = self.session.get(url, params=params)
                    response.raise_for_status()
                    
                    data = response.json()
                    if data['success']:
                        dataset = data['result']
                        found_datasets.append(dataset)
                        logger.info(f"Found main patent dataset: {dataset.get('title', 'No title')}")
                
                except Exception as e:
                    logger.warning(f"Could not fetch dataset {dataset_id}: {e}")
                    continue
            
            return found_datasets  # Focus only on main patent dataset
                
        except Exception as e:
            logger.error(f"Error searching datasets: {e}")
            return []
    
    def get_dataset_resources(self, dataset_id: str) -> List[Dict]:
        """
        Get resources (data files) for a specific dataset.
        
        Args:
            dataset_id: The ID of the dataset
            
        Returns:
            List of resource dictionaries
        """
        try:
            url = f"{self.base_url}/action/package_show"
            params = {'id': dataset_id}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data['success']:
                return data['result']['resources']
            else:
                logger.error(f"Error getting dataset resources: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching dataset resources: {e}")
            return []
    
    def download_and_extract_zip(self, resource: Dict) -> List[Dict]:
        """
        Download and extract ZIP files containing patent data (with caching).
        
        Args:
            resource: Resource dictionary from CKAN
            
        Returns:
            List of parsed patent records
        """
        patents = []
        
        try:
            import zipfile
            import io
            
            url = resource.get('url')
            if not url:
                logger.warning("Resource has no URL")
                return patents
            
            resource_name = resource.get('name', 'Unknown')
            
            # Check if file was already processed - skip if so
            if self.is_file_processed(url):
                logger.info(f"File already processed, skipping: {resource_name}")
                return patents
            
            # Check if file is already cached
            cached_path = self.is_file_cached(url)
            if cached_path:
                logger.info(f"Using cached file: {cached_path}")
                local_path = cached_path
            else:
                logger.info(f"Downloading ZIP resource: {resource_name}")
                logger.info(f"URL: {url}")
                
                # Download the ZIP file
                headers = {
                    'User-Agent': 'Canadian Patent Fetcher/1.0',
                    'Accept': 'application/zip,*/*'
                }
                
                response = self.session.get(url, timeout=120, headers=headers)
                response.raise_for_status()
                
                # Check if we got a ZIP file
                if response.headers.get('content-type', '').find('zip') == -1 and not url.endswith('.zip'):
                    logger.warning("Response doesn't appear to be a ZIP file")
                    return patents
                
                # Cache the file
                local_path = self.cache_file(url, response.content)
                if not local_path:
                    return patents
            
            # Extract ZIP file from local cache
            with zipfile.ZipFile(local_path) as zip_file:
                file_list = zip_file.namelist()
                logger.info(f"ZIP contains {len(file_list)} files: {file_list[:3]}...")
                
                for file_name in file_list:
                    # Skip directories and focus on main patent files
                    if file_name.endswith('/'):
                        continue
                    
                    # Prioritize main patent files over disclosure/claim files
                    is_main_file = any(keyword in file_name.lower() for keyword in [
                        'pt_main', 'patent_main', 'main_patent'
                    ])
                    
                    is_csv_file = file_name.lower().endswith('.csv')
                    
                    if not is_csv_file:
                        continue
                    
                    # Process all CSV files, prioritizing main files
                    if not is_main_file:
                        # Also process other types but log them differently
                        logger.info(f"Processing additional patent file: {file_name}")
                    else:
                        logger.info(f"Processing main patent file: {file_name}")
                    
                    logger.info(f"Processing main patent CSV file from ZIP: {file_name}")
                    
                    # Extract and read the CSV file
                    with zip_file.open(file_name) as csv_file:
                        # Read full file content
                        csv_content = csv_file.read().decode('utf-8', errors='ignore')
                        
                        logger.info(f"File {file_name} size: {len(csv_content)} characters")
                        
                        # Determine file type and parse accordingly
                        file_type = self.determine_file_type(file_name)
                        logger.info(f"Detected file type: {file_type}")
                        
                        # Parse the CSV content
                        parsed_data = self.parse_csv_data(csv_content)
                        if parsed_data:
                            logger.info(f"Found {len(parsed_data)} records in {file_name}")
                            
                            # Save data to appropriate table based on file type
                            if file_type == 'main':
                                self.save_main_patents(parsed_data)
                            elif file_type == 'abstract':
                                self.save_abstracts(parsed_data)
                            elif file_type == 'claim':
                                self.save_claims(parsed_data)
                            elif file_type == 'disclosure':
                                self.save_disclosures(parsed_data)
                            elif file_type == 'interested_party':
                                self.save_interested_parties(parsed_data)
                            elif file_type == 'ipc_classification':
                                self.save_ipc_classifications(parsed_data)
                            elif file_type == 'priority_claim':
                                self.save_priority_claims(parsed_data)
                            else:
                                logger.warning(f"Unknown file type {file_type}, using legacy method")
                                # Fall back to old method for unknown file types
                                file_patents = []
                                for record in parsed_data:
                                    patent = self.extract_patent_info(record)
                                    if patent:
                                        file_patents.append(patent)
                                patents.extend(file_patents)
                        else:
                            logger.info(f"No records found in {file_name}")
                    
                    # Process all CSV files (no artificial limits)
                    logger.info(f"Continuing to process all CSV files in ZIP...")
            
            # Mark as processed in cache
            if local_path:
                self.mark_file_processed(url)
                
            logger.info(f"Total patents extracted from ZIP: {len(patents)}")
            
        except zipfile.BadZipFile:
            logger.error("Downloaded file is not a valid ZIP file")
        except Exception as e:
            logger.error(f"Error processing ZIP file: {e}")
        
        return patents
    
    def mark_file_processed(self, url: str):
        """Mark a cached file as processed."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("UPDATE file_cache SET processed = TRUE WHERE url = ?", (url,))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error marking file as processed: {e}")
    
    def download_and_parse_resource(self, resource: Dict) -> List[Dict]:
        """
        Download and parse a resource file.
        
        Args:
            resource: Resource dictionary from CKAN
            
        Returns:
            List of parsed patent records
        """
        patents = []
        
        try:
            url = resource.get('url')
            if not url:
                logger.warning("Resource has no URL")
                return patents
            
            format_type = resource.get('format', '').lower()
            resource_name = resource.get('name', 'Unknown')
            
            # Check if file was already processed - skip if so
            if self.is_file_processed(url):
                logger.info(f"File already processed, skipping: {resource_name}")
                return patents
                
            logger.info(f"Downloading resource: {resource_name} ({format_type})")
            
            # Add headers to handle different content types
            headers = {
                'User-Agent': 'Canadian Patent Fetcher/1.0',
                'Accept': 'text/csv,application/json,text/xml,application/xml,*/*'
            }
            
            response = self.session.get(url, timeout=30, headers=headers, verify=False)
            response.raise_for_status()
            
            # Get response content with better encoding detection
            try:
                # First try to detect if content is binary
                if 'content-length' in response.headers:
                    content_length = int(response.headers['content-length'])
                else:
                    content_length = len(response.content)
                
                # Check if content appears to be binary/compressed
                raw_content = response.content[:100]
                if b'\x00' in raw_content or len([b for b in raw_content if b < 32 and b not in [9, 10, 13]]) > 10:
                    logger.warning("Content appears to be binary/compressed, not text data")
                    return patents
                
                # Try to decode with different encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        content_text = response.content.decode(encoding)
                        # Check if decoded content looks reasonable
                        if content_text and len(content_text) > 100:
                            break
                    except UnicodeDecodeError:
                        continue
                else:
                    logger.warning("Could not decode content with any common encoding")
                    return patents
                    
            except Exception as e:
                logger.error(f"Error decoding content: {e}")
                return patents
            
            # Skip if content is too small (likely not real data)
            if len(content_text.strip()) < 100:
                logger.warning(f"Resource content too small ({len(content_text)} chars), skipping")
                return patents
            
            # Show a sample of the content for debugging
            logger.debug(f"Content sample (first 200 chars): {content_text[:200]}...")
            
            # Check if content looks like HTML error page or binary data
            content_preview = content_text[:200].strip()
            if (content_preview.lower().startswith('<!doctype html') or 
                '<html' in content_preview.lower() or
                'PK' in content_preview[:10] or  # ZIP file signature
                len([c for c in content_preview if ord(c) < 32 and c not in '\t\n\r']) > 20):  # Too many control chars
                
                logger.warning("Resource appears to be HTML page, compressed file, or binary data")
                logger.info(f"Content preview: {content_preview}")
                return patents
            
            # Get content type for format detection
            content_type = response.headers.get('content-type', '').lower()
            
            # Parse based on format
            if format_type == 'json' or 'json' in content_type:
                try:
                    json_data = response.json()
                    patents = self.parse_json_data(json_data)
                except ValueError as e:
                    logger.error(f"Invalid JSON in resource: {e}")
                    
            elif format_type == 'csv' or 'csv' in content_type:
                patents = self.parse_csv_data(content_text)
                
            elif format_type in ['xml', 'rdf'] or 'xml' in content_type:
                patents = self.parse_xml_data(content_text)
                
            else:
                logger.warning(f"Unsupported format: {format_type}")
            
            if patents:
                logger.info(f"Successfully parsed {len(patents)} patent records from resource")
            else:
                logger.info(f"No patent records found in resource (may not contain patent data)")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading resource: {e}")
        except Exception as e:
            logger.error(f"Error processing resource: {e}")
        
        return patents
    
    def parse_json_data(self, data) -> List[Dict]:
        """Parse JSON data and extract patent information."""
        patents = []
        
        try:
            # Handle different JSON structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Look for common keys that might contain the records
                records = data.get('records', data.get('data', data.get('results', [data])))
            else:
                records = [data]
            
            for record in records:
                if isinstance(record, dict):
                    patent = self.extract_patent_info(record)
                    if patent:
                        patents.append(patent)
                        
        except Exception as e:
            logger.error(f"Error parsing JSON data: {e}")
        
        return patents
    
    def parse_csv_data(self, csv_text: str) -> List[Dict]:
        """Parse CSV data and extract patent information."""
        patents = []
        
        try:
            import csv
            import io
            
            # Increase CSV field size limit to handle large patent descriptions and disclosures
            csv.field_size_limit(2000000)  # 2MB limit for disclosure text
            
            # Try different CSV parsing approaches
            try:
                # First try with pipe delimiter (Canadian patents use | delimiter)
                reader = csv.DictReader(io.StringIO(csv_text), delimiter='|')
                
                # Process in chunks to avoid memory issues
                count = 0
                for row in reader:
                    # For raw CSV data (like claims, disclosures, interested parties)
                    # return the raw row data instead of extracting patent info
                    patents.append(row)
                    
                    count += 1
                    # Process all records (no artificial limits)
                    if count % 10000 == 0:
                        logger.info(f"Processed {count} records so far...")
                        
            except csv.Error as e:
                # If that fails, try with different quoting and escaping
                logger.warning(f"Standard CSV parsing failed: {e}, trying alternative approach")
                try:
                    reader = csv.DictReader(
                        io.StringIO(csv_text),
                        delimiter='|',
                        quoting=csv.QUOTE_ALL,
                        skipinitialspace=True
                    )
                    count = 0
                    for row in reader:
                        patents.append(row)
                        count += 1
                        if count % 10000 == 0:
                            logger.info(f"Processed {count} records so far...")
                except csv.Error:
                    # Final attempt with minimal quoting and pipe delimiter
                    reader = csv.DictReader(
                        io.StringIO(csv_text),
                        delimiter='|',
                        quoting=csv.QUOTE_MINIMAL,
                        skipinitialspace=True,
                        quotechar='"',
                        escapechar='\\'
                    )
                    count = 0
                    for row in reader:
                        patents.append(row)
                        count += 1
                        if count % 10000 == 0:
                            logger.info(f"Processed {count} records so far...")
                    
        except Exception as e:
            logger.error(f"Error parsing CSV data: {e}")
            # Try to show first few lines of problematic CSV for debugging
            lines = csv_text.split('\n')[:3]
            logger.debug(f"First few lines of CSV: {lines}")
            # Show headers if available
            if lines:
                logger.info(f"CSV headers: {lines[0]}")
        
        return patents
    
    def parse_xml_data(self, xml_text: str) -> List[Dict]:
        """Parse XML data and extract patent information."""
        patents = []
        
        try:
            import xml.etree.ElementTree as ET
            
            # Clean up the XML text first
            xml_text = xml_text.strip()
            
            # Check if it's actually XML or if it's HTML/other format
            if not xml_text.startswith('<?xml') and not xml_text.startswith('<'):
                logger.warning("Data doesn't appear to be XML format")
                return patents
            
            # Try to handle BOM and encoding issues
            if xml_text.startswith('\ufeff'):
                xml_text = xml_text[1:]
            
            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as e:
                # Try with a more lenient approach
                logger.warning(f"XML parse error: {e}. Attempting to clean XML...")
                
                # Remove problematic characters and try again
                import re
                # Remove control characters except for tab, newline, and carriage return
                xml_text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_text)
                root = ET.fromstring(xml_text)
            
            # Look for patent-related elements
            patent_elements = []
            
            # Common patent XML element names
            patent_tags = ['patent', 'application', 'document', 'record', 'item', 'entry']
            
            for tag in patent_tags:
                found = root.findall(f".//{tag}")
                if found:
                    patent_elements.extend(found)
                    break
            
            # If no specific patent elements found, try all child elements
            if not patent_elements:
                patent_elements = list(root)
            
            for element in patent_elements:
                patent_data = {}
                
                # Extract text content and attributes
                if element.text and element.text.strip():
                    patent_data[element.tag] = element.text.strip()
                
                # Extract attributes
                for attr, value in element.attrib.items():
                    patent_data[f"{element.tag}_{attr}"] = value
                
                # Extract child elements
                for child in element:
                    if child.text and child.text.strip():
                        patent_data[child.tag] = child.text.strip()
                    
                    # Also check child attributes
                    for attr, value in child.attrib.items():
                        patent_data[f"{child.tag}_{attr}"] = value
                
                patent = self.extract_patent_info(patent_data)
                if patent:
                    patents.append(patent)
                        
        except Exception as e:
            logger.error(f"Error parsing XML data: {e}")
            # Show first few lines for debugging
            lines = xml_text.split('\n')[:3]
            logger.debug(f"First few lines of XML: {lines}")
            logger.info(f"XML content starts with: {xml_text[:100]}...")
        
        return patents
    
    def extract_patent_info(self, record: Dict) -> Optional[Dict]:
        """
        Extract patent information from a record.
        
        Args:
            record: Raw data record
            
        Returns:
            Standardized patent dictionary or None
        """
        try:
            # Map common field names to our standardized format (updated for Canadian bilingual fields)
            field_mappings = {
                'title': ['title', 'patent_title', 'name', 'invention_title', 'Title - Titre', 'Application/Patent Title English - Demande/Titre anglais du brevet', 'Application/Patent Title French - Demande/Titre français du brevet'],
                'description': ['description', 'abstract', 'summary', 'patent_abstract', 'Abstract - Abrégé', 'Abstract Text - Texte de l\'abrégé'],
                'patent_number': ['patent_number', 'patent_id', 'application_number', 'id', 'Patent Number - Numéro du brevet', 'Application Number - Numéro de demande'],
                'inventor_name': ['inventor', 'inventor_name', 'inventors', 'applicant', 'Inventor Name - Nom de l\'inventeur'],
                'assignee': ['assignee', 'assignee_name', 'owner', 'applicant_name', 'Assignee Name - Nom du cessionnaire'],
                'filing_date': ['filing_date', 'application_date', 'filed_date', 'date_filed', 'Filing Date - Date de dépôt', 'Application Filing Date - Date de dépôt de la demande'],
                'grant_date': ['grant_date', 'issue_date', 'granted_date', 'date_granted', 'Grant Date - Date de l\'octroi', 'Patent Grant Date - Date d\'octroi du brevet'],
                'classification': ['classification', 'ipc_class', 'class', 'category', 'IPC Class - Classe IPC'],
                'status': ['status', 'patent_status', 'state', 'Application Status Code - Code du statut de la demande', 'Application Status - État de la demande'],
                'url': ['url', 'link', 'patent_url', 'href']
            }
            
            patent = {}
            
            for std_field, possible_fields in field_mappings.items():
                value = None
                for field in possible_fields:
                    if field in record and record[field]:
                        value = str(record[field]).strip()
                        break
                patent[std_field] = value
            
            # Only return patent if we have at least a title or patent number
            if patent.get('title') or patent.get('patent_number'):
                patent['id'] = patent.get('patent_number') or f"patent_{hash(str(record))}"
                logger.debug(f"Extracted patent: {patent.get('title', 'No title')} (Number: {patent.get('patent_number', 'None')})")
                return patent
                
        except Exception as e:
            logger.error(f"Error extracting patent info: {e}")
        
        return None
    
    def determine_file_type(self, filename: str) -> str:
        """Determine the type of patent data file based on filename"""
        filename_lower = filename.lower()
        
        if 'pt_main' in filename_lower:
            return 'main'
        elif 'pt_abstract' in filename_lower:
            return 'abstract'
        elif 'pt_claim' in filename_lower:
            return 'claim'
        elif 'pt_disclosure' in filename_lower:
            return 'disclosure'
        elif 'pt_interested_party' in filename_lower:
            return 'interested_party'
        elif 'pt_ipc_classification' in filename_lower:
            return 'ipc_classification'
        elif 'pt_priority_claim' in filename_lower:
            return 'priority_claim'
        else:
            return 'unknown'
    
    def save_main_patents(self, data: List[Dict]):
        """Save main patent data to patents_main table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patents_main 
                    (patent_number, filing_date, grant_date, application_status_code, 
                     application_type_code, title_english, title_french, 
                     bibliographic_extract_date, country_publication_code, document_kind_type,
                     examination_request_date, filing_country_code, language_filing_code,
                     license_sale_indicator, pct_application_number, pct_publication_number,
                     pct_publication_date, parent_application_number, pct_article_22_39_date,
                     pct_section_371_date, pct_publication_country_code, publication_kind_type,
                     printed_amended_country_code, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('Filing Date - Date de dépôt'),
                    record.get('Grant Date - Date de l\'octroi'),
                    record.get('Application Status Code - Code du statut de la demande'),
                    record.get('Application Type Code - Code du type de la demande'),
                    record.get('Application/Patent Title English - Demande/Titre anglais du brevet'),
                    record.get('Application/Patent Title French - Demande/Titre français du brevet'),
                    record.get('Bibliographic File Extract Date - Date d\'extraction du fichier bibliographique'),
                    record.get('Country of Publication Code - Code du pays de publication'),
                    record.get('Document Kind Type - Genre du type de document'),
                    record.get('Examination Request Date - Date de la demande d\'examen'),
                    record.get('Filing Country Code - Code du pays de dépôt'),
                    record.get('Language of Filing Code - Langue du dépôt de la demande'),
                    1 if record.get('License For Sale Indicator - Indicateur de la licence de vente') == '1' else 0,
                    record.get('PCT Application Number - Numéro de demande du TCMB'),
                    record.get('PCT Publication Number - Numéro de publication du TCMB'),
                    record.get('PCT Publication Date - Date de publication du TCMB'),
                    record.get('Parent Application Number - Numéro de la demande principale'),
                    record.get('PCT Article 22-39 fulfilled Date - Date d\'accomplissement des articles 22 à 29 du TCMB'),
                    record.get('PCT Section 371 Date - Date de l\'article 371 du TCMB'),
                    record.get('PCT Publication Country Code - Code du pays de publication du TCMB'),
                    record.get('Publication Kind Type - Genre de type de publication'),
                    record.get('Printed as Amended Country Code - Code du pays de la demande imprimée après modification'),
                    datetime.now().isoformat()
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} main patent records")
            
        except Exception as e:
            logger.error(f"Error saving main patent data: {e}")
    
    def save_abstracts(self, data: List[Dict]):
        """Save patent abstracts to patent_abstracts table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_abstracts 
                    (patent_number, sequence_number, filing_language_code, 
                     abstract_language_code, abstract_text)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('Abstract text sequence number - Texte de l\'abrégé numéro de séquence'),
                    record.get('Language of Filing Code - Langue du type de dépôt'),
                    record.get('Abstract Language Code - Code de la langue du résumé'),
                    record.get('Abstract Text - Texte de l\'abrégé')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} patent abstracts")
            
        except Exception as e:
            logger.error(f"Error saving patent abstracts: {e}")
    
    def save_claims(self, data: List[Dict]):
        """Save patent claims to patent_claims table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_claims 
                    (patent_number, sequence_number, filing_language_code, claims_text)
                    VALUES (?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('Claims text sequence number - Texte des revendications numéro de séquence') or
                    record.get('Claim text sequence number - Texte des revendications numéro de séquence'),
                    record.get('Language of Filing Code - Langue du type de dépôt'),
                    record.get('Claims Text - Texte des revendications')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} patent claims")
            
        except Exception as e:
            logger.error(f"Error saving patent claims: {e}")
    
    def save_disclosures(self, data: List[Dict]):
        """Save patent disclosures to patent_disclosures table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_disclosures 
                    (patent_number, sequence_number, filing_language_code, disclosure_text)
                    VALUES (?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('Disclosure text sequence number - Texte de la divulgation numéro de séquence'),
                    record.get('Language of Filing Code - Langue du type de dépôt'),
                    record.get('Disclosure Text - Texte de la divulgation')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} patent disclosures")
            
        except Exception as e:
            logger.error(f"Error saving patent disclosures: {e}")
    
    def save_interested_parties(self, data: List[Dict]):
        """Save interested parties to patent_interested_parties table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_interested_parties 
                    (patent_number, agent_type_code, applicant_type_code, 
                     interested_party_type_code, interested_party_type, owner_enable_date,
                     ownership_end_date, party_name, party_address_line1, party_address_line2,
                     party_address_line3, party_address_line4, party_address_line5, party_city,
                     party_province_code, party_province, party_postal_code, 
                     party_country_code, party_country)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get("Agent Type Code - Code du type d'agent"),
                    record.get('Applicant Type Code - Code du type de demandeur'),
                    record.get('Interested Party Type Code - Code du type de partie intéressée'),
                    record.get('Interested Party Type - Type de partie intéressée'),
                    record.get('Owner Enable Date - Date de validation du propriétaire'),
                    record.get('Ownership End Date - Date de fin de propriété'),
                    record.get('Party Name - Nom de la partie'),
                    record.get('Party Address Line 1 - Adresse de la partie ligne 1'),
                    record.get('Party Address Line 2 - Adresse de la partie ligne 2'),
                    record.get('Party Address Line 3 - Adresse de la partie ligne 3'),
                    record.get('Party Address Line 4 - Adresse de la partie ligne 4'),
                    record.get('Party Address Line 5 - Adresse de la partie ligne 5'),
                    record.get('Party City - Ville de la partie'),
                    record.get('Party Province Code - Code de la province de la partie'),
                    record.get('Party Province - Province de la partie'),
                    record.get('Party Postal Code - Code postal de la partie'),
                    record.get('Party Country Code - Code du pays de la partie'),
                    record.get('Party Country - Pays de la partie')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} interested party records")
            
        except Exception as e:
            logger.error(f"Error saving interested parties: {e}")
    
    def save_ipc_classifications(self, data: List[Dict]):
        """Save IPC classifications to patent_ipc_classifications table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_ipc_classifications 
                    (patent_number, sequence_number, ipc_version_date, classification_level,
                     classification_status_code, classification_status, ipc_section_code,
                     ipc_section, ipc_class_code, ipc_class, ipc_subclass_code, ipc_subclass,
                     ipc_main_group_code, ipc_group, ipc_subgroup_code, ipc_subgroup)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('IPC Classification Sequence Number - Numéro de séquence de la classification de la CIB'),
                    record.get('IPC Version Date - Date de la version de la CIB'),
                    record.get('Classification Level - Niveau de classification'),
                    record.get('Classification Status Code - Code du statut de classification'),
                    record.get('Classification Status - Statut de classification'),
                    record.get('IPC Section Code - Code de la section de la CIB'),
                    record.get('IPC Section - Section de la CIB'),
                    record.get('IPC Class Code - Code de la classe de la CIB'),
                    record.get('IPC Class - Classe de la CIB'),
                    record.get('IPC Subclass Code - Code de la sous-classe de la CIB'),
                    record.get('IPC Subclass - Sous-classe de la CIB'),
                    record.get('IPC Main Group Code - Code du groupe principal de la CIB'),
                    record.get('IPC Group - Groupe de la CIB'),
                    record.get('IPC Subgroup Code - Code du sous-groupe de la CIB'),
                    record.get('IPC Subgroup - Sous-groupe de la CIB')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} IPC classification records")
            
        except Exception as e:
            logger.error(f"Error saving IPC classifications: {e}")
    
    def save_priority_claims(self, data: List[Dict]):
        """Save priority claims to patent_priority_claims table"""
        if not data:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO patent_priority_claims 
                    (patent_number, foreign_application_number, priority_claim_kind_code,
                     priority_claim_country_code, priority_claim_country, priority_claim_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    record.get('Patent Number - Numéro du brevet'),
                    record.get('Foreign Application/Patent Number - Numéro du brevet étranger / national'),
                    record.get('Priority Claim Kind Code - Code de type de revendications de priorité'),
                    record.get('Priority Claim Country Code - Code du pays d\'origine de revendications de priorité'),
                    record.get('Priority Claim Country - Pays d\'origine de revendications de priorité'),
                    record.get('Priority Claim Calendar Dt - Date de revendications de priorité')
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(data)} priority claim records")
            
        except Exception as e:
            logger.error(f"Error saving priority claims: {e}")
    
    def save_patents_to_db(self, patents: List[Dict]):
        """Save patent records to the SQLite database."""
        if not patents:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for patent in patents:
                cursor.execute('''
                    INSERT OR REPLACE INTO patents 
                    (id, title, description, patent_number, inventor_name, assignee,
                     filing_date, grant_date, classification, status, url, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    patent.get('id'),
                    patent.get('title'),
                    patent.get('description'),
                    patent.get('patent_number'),
                    patent.get('inventor_name'),
                    patent.get('assignee'),
                    patent.get('filing_date'),
                    patent.get('grant_date'),
                    patent.get('classification'),
                    patent.get('status'),
                    patent.get('url'),
                    datetime.now().isoformat()
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(patents)} patents to database")
            
        except Exception as e:
            logger.error(f"Error saving patents to database: {e}")
    
    def fetch_all_patent_data(self):
        """Main method to fetch all patent data from CKAN."""
        logger.info("Starting patent data fetch from Government of Canada CKAN")
        
        # Search for patent-related datasets
        datasets = self.search_patent_datasets("patent")
        
        if not datasets:
            logger.warning("No patent datasets found")
            return
        
        total_patents = 0
        
        for dataset in datasets:
            try:
                dataset_id = dataset['id']
                dataset_name = dataset.get('name', 'Unknown')
                dataset_title = dataset.get('title', 'No title')
                logger.info(f"Processing dataset: {dataset_name}")
                logger.info(f"Dataset title: {dataset_title}")
                
                # Get resources for this dataset
                resources = self.get_dataset_resources(dataset_id)
                
                for resource in resources:
                    # Skip if resource format is not supported
                    format_type = resource.get('format', '').lower()
                    resource_name = resource.get('name', 'Unknown')
                    resource_desc = resource.get('description', '')
                    resource_url = resource.get('url', '')
                    
                    logger.info(f"Found resource: {resource_name} ({format_type})")
                    if resource_desc:
                        logger.info(f"Resource description: {resource_desc[:200]}...")
                    logger.info(f"Resource URL: {resource_url}")
                    
                    # Handle ZIP files containing CSV data (common for CIPO data)
                    if format_type == 'zip' or resource_url.endswith('.zip'):
                        logger.info("Found ZIP file - attempting to download and extract")
                        patents = self.download_and_extract_zip(resource)
                        if patents:
                            self.save_patents_to_db(patents)
                            total_patents += len(patents)
                        continue
                    
                    if format_type not in ['json', 'csv', 'xml', 'rdf']:
                        continue
                    
                    logger.info(f"Processing resource: {resource_name} ({format_type})")
                    
                    # Download and parse the resource
                    patents = self.download_and_parse_resource(resource)
                    
                    if patents:
                        self.save_patents_to_db(patents)
                        total_patents += len(patents)
                    
                    # Be respectful with API calls
                    time.sleep(1)
                
                # Record that we've processed this dataset
                self.save_dataset_info(dataset)
                
            except Exception as e:
                logger.error(f"Error processing dataset {dataset.get('name', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Completed patent data fetch. Total patents collected: {total_patents}")
    
    def save_dataset_info(self, dataset: Dict):
        """Save information about processed datasets."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO datasets 
                (id, name, title, description, last_updated, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                dataset.get('id'),
                dataset.get('name'),
                dataset.get('title'),
                dataset.get('notes', '')[:500],  # Limit description length
                dataset.get('metadata_modified'),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error saving dataset info: {e}")
            
    def get_patent_count(self) -> int:
        """Get the total number of patents in the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM patents")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            logger.error(f"Error getting patent count: {e}")
            return 0

    def analyze_patent_trends(self):
        """Analyze patent trends and provide insights."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\n" + "="*60)
            print("🧠 CANADIAN PATENT ANALYSIS & INSIGHTS")
            print("="*60)
            
            # Basic statistics
            cursor.execute("SELECT COUNT(*) FROM patents")
            total_patents = cursor.fetchone()[0]
            print(f"📊 Total Patents in Database: {total_patents:,}")
            
            if total_patents == 0:
                print("No patents found in database. Run data fetch first.")
                return
            
            # Recent patents (last 5 years)
            cursor.execute("""
                SELECT COUNT(*) FROM patents 
                WHERE filing_date >= date('now', '-5 years')
            """)
            recent_patents = cursor.fetchone()[0]
            print(f"📈 Patents Filed in Last 5 Years: {recent_patents:,}")
            
            # Top patent categories/classifications
            print(f"\n🔥 TOP PATENT CATEGORIES:")
            cursor.execute("""
                SELECT classification, COUNT(*) as count 
                FROM patents 
                WHERE classification IS NOT NULL AND classification != ''
                GROUP BY classification 
                ORDER BY count DESC 
                LIMIT 10
            """)
            for i, (category, count) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2}. {category[:50]:<50} ({count:,} patents)")
            
            # Top inventors/assignees
            print(f"\n👨‍🔬 TOP INVENTORS/ASSIGNEES:")
            cursor.execute("""
                SELECT assignee, COUNT(*) as count 
                FROM patents 
                WHERE assignee IS NOT NULL AND assignee != ''
                GROUP BY assignee 
                ORDER BY count DESC 
                LIMIT 10
            """)
            for i, (assignee, count) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2}. {assignee[:50]:<50} ({count:,} patents)")
            
            # Patent trends by year
            print(f"\n📅 PATENT FILING TRENDS BY YEAR:")
            cursor.execute("""
                SELECT substr(filing_date, 1, 4) as year, COUNT(*) as count 
                FROM patents 
                WHERE filing_date IS NOT NULL AND filing_date != ''
                AND year BETWEEN '2020' AND '2024'
                GROUP BY year 
                ORDER BY year DESC
            """)
            year_data = cursor.fetchall()
            if year_data:
                for year, count in year_data:
                    bar = "█" * min(int(count / 100), 50)
                    print(f"  {year}: {bar} ({count:,})")
            
            # Identify emerging technologies
            print(f"\n🚀 EMERGING TECHNOLOGY KEYWORDS (Recent Patents):")
            cursor.execute("""
                SELECT title, classification FROM patents 
                WHERE filing_date >= date('now', '-2 years')
                AND title IS NOT NULL AND title != ''
                ORDER BY filing_date DESC
                LIMIT 100
            """)
            
            # Analyze titles for keywords
            titles = [row[0].lower() for row in cursor.fetchall() if row[0]]
            tech_keywords = {}
            
            # Technology keywords to look for
            keywords = [
                'ai', 'artificial intelligence', 'machine learning', 'neural network',
                'blockchain', 'cryptocurrency', 'quantum', 'solar', 'battery',
                'drone', 'autonomous', 'iot', 'internet of things', '5g',
                'virtual reality', 'vr', 'augmented reality', 'ar', 'biotech',
                'gene', 'crispr', 'nanotechnology', 'carbon capture',
                'renewable', 'sustainable', 'electric vehicle', 'ev'
            ]
            
            for title in titles:
                for keyword in keywords:
                    if keyword in title:
                        tech_keywords[keyword] = tech_keywords.get(keyword, 0) + 1
            
            # Sort and display top emerging tech
            sorted_keywords = sorted(tech_keywords.items(), key=lambda x: x[1], reverse=True)
            for i, (keyword, count) in enumerate(sorted_keywords[:10], 1):
                print(f"  {i:2}. {keyword.upper():<20} ({count} mentions)")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error analyzing patent trends: {e}")
    
    def suggest_patent_opportunities(self):
        """Suggest potential patent opportunities based on data analysis."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\n" + "="*60)
            print("💡 PATENT OPPORTUNITY SUGGESTIONS")
            print("="*60)
            
            # Find underexplored areas
            print("🔍 ANALYSIS-BASED SUGGESTIONS:")
            
            # Look for gaps in popular categories
            cursor.execute("""
                SELECT classification, COUNT(*) as count 
                FROM patents 
                WHERE classification IS NOT NULL AND classification != ''
                GROUP BY classification 
                ORDER BY count ASC 
                LIMIT 10
            """)
            
            print("\n📈 UNDEREXPLORED PATENT CATEGORIES (Potential Opportunities):")
            for i, (category, count) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2}. {category[:60]:<60} ({count} patents)")
            
            print("\n🎯 INNOVATION OPPORTUNITY AREAS:")
            
            opportunities = [
                ("🌱 Sustainability Tech", "Carbon capture, waste reduction, renewable energy storage"),
                ("🏠 Smart Home Integration", "IoT devices, home automation, energy management"),
                ("🚗 Transportation Innovation", "Electric vehicle tech, autonomous systems, traffic optimization"),
                ("🏥 Healthcare Technology", "Telemedicine devices, health monitoring, medical AI"),
                ("🌐 Remote Work Tools", "Collaboration software, virtual meeting tech, productivity apps"),
                ("🍔 Food Technology", "Plant-based alternatives, food preservation, smart agriculture"),
                ("👕 Wearable Technology", "Health monitoring, fitness tracking, smart clothing"),
                ("🔒 Cybersecurity", "Privacy protection, secure communications, identity verification"),
                ("♿ Accessibility Tech", "Assistive devices, universal design, inclusive technology"),
                ("🎓 Educational Technology", "Learning platforms, skills assessment, adaptive learning")
            ]
            
            for category, description in opportunities:
                print(f"  {category} - {description}")
            
            print("\n🎲 RANDOM PATENT IDEA GENERATORS:")
            
            # Generate some creative combinations
            import random
            
            tech_areas = ["AI", "IoT", "Blockchain", "AR/VR", "Robotics", "Biotech", "Quantum"]
            applications = ["Healthcare", "Education", "Transportation", "Agriculture", "Entertainment", 
                          "Manufacturing", "Communication", "Energy", "Security", "Environment"]
            problems = ["efficiency", "cost reduction", "accessibility", "sustainability", "security", 
                       "user experience", "automation", "personalization", "scalability", "integration"]
            
            print("\n🚀 CREATIVE PATENT IDEA COMBINATIONS:")
            for i in range(5):
                tech = random.choice(tech_areas)
                app = random.choice(applications)
                problem = random.choice(problems)
                print(f"  💡 {tech} + {app} + {problem.upper()}")
                print(f"     Example: '{tech}-powered {app.lower()} system for improved {problem}'")
            
            # Suggest based on recent trends
            print("\n📊 TREND-BASED SUGGESTIONS:")
            cursor.execute("""
                SELECT assignee, COUNT(*) as count 
                FROM patents 
                WHERE filing_date >= date('now', '-2 years')
                AND assignee IS NOT NULL AND assignee != ''
                GROUP BY assignee 
                ORDER BY count DESC 
                LIMIT 5
            """)
            
            active_companies = cursor.fetchall()
            if active_companies:
                print("🏢 Consider innovations that complement or compete with recent filings by:")
                for company, count in active_companies:
                    print(f"  • {company[:50]} ({count} recent patents)")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error suggesting patent opportunities: {e}")
    
    def generate_patent_report(self):
        """Generate a comprehensive patent analysis report."""
        print("\n" + "="*80)
        print("📋 COMPREHENSIVE CANADIAN PATENT ANALYSIS REPORT")
        print("="*80)
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.analyze_patent_trends()
        self.suggest_patent_opportunities()
        
        print("\n" + "="*80)
        print("📝 NEXT STEPS FOR PATENT RESEARCH:")
        print("="*80)
        print("1. 🔍 Research specific technologies that interest you")
        print("2. 📚 Study existing patents in your area of interest")
        print("3. 🧠 Identify gaps or improvements in current solutions")
        print("4. 💼 Consider market demand and commercial viability")
        print("5. 🏛️  Consult with a patent attorney for filing guidance")
        print("6. 🔬 Develop and test your innovative concepts")
        print("="*80)


def main():
    """Main function to run the patent data fetcher."""
    fetcher = CanadianPatentFetcher()
    
    print("🍁 Canadian Patent Data Fetcher & Analyzer")
    print("=" * 50)
    print(f"Database: {fetcher.db_path}")
    print(f"Cache Directory: {fetcher.cache_dir}")
    print(f"Current patent count: {fetcher.get_patent_count()}")
    print()
    
    while True:
        print("\n📋 MENU OPTIONS:")
        print("1. 📥 Fetch patent data from CKAN")
        print("2. 📊 Analyze patent trends & insights")
        print("3. 💡 Get patent opportunity suggestions")
        print("4. 📋 Generate full analysis report")
        print("5. 🗂️  Show cache status")
        print("6. 🚪 Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            try:
                print("\n🔄 Starting patent data fetch...")
                fetcher.fetch_all_patent_data()
                final_count = fetcher.get_patent_count()
                print(f"\n✅ Fetch completed. Total patents in database: {final_count}")
            except KeyboardInterrupt:
                print("\n❌ Fetch interrupted by user")
            except Exception as e:
                print(f"\n❌ Error during fetch: {e}")
        
        elif choice == '2':
            fetcher.analyze_patent_trends()
            
        elif choice == '3':
            fetcher.suggest_patent_opportunities()
            
        elif choice == '4':
            fetcher.generate_patent_report()
            
        elif choice == '5':
            try:
                conn = sqlite3.connect(fetcher.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM file_cache")
                cached_files = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM file_cache WHERE processed = TRUE")
                processed_files = cursor.fetchone()[0]
                cursor.execute("SELECT SUM(file_size) FROM file_cache")
                total_size = cursor.fetchone()[0] or 0
                conn.close()
                
                print(f"\n📁 CACHE STATUS:")
                print(f"  • Cached files: {cached_files}")
                print(f"  • Processed files: {processed_files}")
                print(f"  • Total cache size: {total_size / 1024 / 1024:.1f} MB")
                print(f"  • Cache directory: {fetcher.cache_dir}")
                
            except Exception as e:
                print(f"❌ Error checking cache: {e}")
        
        elif choice == '6':
            print("\n👋 Thank you for using Canadian Patent Analyzer!")
            break
            
        else:
            print("\n❌ Invalid choice. Please enter 1-6.")


if __name__ == "__main__":
    main()