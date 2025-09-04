#!/usr/bin/env python3
"""
Flask Patent Browser - Web interface for browsing and searching Canadian patents
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import sqlite3
import math
import threading
import time
import os
import logging
from datetime import datetime
from pull_patents import CanadianPatentFetcher

app = Flask(__name__)
app.config['SECRET_KEY'] = 'patent-browser-secret-key-2024'

DATABASE = 'cnd_patents.db'

def create_database_schema(db_path=DATABASE):
    """Create comprehensive database schema if database doesn't exist"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        logging.info("Creating comprehensive patent database schema...")
        
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
        
        # Additional tables for the patent fetcher functionality
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                file_size INTEGER,
                last_modified TEXT,
                processed BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                file_path TEXT,
                file_size INTEGER,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        logging.info("Creating database indexes...")
        
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
        
        logging.info("✅ Comprehensive patent database schema created successfully!")
        return True
        
    except Exception as e:
        logging.error(f"Error creating database schema: {e}")
        return False

def initialize_database():
    """Initialize database on application startup"""
    if not os.path.exists(DATABASE):
        logging.info(f"Database {DATABASE} not found. Creating new database...")
        if create_database_schema():
            logging.info("Database created successfully. You can now download patent data using the /download page.")
        else:
            logging.error("Failed to create database!")
    else:
        logging.info(f"Database {DATABASE} found.")

# Global variables for download status tracking
download_status = {
    'active': False,
    'progress': '',
    'total_downloaded': 0,
    'current_file': '',
    'start_time': None,
    'error': None
}

def get_db_connection():
    """Get database connection"""
    # Check if database exists, create if it doesn't
    if not os.path.exists(DATABASE):
        logging.warning(f"Database {DATABASE} not found during connection attempt. Creating...")
        create_database_schema()
    
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def execute_search_query(search_term, status_filter, category_filter, sort_by, sort_order, page, per_page):
    """Execute search query with filters and pagination"""
    conn = get_db_connection()
    
    # Base query using new comprehensive schema
    query = """
    SELECT 
           pm.patent_number, 
           pm.title_english as title,
           GROUP_CONCAT(pa.abstract_text, ' ') as description,
           GROUP_CONCAT(CASE WHEN pip.interested_party_type = 'Inventor' THEN pip.party_name END, '; ') as inventor_name,
           GROUP_CONCAT(CASE WHEN pip.interested_party_type IN ('Owner', 'Assignee') THEN pip.party_name END, '; ') as assignee,
           pm.filing_date, 
           pm.grant_date, 
           pm.application_status_code as status,
           GROUP_CONCAT(pic.ipc_section_code || pic.ipc_class_code, '; ') as classification,
           pm.created_at,
           GROUP_CONCAT(pic.ipc_section, '; ') as categories
    FROM patents_main pm
    LEFT JOIN patent_abstracts pa ON pm.patent_number = pa.patent_number
    LEFT JOIN patent_interested_parties pip ON pm.patent_number = pip.patent_number
    LEFT JOIN patent_ipc_classifications pic ON pm.patent_number = pic.patent_number
    WHERE pm.patent_number IS NOT NULL
    """
    
    params = []
    
    # Add search term filter
    if search_term:
        query += " AND (pm.title_english LIKE ? OR pm.title_french LIKE ? OR pa.abstract_text LIKE ? OR pip.party_name LIKE ?)"
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
    
    # Add status filter
    if status_filter and status_filter != 'all':
        query += " AND pm.application_status_code = ?"
        params.append(status_filter)
    
    # Add category filter
    if category_filter and category_filter != 'all':
        query += " AND pic.ipc_section = ?"
        params.append(category_filter)
    
    query += " GROUP BY pm.patent_number"
    
    # Add sorting
    sort_columns = {
        'patent_number': 'pm.patent_number',
        'title': 'pm.title_english',
        'filing_date': 'pm.filing_date',
        'status': 'pm.application_status_code',
        'assignee': 'assignee'
    }
    
    if sort_by in sort_columns:
        query += f" ORDER BY {sort_columns[sort_by]} {sort_order}"
    else:
        query += " ORDER BY pm.patent_number ASC"
    
    # Get total count for pagination
    count_query = f"""
    SELECT COUNT(DISTINCT pm.patent_number)
    FROM patents_main pm
    LEFT JOIN patent_abstracts pa ON pm.patent_number = pa.patent_number
    LEFT JOIN patent_interested_parties pip ON pm.patent_number = pip.patent_number
    LEFT JOIN patent_ipc_classifications pic ON pm.patent_number = pic.patent_number
    WHERE pm.patent_number IS NOT NULL
    """
    
    count_params = []
    if search_term:
        count_query += " AND (pm.title_english LIKE ? OR pm.title_french LIKE ? OR pa.abstract_text LIKE ? OR pip.party_name LIKE ?)"
        count_params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
    
    if status_filter and status_filter != 'all':
        count_query += " AND pm.application_status_code = ?"
        count_params.append(status_filter)
    
    if category_filter and category_filter != 'all':
        count_query += " AND pic.ipc_section = ?"
        count_params.append(category_filter)
    
    total = conn.execute(count_query, count_params).fetchone()[0]
    
    # Add pagination
    offset = (page - 1) * per_page
    query += f" LIMIT {per_page} OFFSET {offset}"
    
    # Execute main query
    patents = conn.execute(query, params).fetchall()
    
    conn.close()
    
    return patents, total

@app.route('/')
def index():
    """Main patents listing page"""
    # Get filter options
    conn = get_db_connection()
    
    # Get available statuses
    statuses = conn.execute("SELECT DISTINCT application_status_code as status FROM patents_main WHERE application_status_code IS NOT NULL ORDER BY application_status_code").fetchall()
    
    # Get available categories (IPC sections)
    categories = conn.execute("SELECT DISTINCT ipc_section as category_name FROM patent_ipc_classifications WHERE ipc_section IS NOT NULL ORDER BY ipc_section").fetchall()
    
    conn.close()
    
    # Get search parameters
    search_term = request.args.get('search', '')
    status_filter = request.args.get('status', 'all')
    category_filter = request.args.get('category', 'all')
    sort_by = request.args.get('sort', 'patent_number')
    sort_order = request.args.get('order', 'asc')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 25))
    
    # Execute search
    patents, total = execute_search_query(search_term, status_filter, category_filter, 
                                         sort_by, sort_order, page, per_page)
    
    # Calculate pagination
    total_pages = math.ceil(total / per_page)
    
    return render_template('index.html', 
                         patents=patents,
                         statuses=statuses,
                         categories=categories,
                         search_term=search_term,
                         status_filter=status_filter,
                         category_filter=category_filter,
                         sort_by=sort_by,
                         sort_order=sort_order,
                         page=page,
                         per_page=per_page,
                         total=total,
                         total_pages=total_pages)

@app.route('/patent/<patent_number>')
def patent_detail(patent_number):
    """Patent detail page - uses new comprehensive data structure"""
    return render_template('patent_detail_comprehensive.html', patent_number=patent_number)

@app.route('/analytics')
def analytics():
    """Analytics dashboard"""
    conn = get_db_connection()
    
    # Get basic stats from new schema
    total_patents = conn.execute("SELECT COUNT(*) FROM patents_main").fetchone()[0]
    
    # Status distribution using new schema
    status_stats = conn.execute("""
        SELECT application_status_code as status, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM patents_main), 1) as percentage
        FROM patents_main 
        WHERE application_status_code IS NOT NULL 
        GROUP BY application_status_code 
        ORDER BY count DESC
    """).fetchall()
    
    # Category distribution using IPC classifications
    category_stats = conn.execute("""
        SELECT 
            pic.ipc_section as category_name, 
            COUNT(DISTINCT pm.patent_number) as count,
            CASE 
                WHEN pic.ipc_section = 'A' THEN '#FF6B6B'
                WHEN pic.ipc_section = 'B' THEN '#4ECDC4'
                WHEN pic.ipc_section = 'C' THEN '#45B7D1'
                WHEN pic.ipc_section = 'D' THEN '#96CEB4'
                WHEN pic.ipc_section = 'E' THEN '#FFEAA7'
                WHEN pic.ipc_section = 'F' THEN '#DDA0DD'
                WHEN pic.ipc_section = 'G' THEN '#98D8C8'
                WHEN pic.ipc_section = 'H' THEN '#F7DC6F'
                ELSE '#BDC3C7'
            END as color
        FROM patent_ipc_classifications pic
        JOIN patents_main pm ON pic.patent_number = pm.patent_number
        WHERE pic.ipc_section IS NOT NULL 
        GROUP BY pic.ipc_section
        ORDER BY count DESC
    """).fetchall()
    
    # Year distribution using new schema
    year_stats = conn.execute("""
        SELECT substr(filing_date, 1, 4) as year, COUNT(*) as count
        FROM patents_main 
        WHERE filing_date LIKE '19__-__-__' OR filing_date LIKE '20__-__-__'
        GROUP BY year
        ORDER BY year DESC
        LIMIT 20
    """).fetchall()
    
    # Top assignees from interested parties
    assignee_stats = conn.execute("""
        SELECT pip.party_name as assignee, COUNT(DISTINCT pip.patent_number) as count
        FROM patent_interested_parties pip 
        WHERE pip.interested_party_type IN ('Owner', 'Assignee', 'Applicant') 
        AND pip.party_name IS NOT NULL 
        AND pip.party_name != 'N/A' 
        AND pip.party_name != ''
        GROUP BY pip.party_name
        ORDER BY count DESC
        LIMIT 15
    """).fetchall()
    
    # Technology categories using title analysis (since we don't have IPC yet)
    tech_keywords = conn.execute("""
        SELECT 
            CASE 
                WHEN LOWER(title_english) LIKE '%computer%' OR LOWER(title_english) LIKE '%software%' OR LOWER(title_english) LIKE '%digital%' THEN 'Computing & Software'
                WHEN LOWER(title_english) LIKE '%medical%' OR LOWER(title_english) LIKE '%pharmaceutical%' OR LOWER(title_english) LIKE '%drug%' OR LOWER(title_english) LIKE '%treatment%' THEN 'Medical & Pharmaceutical'
                WHEN LOWER(title_english) LIKE '%engine%' OR LOWER(title_english) LIKE '%motor%' OR LOWER(title_english) LIKE '%vehicle%' OR LOWER(title_english) LIKE '%automotive%' THEN 'Mechanical & Automotive'
                WHEN LOWER(title_english) LIKE '%electronic%' OR LOWER(title_english) LIKE '%circuit%' OR LOWER(title_english) LIKE '%semiconductor%' THEN 'Electronics'
                WHEN LOWER(title_english) LIKE '%chemical%' OR LOWER(title_english) LIKE '%compound%' OR LOWER(title_english) LIKE '%polymer%' THEN 'Chemistry'
                WHEN LOWER(title_english) LIKE '%communication%' OR LOWER(title_english) LIKE '%wireless%' OR LOWER(title_english) LIKE '%network%' THEN 'Communications'
                WHEN LOWER(title_english) LIKE '%energy%' OR LOWER(title_english) LIKE '%solar%' OR LOWER(title_english) LIKE '%battery%' THEN 'Energy & Power'
                ELSE 'Other'
            END as category,
            COUNT(*) as count
        FROM patents_main 
        WHERE title_english IS NOT NULL 
        AND grant_date > '1990-01-01'  -- Focus on more recent patents
        GROUP BY category
        ORDER BY count DESC
    """).fetchall()
    
    # Status code meanings
    status_meanings = {
        'EX': 'Expired - Patent term has ended',
        'DE': 'Dead - Application abandoned or withdrawn',
        'LA': 'Lapsed - Patent lapsed due to non-payment',
        'GR': 'Granted - Patent has been granted and is active',
        'CO': 'Conditional - Application conditionally approved',
        'ER': 'Examination Requested - Under examination',
        'RP': 'Request to Publish - Published but not yet examined',
        'AL': 'Allowed - Application allowed, pending final steps',
        'PG': 'Pending Grant - Final processing before grant',
        'WI': 'Withdrawn - Application withdrawn by applicant',
        'PI': 'Published International - PCT application published',
        'AA': 'Application Acknowledged - Initial processing',
        'CA': 'Continued Application - Continuation of earlier app',
        'SU': 'Suspended - Application processing suspended',
        'NI': 'Notice Issued - Official notice sent to applicant',
        'AC': 'Application Complete - Ready for examination',
        'FA': 'Final Action - Examiner final decision issued',
        'FF': 'File Forwarded - Transferred to another office',
        'CE': 'Certificate Error - Error in patent certificate',
        'DP': 'Deferred Publication - Publication delayed'
    }
    
    conn.close()
    
    return render_template('analytics.html',
                         total_patents=total_patents,
                         status_stats=status_stats,
                         category_stats=category_stats,
                         year_stats=year_stats,
                         assignee_stats=assignee_stats,
                         tech_keywords=tech_keywords,
                         status_meanings=status_meanings)

@app.route('/api/search')
def api_search():
    """API endpoint for search suggestions"""
    term = request.args.get('term', '')
    if len(term) < 2:
        return jsonify([])
    
    conn = get_db_connection()
    
    suggestions = conn.execute("""
        SELECT DISTINCT title_english as title
        FROM patents_main 
        WHERE title_english LIKE ? 
        LIMIT 10
    """, (f"%{term}%",)).fetchall()
    
    conn.close()
    
    return jsonify([row['title'] for row in suggestions])

def download_patents_background():
    """Background function to download patents"""
    global download_status
    
    try:
        download_status['active'] = True
        download_status['progress'] = 'Initializing patent fetcher...'
        download_status['start_time'] = time.time()
        download_status['error'] = None
        
        # Initialize the patent fetcher
        fetcher = CanadianPatentFetcher(db_path=DATABASE)
        
        download_status['progress'] = 'Checking for available patent datasets...'
        
        # Get current patent count
        initial_count = fetcher.get_patent_count()
        
        # Custom logging to update our status
        import logging
        
        class StatusHandler(logging.Handler):
            def emit(self, record):
                global download_status
                message = record.getMessage()
                if 'Processing' in message or 'Found' in message or 'Downloaded' in message:
                    download_status['progress'] = message
                if 'patents' in message.lower() and any(word in message for word in ['extracted', 'saved', 'found']):
                    try:
                        # Extract number from messages like "Found 1234 patents"
                        import re
                        numbers = re.findall(r'\d+', message)
                        if numbers:
                            download_status['total_downloaded'] = max(download_status['total_downloaded'], int(numbers[0]))
                    except:
                        pass
        
        # Add our custom handler
        status_handler = StatusHandler()
        logging.getLogger('pull_patents').addHandler(status_handler)
        
        download_status['progress'] = 'Starting patent data fetch...'
        
        # Start the download process
        fetcher.fetch_all_patent_data()
        
        # Get final count
        final_count = fetcher.get_patent_count()
        download_status['total_downloaded'] = final_count - initial_count
        download_status['progress'] = f'Download completed! Added {download_status["total_downloaded"]} new patents.'
        
    except Exception as e:
        download_status['error'] = str(e)
        download_status['progress'] = f'Download failed: {str(e)}'
    finally:
        download_status['active'] = False

@app.route('/api/download/start', methods=['POST'])
def start_download():
    """API endpoint to start patent download"""
    global download_status
    
    if download_status['active']:
        return jsonify({
            'success': False,
            'message': 'Download already in progress'
        })
    
    # Start download in background thread
    download_thread = threading.Thread(target=download_patents_background)
    download_thread.daemon = True
    download_thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Patent download started'
    })

@app.route('/api/download/status')
def download_status_api():
    """API endpoint to get download status"""
    global download_status
    
    status_copy = download_status.copy()
    
    # Calculate elapsed time if download is active
    if status_copy['active'] and status_copy['start_time']:
        status_copy['elapsed_time'] = int(time.time() - status_copy['start_time'])
    else:
        status_copy['elapsed_time'] = 0
    
    # Get current patent count and database size
    try:
        conn = get_db_connection()
        current_count = conn.execute("SELECT COUNT(*) FROM patents_main").fetchone()[0]
        conn.close()
        status_copy['current_patent_count'] = current_count
        
        # Get database file size
        import os
        if os.path.exists(DATABASE):
            db_size_bytes = os.path.getsize(DATABASE)
            if db_size_bytes > 1024*1024:
                status_copy['db_size'] = f"{db_size_bytes / (1024*1024):.1f} MB"
            else:
                status_copy['db_size'] = f"{db_size_bytes / 1024:.1f} KB"
        else:
            status_copy['db_size'] = "0 KB"
    except:
        status_copy['current_patent_count'] = 0
        status_copy['db_size'] = "Unknown"
    
    return jsonify(status_copy)

@app.route('/download')
def download_page():
    """Patent download management page"""
    return render_template('download.html')

@app.route('/api/patent/<patent_number>/details')
def get_patent_details(patent_number):
    """API endpoint to get comprehensive patent details"""
    try:
        conn = get_db_connection()
        
        # Get main patent info
        patent = conn.execute("""
            SELECT * FROM patents_main WHERE patent_number = ?
        """, (patent_number,)).fetchone()
        
        if not patent:
            conn.close()
            return jsonify({'error': 'Patent not found'}), 404
        
        # Get abstracts
        abstracts = conn.execute("""
            SELECT * FROM patent_abstracts 
            WHERE patent_number = ? 
            ORDER BY sequence_number
        """, (patent_number,)).fetchall()
        
        # Get interested parties
        parties = conn.execute("""
            SELECT * FROM patent_interested_parties 
            WHERE patent_number = ? 
            ORDER BY interested_party_type, party_name
        """, (patent_number,)).fetchall()
        
        # Get IPC classifications
        classifications = conn.execute("""
            SELECT * FROM patent_ipc_classifications 
            WHERE patent_number = ? 
            ORDER BY sequence_number
        """, (patent_number,)).fetchall()
        
        # Get priority claims
        priority_claims = conn.execute("""
            SELECT * FROM patent_priority_claims 
            WHERE patent_number = ? 
            ORDER BY priority_claim_date
        """, (patent_number,)).fetchall()
        
        # Get claims count
        claims_count = conn.execute("""
            SELECT COUNT(*) FROM patent_claims WHERE patent_number = ?
        """, (patent_number,)).fetchone()[0]
        
        # Get disclosure count
        disclosure_count = conn.execute("""
            SELECT COUNT(*) FROM patent_disclosures WHERE patent_number = ?
        """, (patent_number,)).fetchone()[0]
        
        conn.close()
        
        # Convert to dict format
        result = {
            'patent': dict(patent),
            'abstracts': [dict(row) for row in abstracts],
            'parties': [dict(row) for row in parties],
            'classifications': [dict(row) for row in classifications],
            'priority_claims': [dict(row) for row in priority_claims],
            'claims_count': claims_count,
            'disclosure_count': disclosure_count
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/patent/<patent_number>/claims')
def get_patent_claims(patent_number):
    """API endpoint to get patent claims (on-demand loading)"""
    try:
        conn = get_db_connection()
        
        claims = conn.execute("""
            SELECT * FROM patent_claims 
            WHERE patent_number = ? 
            ORDER BY sequence_number
        """, (patent_number,)).fetchall()
        
        conn.close()
        
        return jsonify([dict(row) for row in claims])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/patent/<patent_number>/disclosure')
def get_patent_disclosure(patent_number):
    """API endpoint to get patent disclosure (on-demand loading)"""
    try:
        conn = get_db_connection()
        
        disclosures = conn.execute("""
            SELECT * FROM patent_disclosures 
            WHERE patent_number = ? 
            ORDER BY sequence_number
        """, (patent_number,)).fetchall()
        
        conn.close()
        
        return jsonify([dict(row) for row in disclosures])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Initialize database on startup
    initialize_database()
    
    app.run(debug=True, host='0.0.0.0', port=5000)