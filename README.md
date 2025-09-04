# Canadian Patent Analyzer

A comprehensive tool for downloading, storing, and analyzing Canadian patent data from the Government of Canada's Open Data portal. This application fetches patent information from the CKAN API and provides both command-line analysis tools and a web interface for browsing patents.

## Features

### Data Collection
- **Automated Patent Fetching**: Downloads all Canadian patent data from Government of Canada CKAN API
- **Comprehensive Data Storage**: Stores complete patent information including:
  - Main patent details (filing dates, grant dates, status)
  - Patent abstracts in English and French
  - Claims and disclosure text
  - Interested parties (inventors, assignees, agents)
  - IPC classifications
  - Priority claims
- **Smart Caching**: Avoids re-downloading already processed files
- **Incremental Updates**: Only processes new or changed data

### Web Interface
- **Patent Browser**: Search and browse patents with advanced filtering
- **Analytics Dashboard**: Visualizations of patent trends, categories, and statistics
- **Detailed Patent View**: Complete patent information with lazy-loaded claims and disclosures
- **Download Management**: Monitor and control bulk patent downloads

### Analysis Tools
- **Trend Analysis**: Patent filing trends by year, category, and status
- **Technology Insights**: Identify emerging technologies and popular patent areas
- **Market Intelligence**: Top assignees, inventors, and patent categories
- **Opportunity Identification**: Suggestions for underexplored patent areas

## Installation

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd ckan
```

2. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start Web Interface

1. Start the web application:
```bash
./start.sh
```

2. Open your browser to http://localhost:5000

3. Use the download page to fetch patent data or browse existing data

### Command Line Usage

#### Interactive Patent Fetcher
```bash
python3 pull_patents.py
```

Options:
1. Fetch patent data from CKAN
2. Analyze patent trends & insights  
3. Get patent opportunity suggestions
4. Generate full analysis report
5. Show cache status
6. Exit

#### Bulk Download (Automated)
```bash
./download_all_patents.sh
```

This script will:
- Download all available Canadian patent data
- Process and import into SQLite database
- Display progress and final statistics
- **Warning**: Can take 2-6 hours and create 100-500MB database

## Database Schema

The application uses SQLite with comprehensive patent data structure:

- **patents_main**: Core patent information
- **patent_abstracts**: Patent abstracts in multiple languages
- **patent_claims**: Patent claims text
- **patent_disclosures**: Full patent disclosure text
- **patent_interested_parties**: Inventors, assignees, agents
- **patent_ipc_classifications**: International Patent Classification codes
- **patent_priority_claims**: Priority claim information

## Web Interface Features

### Patent Browser (`/`)
- Search patents by title, abstract, inventor, or assignee
- Filter by status (granted, expired, pending, etc.)
- Filter by IPC category
- Sort by various fields
- Pagination for large result sets

### Analytics Dashboard (`/analytics`)
- Patent statistics and distributions
- Filing trends by year
- Technology category breakdowns
- Top assignees and inventors
- Interactive charts and visualizations

### Patent Details (`/patent/<number>`)
- Complete patent information
- Lazy-loaded claims and disclosures (for performance)
- Interested parties and classifications
- Priority claims and related data

### Download Management (`/download`)
- Start bulk patent downloads
- Monitor download progress
- View database statistics

## API Endpoints

- `GET /api/patent/<number>/details` - Get comprehensive patent details
- `GET /api/patent/<number>/claims` - Get patent claims (lazy-loaded)
- `GET /api/patent/<number>/disclosure` - Get patent disclosure text
- `GET /api/search?term=<query>` - Search suggestions
- `POST /api/download/start` - Start bulk download
- `GET /api/download/status` - Get download progress

## Data Source

Patent data is fetched from:
- **Government of Canada Open Data Portal**
- **CKAN API**: https://open.canada.ca/data/api/3
- **Dataset**: Patent data from Canadian Intellectual Property Office (CIPO)

## File Structure

```
├── app.py                    # Flask web application
├── pull_patents.py           # Patent data fetcher and analyzer
├── download_all_patents.sh   # Automated download script
├── start.sh                  # Start web application
├── requirements.txt          # Python dependencies
├── cnd_patents.db           # SQLite database (created on first run)
├── patent_cache/            # Cached downloaded files
└── templates/               # HTML templates for web interface
```

## Performance Notes

- Initial patent download can take several hours
- Final database size: 100-500MB depending on data volume
- Web interface uses lazy loading for large text fields
- Indexes are created for optimal search performance

## Development

To contribute or modify:

1. Fork the repository
2. Create a feature branch
3. Make changes and test thoroughly
4. Submit a pull request

## License

This project is released under the MIT License. Patent data is provided by the Government of Canada under the Open Government License.

## Support

For issues or questions:
1. Check existing issues on GitHub
2. Create a new issue with detailed description
3. Include system information and error messages

---

**Note**: This tool is for research and analysis purposes. For official patent information, always consult the official Canadian Intellectual Property Office (CIPO) database.