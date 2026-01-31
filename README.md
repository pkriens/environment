# Environment Data Processing

Python workspace for processing environmental and water data.

## Project Structure

```
Environment Workspace/
├── 📦 ddapi/                 # Digital Delta API Python package
│   ├── __init__.py
│   ├── client.py             # DD API client (uses OData)
│   ├── cli.py                # DD API command-line interface
│   ├── models.py             # DD API data models
│   └── exceptions.py         # DD API exceptions
├── 📦 odata/                 # Generic OData v4 Python package  
│   ├── __init__.py
│   ├── client.py             # OData client + CLI
│   └── exceptions.py         # OData exceptions
├── 🔧 CLI Scripts
│   ├── ddapi_cli.py          # DD API CLI entry point
│   └── odata_cli.py          # OData CLI entry point
├── 📊 Example Scripts
│   ├── examples.py           # DD API usage examples
│   ├── find_counts.py        # Count analysis script
│   ├── mock_client.py        # Mock client for testing
│   └── demo.py               # Demonstration script
├── 📁 Data Directories
│   ├── aquo_data/            # Water quality scripts and data
│   ├── ddapi_data/           # DD API documentation and legacy
│   ├── water/                # Water authority CSV data and processing
│   └── csvs/                 # CSV datasets
└── ⚙️ Configuration
    ├── pyproject.toml         # Python package configuration
    ├── LICENSE                # MIT License
    └── README.md              # This file
```

## Python Packages

### 🌊 **ddapi** - Digital Delta API Client
Python package specifically designed for the Dutch Digital Delta API V3.

- **Features**: Type-safe models, search helpers, CLI tool
- **Usage**: `from ddapi import DDClient`
- **CLI**: `./ddapi_cli.py` or `python3 -m ddapi.cli`

### 🔗 **odata** - Generic OData Client  
Python package for any OData v4 API.

- **Features**: Query builder, CLI, multiple output formats  
- **Usage**: `from odata import ODataClient`
- **CLI**: `./odata_cli.py` or `python3 -m odata.client`

### 📊 **Scripts**
Additional tools and examples for data processing.

## Quick Start

### Installation

Install the entire workspace as a Python package:

```bash
pip install -e .
```

### Usage Examples

```bash
# Generic OData queries (any service)
./odata_cli.py --url https://services.odata.org/V4/Northwind/Northwind.svc Categories --top 5

# Dutch water data (DD API specific)  
./ddapi_cli.py observations --organisation RWS --limit 10

# Python modules
python3 -c "from ddapi import DDClient; from odata import ODataClient"

# Run demonstration
./demo.py
```

## Development

Install the workspace in development mode:

```bash
pip install -e ".[dev]"
```

Run tests and linting:

```bash
pytest
black ddapi/ odata/
isort ddapi/ odata/
mypy ddapi/ odata/
```

## Data Directories

- **aquo_data/**: Water quality analysis scripts and data
- **ddapi_data/**: DD API documentation, legacy files, and data  
- **water/**: Water authority datasets and processing scripts
- **csvs/**: CSV data files for analysis

## API Documentation

- [DD API V3 Specification](https://digitaledeltaorg.github.io/DD-API-V3-ReSpec/)
- [OData Query Syntax](https://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html)