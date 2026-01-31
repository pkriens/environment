# RWS Water Quality Portal (WKP) Downloader

This script downloads data from the Dutch Water Quality Portal (WKP) at https://wkp.rws.nl/

## Features

- **Subject Selection**: Download specific subjects by ID or pattern (supports globbing)
- **Filtering**: Filter by theme, years, area levels, and area names
- **Testing Support**: Limit requests for testing and dry-run capability
- **Hierarchy Display**: View the complete data structure and relationships
- **Subject Organization**: List subjects by theme or subject code
- **Subcommand Structure**: Clean command organization with `subjects` and `dl` subcommands

## Commands

The script uses subcommands for different operations:

- **`subjects`** - List and browse available subjects
- **`dl`** - Download subject data

## Usage Examples

### Browse Subjects
```bash
# List all available subjects
python3 rws.py subjects

# Show data hierarchy
python3 rws.py subjects --hierarchy

# List subjects grouped by code
python3 rws.py subjects --by-code

# Filter by theme
python3 rws.py subjects --theme "Kaderrichtlijn Water"

# Filter by name pattern
python3 rws.py subjects --name "*geometrie*"

# Filter by area level type
python3 rws.py subjects --areaLevel NSPW

# Filter by area names (shows subjects that have matching areas)
python3 rws.py subjects --areaName "*Rijnland*"
```

### Download Data
```bash
# Download a specific subject
python3 rws.py dl 1

# Download multiple subjects
python3 rws.py dl 1 5 15

# Download subjects matching pattern (all starting with 1)
python3 rws.py dl "1*"

# Download with filters
python3 rws.py dl --years 2020-2023 --theme "Kaderrichtlijn Water" "1*"

# Download for specific areas only
python3 rws.py dl --levels waterbeheerder --names "*Rijnland*" 23
```

### Testing & Development
```bash
# Dry run to see what would be downloaded
python3 rws.py dl --dry-run --limit 5 15

# Limit number of requests for testing
python3 rws.py dl --limit 10 "5*"

# Verbose output with detailed hierarchy
python3 rws.py subjects --hierarchy --verbose
```

## Data Structure

### Themes
1. **Kaderrichtlijn Water** (Water Framework Directive) - 21 subjects
2. **Ecologie** (Ecology) - 5 subjects  
3. **Oppervlaktewaterkwaliteit** (Surface Water Quality) - 1 subject
4. **Grondwaterkwaliteit** (Groundwater Quality) - 1 subject

### Area Level Types
- **nederland_only**: National level data only
- **waterbeheerder_only**: Water authority level only
- **NSPW**: Nederland + Stroomgebieddistricten + Provincie + Waterbeheerder
- **NW**: Nederland + Waterbeheerder
- **NSW**: Nederland + Stroomgebieddistricten + Waterbeheerder
- **PW**: Provincie + Waterbeheerder

### Subject Codes
Subjects are organized with descriptive codes like:
- **KW**: Kaderrichtlijn Water (Water Framework Directive)
- **EC**: Ecologie (Ecology)
- **OK**: Oppervlaktewaterkwaliteit (Surface Water Quality)
- **GK**: Grondwaterkwaliteit (Groundwater Quality)

## Subject Examples

### High-Volume Subjects (many requests)
- Subject 15 (OKME): Surface water measurements, 16 years × 22 water authorities = 352 requests
- Subject 16 (GKME): Groundwater measurements, 95 years × 22 water authorities = 2,090 requests
- Subject 6 (KWBI): KRW impacts, 3 years × (1 + 4 + 12 + 22) areas = 117 requests

### Simple Subjects (few requests)
- Subject 1 (KWSG): Stream basin geometry, 1 year × 1 area = 1 request
- Subject 2 (KWOG): Surface water body geometry, 1 year × 1 area = 1 request

## Rate Limiting

The script includes automatic rate limiting (1 second delay between requests) to be respectful of the API.

## Output

Downloaded files are saved as ZIP files with descriptive names:
```
subject_{id}_{name}_year_{year}_{areaLevel}_{areaName}.zip
```

Example:
```
subject_15_Meetgegevens_year_2023_waterbeheerder_Hoogheemraadschap_van_Rijnland.zip
```