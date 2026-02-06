# Environmental Database (EnvDB)

A normalized, storage-optimized Python database model for environmental monitoring data. Supports water quality, air quality, soil monitoring, biodiversity assessments, and Natura 2000 conservation areas.

## Features

- **Fully normalized schema** optimized for large-scale environmental datasets
- **Domain-neutral design** supports water, air, soil, and biodiversity monitoring  
- **Storage-optimized** sample table designed for billions of measurements
- **Short primary keys** (3-5 characters) for efficient storage and indexing
- **Built-in classification systems** for environmental standards (WFD, Natura 2000, etc.)
- **Temporal optimization** with time-series partitioning support
- **Rich query interface** for common environmental analysis patterns

## Quick Start

```python
import envdb

# Initialize database
db = envdb.Database("sqlite:///monitoring.db")
db.create_all_tables()

with db.get_session() as session:
    # Create authority
    authority = envdb.Authority(
        auth_id="WS001",
        name="Regional Water Board", 
        authority_type="waterschap"
    )
    session.add(authority)
    
    # Create region
    region = envdb.Region(
        reg_id="N2001",
        auth_id="WS001",
        name="Protected Wetland Area",
        region_type="natura2000" 
    )
    session.add(region)
    
    # Create monitoring station
    station = envdb.Station(
        stn_id="WQ001",
        reg_id="N2001", 
        auth_id="WS001",
        name="River monitoring point",
        station_type="water_quality",
        x_coord=142500.0,
        y_coord=465000.0
    )
    session.add(station)
    
    session.commit()
```

## Core Models

### Authority
Environmental monitoring authorities (water boards, provinces, municipalities):
- `auth_id` (5 chars): Primary key
- `name`: Authority name
- `authority_type`: Type of organization
- `jurisdiction_area`: Geographic scope

### Region  
Environmental management regions (Natura 2000, watersheds, administrative areas):
- `reg_id` (5 chars): Primary key
- `region_type`: 'natura2000', 'watershed', 'administrative', 'krw_waterbody'
- `designation_code`: Official designation (e.g., Natura 2000 site code)
- `protection_level`: Conservation status

### Station
Monitoring stations/locations:
- `stn_id` (5 chars): Primary key  
- `station_type`: 'water_quality', 'air_quality', 'biodiversity', 'soil'
- `environment_type`: Specific environment classification
- `classification_code`: Environmental classification (e.g., KRW water type, habitat type)

### Sample (Storage Optimized)
Individual measurements - designed for billions of records:
- Composite primary key: `(smp_id, stn_id, par_id, sample_date)`
- `value_numeric`/`value_text`: Measurement values  
- `quality_code`: Data quality indicator
- Minimal nullable fields for space efficiency

### Parameter
Environmental monitoring parameters:
- `par_id` (3-5 chars): Primary key
- `parameter_group`: 'chemical', 'biological', 'physical', 'habitat'
- `measurement_type`: 'concentration', 'count', 'presence', 'index'

### Classifier
Environmental classification systems (standards, thresholds):
- `cls_id` (5 chars): Primary key
- `classification_system`: 'WFD', 'Natura2000', 'RED_LIST'
- Threshold values for classification levels
- Built-in `classify_value()` method

### Assessment  
Environmental status assessments:
- `ass_id` (5 chars): Primary key
- `assessment_type`: 'WFD_ecological', 'conservation_status', 'red_list'
- `overall_status`: Final assessment result
- Supports one-out-all-out principle for WFD assessments

## Query Interface

```python
query_builder = envdb.QueryBuilder(session)

# Get stations in a region
stations = query_builder.get_stations_in_region("N2001", station_type="water_quality")

# Time series data as pandas DataFrame  
df = query_builder.get_water_quality_time_series(
    station_id="WQ001", 
    parameter_id="NO3N",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31)
)

# Natura 2000 conservation status
conservation = query_builder.get_natura2000_conservation_status()

# Classify samples according to environmental standards
classifications = query_builder.classify_samples(
    station_id="WQ001",
    parameter_id="NO3N", 
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31)
)

# Regional assessment summary
summary = query_builder.get_regional_assessment_summary(
    region_id="N2001", 
    assessment_year=2024
)
```

## Database Optimization

### Storage Optimization
- Composite primary keys reduce index overhead
- Minimal nullable columns in Sample table
- Short primary key IDs (3-5 chars) save significant space
- Efficient data types chosen for each field

### Performance Features  
- Time-series optimized indexes
- Bulk operation support
- PostgreSQL partitioning support for large datasets
- SQLite WAL mode for better concurrency

### Scaling Support
```python
# Optimize for bulk data loading
db.optimize_for_bulk_operations()

# Load millions of samples...

# Restore normal operations
db.restore_normal_operations()

# Create time-based partitions (PostgreSQL)
db.create_partitions('samples', partition_type='monthly')
```

## Installation

```bash
pip install sqlalchemy pandas
```

## Example Usage

See `envdb/example.py` for a complete working example with sample data creation and various query demonstrations.

## Schema Design Principles

1. **Domain Neutrality**: Schema works for water, air, soil, and biodiversity monitoring
2. **Storage Efficiency**: Optimized for datasets with billions of measurements  
3. **Normalization**: Fully normalized to eliminate data redundancy
4. **Flexibility**: Supports multiple classification systems and assessment types
5. **Performance**: Designed for fast time-series queries and bulk operations
6. **Standards Compliance**: Supports WFD, Natura 2000, and other environmental frameworks

This database schema addresses the constitutional crisis identified in the Dutch water quality system by providing a transparent, auditable framework where environmental standards (Classifier table) can be properly linked to their legal authorization (Authority table), ensuring democratic legitimacy in environmental norm-setting.