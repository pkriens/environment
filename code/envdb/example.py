"""
Example usage of the EnvDB environmental monitoring database.
"""

from datetime import date, datetime, timedelta
import envdb

def example_usage():
    """Demonstrate basic EnvDB usage."""
    
    # Initialize database
    db = envdb.Database("sqlite:///environmental_monitoring.db")
    db.create_all_tables(drop_existing=True)
    
    print("EnvDB Environmental Monitoring Database")
    print("=====================================")
    
    with db.get_session() as session:
        # Create sample data
        create_sample_data(session)
        
        # Demonstrate queries
        query_builder = envdb.QueryBuilder(session)
        demonstrate_queries(query_builder)


def create_sample_data(session):
    """Create sample data for demonstration."""
    
    # Create water authority using enum
    waterschap = envdb.Authority.from_enum(
        envdb.AuthorityEnum.HDSR,
        description="Water authority for Utrecht region"
    )
    session.add(waterschap)
    
    # Create provincia authority using enum
    provincie = envdb.Authority.from_enum(
        envdb.AuthorityEnum.UTRECHT,
        description="Province of Utrecht"
    )
    session.add(provincie)
    
    # Create regions
    natura2000_area = envdb.Region(
        id="N2001",
        authority_id="PR010",  # Utrecht province code
        name="Nieuwkoopse Plassen",
        region_type="natura2000",
        designation_code="NL2003061",
        area_ha=1234.5,
        protection_level="natura2000",
        description="Natura 2000 wetland area"
    )
    session.add(natura2000_area)
    
    water_region = envdb.Region(
        id="WR001",
        authority_id="WS001",  # HDSR code
        name="Vecht catchment",
        region_type="watershed", 
        area_ha=15000.0,
        description="River Vecht watershed area"
    )
    session.add(water_region)
    
    # Create parameters
    nitrogen = envdb.Parameter(
        id="NO3N",
        name="Nitrate as nitrogen",
        parameter_group="chemical",
        measurement_type="concentration", 
        unit_code="mg/L",
        unit_description="milligrams per liter",
        cas_number="14797-55-8",
        value_data_type="numeric",
        min_value=0.0,
        max_value=100.0,
        decimal_places=2
    )
    session.add(nitrogen)
    
    macrofauna_count = envdb.Parameter(
        id="MFCNT",
        name="Macrofauna count",
        parameter_group="biological",
        measurement_type="count",
        unit_code="n/m2", 
        unit_description="number per square meter",
        value_data_type="numeric",
        min_value=0.0,
        max_value=10000.0,
        decimal_places=0
    )
    session.add(macrofauna_count)
    
    # Create monitoring stations
    water_station = envdb.Station(
        id="WQ001",
        region_id="WR001",
        authority_id="WS001",
        name="Vecht at Breukelen",
        station_type="water_quality",
        environment_type="surface_water",
        classification_code="M1a",
        classification_desc="Zoete gebufferde sloten",
        x_coord=142500.0,
        y_coord=465000.0,
        coord_system="RD",
        operational_from=date(2020, 1, 1),
        sampling_frequency="monthly"
    )
    session.add(water_station)
    
    bio_station = envdb.Station(
        id="BIO01", 
        region_id="N2001",
        authority_id="PR010",  # Utrecht province code
        name="Nieuwkoopse Plassen monitoring point",
        station_type="biodiversity",
        environment_type="wetland",
        classification_code="H4010",
        classification_desc="Northern Atlantic wet heaths",
        x_coord=140000.0,
        y_coord=470000.0,
        coord_system="RD",
        operational_from=date(2019, 1, 1),
        sampling_frequency="quarterly"
    )
    session.add(bio_station)
    
    # Create classifier (water quality standard)
    wfd_nitrogen_classifier = envdb.Classifier(
        id="WFD01",
        parameter_id="NO3N",
        name="WFD Nitrogen standard for M1a waters",
        classification_system="WFD",
        environment_classification="M1a",
        threshold_excellent=1.0,
        threshold_good=2.3,
        threshold_moderate=4.5,
        threshold_poor=9.0,
        threshold_bad=999.0,
        label_excellent="Zeer goed",
        label_good="Goed", 
        label_moderate="Matig",
        label_poor="Ontoereikend",
        label_bad="Slecht",
        unit="mg/L",
        source_document="STOWA 2018-49",
        effective_from=date(2021, 1, 1),
        authority_approved="WS001"
    )
    session.add(wfd_nitrogen_classifier)
    
    session.commit()
    
    # Create sample measurements
    base_date = date(2024, 1, 1)
    for i in range(12):  # 12 months of data
        sample_datetime = datetime.combine(base_date.replace(month=i+1), datetime.min.time().replace(hour=10))
        
        # Nitrogen measurement
        nitrogen_sample = envdb.Sample(
            station_id="WQ001",
            parameter_id="NO3N", 
            timestamp=sample_datetime,
            value_numeric=2.1 + (i * 0.2),  # Gradually increasing
            quality_code="OK"
        )
        session.add(nitrogen_sample)
        
        # Macrofauna count
        if i % 3 == 0:  # Quarterly sampling
            macro_sample = envdb.Sample(
                station_id="BIO01", 
                parameter_id="MFCNT",
                timestamp=sample_datetime,
                value_numeric=150 - (i * 5),  # Gradually decreasing
                quality_code="OK"
            )
            session.add(macro_sample)
    
    session.commit()
    
    # Create assessments
    water_assessment = envdb.Assessment(
        id="A2024",
        station_id="WQ001",
        assessment_year=2024,
        assessment_type="WFD_ecological",
        classification_system="WFD",
        overall_status="Moderate",
        status_trend="declining",
        confidence_level="high",
        biological_status="Moderate",
        chemical_status="Good",
        limiting_factor="Elevated nitrogen levels",
        assessed_by="WS001",
        assessment_date=date(2024, 12, 1)
    )
    session.add(water_assessment)
    
    natura_assessment = envdb.Assessment(
        id="N2024",
        region_id="N2001", 
        assessment_year=2024,
        assessment_type="conservation_status",
        classification_system="Natura2000",
        overall_status="Good",
        status_trend="stable",
        confidence_level="moderate",
        biological_status="Good",
        assessed_by="PR010",  # Utrecht province code
        assessment_date=date(2024, 11, 15)
    )
    session.add(natura_assessment)
    
    session.commit()
    print("✓ Sample data created successfully")


def demonstrate_queries(query_builder):
    """Demonstrate various query capabilities."""
    
    print("\n1. Stations in water region:")
    stations = query_builder.get_stations_in_region("WR001")
    for station in stations:
        print(f"  - {station.name} ({station.id}): {station.station_type}")
    
    print("\n2. Water quality time series:")
    df = query_builder.get_water_quality_time_series(
        "WQ001", "NO3N", date(2024, 1, 1), date(2024, 12, 31)
    )
    print(f"  Retrieved {len(df)} measurements")
    if len(df) > 0:
        print(f"  Average value: {df['value_numeric'].mean():.2f} {df['unit_code'].iloc[0]}")
        print(f"  Range: {df['value_numeric'].min():.2f} - {df['value_numeric'].max():.2f}")
    
    print("\n3. Natura 2000 conservation status:")
    conservation_status = query_builder.get_natura2000_conservation_status()
    for site in conservation_status:
        print(f"  - {site['site_name']}: {site['conservation_status']}")
        print(f"    Area: {site['area_ha']} ha, Last assessed: {site['last_assessment_year']}")
    
    print("\n4. Parameter statistics (Nitrogen in 2024):")
    stats = query_builder.get_parameter_statistics("NO3N", year=2024)
    print(f"  - {stats['parameter_name']}")
    print(f"    Count: {stats['count']} measurements")
    print(f"    Mean: {stats['mean']:.2f} {stats['unit']}")
    print(f"    Range: {stats['minimum']:.2f} - {stats['maximum']:.2f}")
    
    print("\n5. Classification of nitrogen samples:")
    classifications = query_builder.classify_samples(
        "WQ001", "NO3N", date(2024, 1, 1), date(2024, 12, 31)
    )
    
    # Count classifications
    class_counts = {}
    for result in classifications:
        cls = result['classification']
        class_counts[cls] = class_counts.get(cls, 0) + 1
    
    print("  Classification distribution:")
    for classification, count in class_counts.items():
        print(f"    - {classification}: {count} samples")
    
    print("\n6. Regional assessment summary:")
    summary = query_builder.get_regional_assessment_summary("WR001", 2024)
    print(f"  - Region: {summary['region_id']}")
    print(f"  - Total stations assessed: {summary['total_stations']}")
    print(f"  - Status distribution: {summary['status_distribution']}")


if __name__ == "__main__":
    example_usage()