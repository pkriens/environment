"""
Query builder and utilities for common environmental data queries.
"""

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, or_, between, desc, asc, distinct
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd

from .models import Authority, Region, Station, Sample, Parameter, Classifier, Assessment


class QueryBuilder:
    """Builder class for common environmental monitoring queries."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_stations_in_region(self, region_id: str, station_type: Optional[str] = None) -> List[Station]:
        """
        Get all monitoring stations in a specific region.
        
        Args:
            region_id: Region identifier
            station_type: Optional filter by station type
            
        Returns:
            List of Station objects
        """
        query = (self.session.query(Station)
                .options(joinedload(Station.region))
                .filter(Station.region_id == region_id))
        
        if station_type:
            query = query.filter(Station.station_type == station_type)
            
        return query.all()
    
    def get_samples_by_station_date_range(self, 
                                        station_id: str, 
                                        start_date: date, 
                                        end_date: date,
                                        parameter_ids: Optional[List[str]] = None) -> List[Sample]:
        """
        Get samples for a station within a date range.
        
        Args:
            station_id: Station identifier
            start_date: Start date for samples
            end_date: End date for samples
            parameter_ids: Optional list of parameter IDs to filter
            
        Returns:
            List of Sample objects
        """
        query = (self.session.query(Sample)
                .options(joinedload(Sample.parameter))
                .filter(and_(
                    Sample.station_id == station_id,
                    between(Sample.timestamp, start_date, end_date)
                )))
        
        if parameter_ids:
            query = query.filter(Sample.parameter_id.in_(parameter_ids))
            
        return query.order_by(Sample.timestamp, Sample.parameter_id).all()
    
    def get_water_quality_time_series(self, 
                                    station_id: str,
                                    parameter_id: str,
                                    start_date: date,
                                    end_date: date) -> pd.DataFrame:
        """
        Get time series data for water quality analysis.
        
        Args:
            station_id: Station identifier
            parameter_id: Parameter identifier  
            start_date: Start date
            end_date: End date
            
        Returns:
            Pandas DataFrame with time series data
        """
        query = (self.session.query(
                    Sample.timestamp,
                    Sample.value_numeric,
                    Sample.quality_code,
                    Parameter.name.label('parameter_name'),
                    Parameter.unit_code
                )
                .join(Parameter)
                .filter(and_(
                    Sample.station_id == station_id,
                    Sample.parameter_id == parameter_id,
                    between(Sample.timestamp, start_date, end_date),
                    Sample.quality_code == 'OK'  # Only validated data
                )))
        
        df = pd.read_sql(query.statement, self.session.bind)
        df['datetime'] = pd.to_datetime(df['timestamp'])
        
        return df.sort_values('datetime')
    
    def get_regional_assessment_summary(self, 
                                      region_id: str, 
                                      assessment_year: int,
                                      assessment_type: str = 'WFD_ecological') -> Dict[str, Any]:
        """
        Get summary of environmental assessments for a region.
        
        Args:
            region_id: Region identifier
            assessment_year: Year of assessment
            assessment_type: Type of assessment
            
        Returns:
            Dictionary with assessment summary
        """
        # Station-level assessments in region
        station_assessments = (self.session.query(Assessment)
                             .join(Station)
                             .filter(and_(
                                 Station.region_id == region_id,
                                 Assessment.assessment_year == assessment_year,
                                 Assessment.assessment_type == assessment_type
                             ))).all()
        
        # Regional-level assessment
        regional_assessment = (self.session.query(Assessment)
                             .filter(and_(
                                 Assessment.region_id == region_id,
                                 Assessment.assessment_year == assessment_year,
                                 Assessment.assessment_type == assessment_type
                             ))).first()
        
        # Calculate statistics
        status_counts = {}
        for assessment in station_assessments:
            status = assessment.overall_status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'region_id': region_id,
            'assessment_year': assessment_year,
            'assessment_type': assessment_type,
            'regional_status': regional_assessment.overall_status if regional_assessment else None,
            'total_stations': len(station_assessments),
            'status_distribution': status_counts,
            'station_assessments': station_assessments,
            'regional_assessment': regional_assessment
        }
    
    def get_natura2000_conservation_status(self, site_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get conservation status for Natura 2000 sites.
        
        Args:
            site_code: Optional specific site code
            
        Returns:
            List of conservation status summaries
        """
        query = (self.session.query(Region, Assessment)
                .outerjoin(Assessment, Region.id == Assessment.region_id)
                .filter(Region.region_type == 'natura2000'))
        
        if site_code:
            query = query.filter(Region.designation_code == site_code)
        
        results = []
        for region, assessment in query.all():
            results.append({
                'site_code': region.designation_code,
                'site_name': region.name,
                'area_ha': region.area_ha,
                'conservation_status': assessment.overall_status if assessment else 'Not assessed',
                'last_assessment_year': assessment.assessment_year if assessment else None,
                'trend': assessment.status_trend if assessment else None
            })
        
        return results
    
    def get_parameter_statistics(self, 
                               parameter_id: str,
                               region_id: Optional[str] = None,
                               year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get statistical summary for a parameter.
        
        Args:
            parameter_id: Parameter identifier
            region_id: Optional region filter
            year: Optional year filter
            
        Returns:
            Dictionary with parameter statistics
        """
        # SQLite doesn't have stddev function, so we'll calculate it manually or omit it
        query = (self.session.query(
                    func.count(Sample.value_numeric).label('count'),
                    func.avg(Sample.value_numeric).label('mean'),
                    func.min(Sample.value_numeric).label('minimum'),
                    func.max(Sample.value_numeric).label('maximum')
                )
                .filter(and_(
                    Sample.parameter_id == parameter_id,
                    Sample.quality_code == 'OK',
                    Sample.value_numeric.isnot(None)
                )))
        
        if region_id:
            query = query.join(Station).filter(Station.region_id == region_id)
            
        if year:
            query = query.filter(func.extract('year', Sample.timestamp) == year)
        
        result = query.first()
        
        # Get parameter info
        parameter = self.session.query(Parameter).filter(Parameter.id == parameter_id).first()
        
        return {
            'parameter_id': parameter_id,
            'parameter_name': parameter.name if parameter else 'Unknown',
            'unit': parameter.unit_code if parameter else None,
            'count': result.count or 0,
            'mean': float(result.mean) if result.mean else None,
            'minimum': float(result.minimum) if result.minimum else None,
            'maximum': float(result.maximum) if result.maximum else None,
            'region_id': region_id,
            'year': year
        }
    
    def classify_samples(self, 
                        station_id: str, 
                        parameter_id: str,
                        start_date: date,
                        end_date: date) -> List[Dict[str, Any]]:
        """
        Classify sample values according to environmental standards.
        
        Args:
            station_id: Station identifier
            parameter_id: Parameter identifier
            start_date: Start date
            end_date: End date
            
        Returns:
            List of classified sample results
        """
        # Get samples
        samples = (self.session.query(Sample)
                  .filter(and_(
                      Sample.station_id == station_id,
                      Sample.parameter_id == parameter_id,
                      between(Sample.timestamp, start_date, end_date),
                      Sample.quality_code == 'OK'
                  ))).all()
        
        # Get applicable classifier
        station = self.session.query(Station).filter(Station.id == station_id).first()
        
        classifier = (self.session.query(Classifier)
                     .filter(and_(
                         Classifier.parameter_id == parameter_id,
                         Classifier.environment_classification == station.classification_code
                     ))).first()
        
        results = []
        for sample in samples:
            classification = 'Unknown'
            if classifier and sample.value_numeric is not None:
                classification = classifier.classify_value(sample.value_numeric)
            
            results.append({
                'timestamp': sample.timestamp,
                'value': sample.value_numeric,
                'classification': classification,
                'classifier_id': classifier.id if classifier else None
            })
        
        return results
    
    def get_assessment_trends(self, 
                            region_id: str,
                            assessment_type: str,
                            years: int = 10) -> Dict[str, Any]:
        """
        Get assessment trends over time for a region.
        
        Args:
            region_id: Region identifier
            assessment_type: Type of assessment
            years: Number of years to look back
            
        Returns:
            Dictionary with trend analysis
        """
        end_year = datetime.now().year
        start_year = end_year - years
        
        assessments = (self.session.query(Assessment)
                      .filter(and_(
                          Assessment.region_id == region_id,
                          Assessment.assessment_type == assessment_type,
                          between(Assessment.assessment_year, start_year, end_year)
                      ))
                      .order_by(Assessment.assessment_year)).all()
        
        trend_data = []
        for assessment in assessments:
            trend_data.append({
                'year': assessment.assessment_year,
                'status': assessment.overall_status,
                'trend': assessment.status_trend
            })
        
        return {
            'region_id': region_id,
            'assessment_type': assessment_type,
            'period': f"{start_year}-{end_year}",
            'data_points': len(trend_data),
            'trend_data': trend_data
        }