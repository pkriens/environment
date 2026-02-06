"""
Core data models for environmental monitoring database.
Fully normalized, storage-optimized design for large-scale environmental data.
"""

from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Date, Time, Text, Boolean,
    ForeignKey, Index, CheckConstraint, UniqueConstraint, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, date
from typing import Optional

Base = declarative_base()


class Authority(Base):
    """Environmental monitoring authority (water boards, provinces, municipalities, etc.)"""
    __tablename__ = 'authorities'
    
    id = Column(String(5), primary_key=True)  # e.g., 'WS014', 'PROV1'
    name = Column(String(100), nullable=False)
    description = Column(String(400), nullable=True)
    authority_type = Column(String(20), nullable=False)  # Store enum name: 'WATERSCHAP', 'PROVINCIE', etc.
    
    # Relationships
    regions = relationship("Region", back_populates="authority")
    stations = relationship("Station", back_populates="authority")
    
    @property
    def authority_type_info(self) -> AuthorityType:
        """Get the AuthorityType enum for this authority"""
        return AuthorityType[self.authority_type]
    
    @classmethod
    def from_enum(cls, authority_enum: AuthorityEnum, description: str = None):
        """Create Authority instance from enum"""
        return cls(
            id=authority_enum.code,
            name=authority_enum.full_name,
            description=description,
            authority_type=authority_enum.authority_type.name
        )
    
    def __repr__(self):
        return f"<Authority(id='{self.id}', name='{self.name}', type='{self.authority_type}')>"


class Region(Base):
    """Environmental management regions (Natura 2000, watersheds, administrative areas)"""
    __tablename__ = 'regions'
    
    id = Column(String(5), primary_key=True)  # e.g., 'N2001', 'CATCH'
    authority_id = Column(String(5), ForeignKey('authorities.id'), nullable=False)
    name = Column(String(100), nullable=False)
    region_type = Column(String(30), nullable=False)  # 'natura2000', 'watershed', 'administrative', 'krw_waterbody'
    designation_code = Column(String(20))  # Official designation (e.g., Natura 2000 site code)
    area_ha = Column(Float)
    protection_level = Column(String(20))  # 'national_park', 'natura2000', 'ramsar', 'normal'
    geometry_wkt = Column(Text)  # Well-Known Text geometry
    description = Column(Text)
    established_date = Column(Date)
    
    # Relationships  
    authority = relationship("Authority", back_populates="regions")
    stations = relationship("Station", back_populates="region")
    assessments = relationship("Assessment", back_populates="region")
    
    __table_args__ = (
        Index('idx_region_type', 'region_type'),
        Index('idx_region_authority', 'authority_id'),
    )
    
    def __repr__(self):
        return f"<Region(id='{self.id}', name='{self.name}', type='{self.region_type}')>"


class Parameter(Base):
    """Environmental monitoring parameters (nutrients, species, physical properties)"""
    __tablename__ = 'parameters'
    
    id = Column(String(5), primary_key=True)  # e.g., 'NO3N', 'TEMP', 'SP001' 
    name = Column(String(100), nullable=False)
    parameter_group = Column(String(30), nullable=False)  # 'chemical', 'biological', 'physical', 'habitat'
    measurement_type = Column(String(20), nullable=False)  # 'concentration', 'count', 'presence', 'index'
    unit_code = Column(String(10))  # 'mg/L', 'n/m2', '°C', 'pH'
    unit_description = Column(String(50))
    cas_number = Column(String(20))  # Chemical Abstract Service number
    scientific_name = Column(String(100))  # For biological parameters
    common_name = Column(String(100))
    detection_limit = Column(Float)
    quantification_limit = Column(Float)
    
    # Data type specifications for values
    value_data_type = Column(String(20), nullable=False, default='numeric')  # 'numeric', 'text', 'boolean', 'categorical'
    allowed_values = Column(Text)  # JSON array for categorical values
    min_value = Column(Float)  # For numeric validation
    max_value = Column(Float)  # For numeric validation
    decimal_places = Column(Integer)  # Precision for numeric values
    
    description = Column(Text)
    
    # Relationships
    samples = relationship("Sample", back_populates="parameter")
    classifiers = relationship("Classifier", back_populates="parameter")
    
    __table_args__ = (
        Index('idx_param_group', 'parameter_group'),
        Index('idx_param_type', 'measurement_type'),
    )
    
    def __repr__(self):
        return f"<Parameter(id='{self.id}', name='{self.name}')>"


class Station(Base):
    """Monitoring stations/locations (meetobjecten)"""
    __tablename__ = 'stations'
    
    id = Column(String(5), primary_key=True)  # e.g., 'S1001', 'WQ045'
    region_id = Column(String(5), ForeignKey('regions.id'), nullable=False)
    authority_id = Column(String(5), ForeignKey('authorities.id'), nullable=False)
    name = Column(String(100), nullable=False)
    station_type = Column(String(30), nullable=False)  # 'water_quality', 'air_quality', 'biodiversity', 'soil'
    environment_type = Column(String(30))  # 'surface_water', 'groundwater', 'ambient_air', 'forest', 'wetland'
    classification_code = Column(String(10))  # KRW water type, habitat type, etc.
    classification_desc = Column(String(100))
    
    # Geographic
    x_coord = Column(Float, nullable=False)  # RD or WGS84
    y_coord = Column(Float, nullable=False)
    coord_system = Column(String(10), nullable=False, default='RD')  # 'RD', 'WGS84'
    altitude_m = Column(Float)
    
    # Operational
    operational_from = Column(Date, nullable=False)
    operational_until = Column(Date)
    sampling_frequency = Column(String(20))  # 'weekly', 'monthly', 'quarterly', 'event_driven'
    access_restrictions = Column(String(100))
    
    # Relationships
    region = relationship("Region", back_populates="stations")
    authority = relationship("Authority", back_populates="stations") 
    samples = relationship("Sample", back_populates="station")
    assessments = relationship("Assessment", back_populates="station")
    
    __table_args__ = (
        Index('idx_station_region', 'region_id'),
        Index('idx_station_type', 'station_type'),
        Index('idx_station_classification', 'classification_code'),
        Index('idx_station_coords', 'x_coord', 'y_coord'),
    )
    
    def __repr__(self):
        return f"<Station(id='{self.id}', name='{self.name}')>"


class Sample(Base):
    """Individual sample measurements - ULTRA STORAGE OPTIMIZED for billions of records"""
    __tablename__ = 'samples'
    
    # Minimal composite primary key: station + timestamp + parameter
    station_id = Column(String(5), ForeignKey('stations.id'), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)  # Combined date+time
    parameter_id = Column(String(5), ForeignKey('parameters.id'), primary_key=True)
    
    # Value storage - type determined by Parameter.value_data_type
    value_numeric = Column(Float)  # For numeric parameters
    value_text = Column(String(100))  # For text/categorical parameters
    value_boolean = Column(Boolean)  # For boolean parameters
    
    # Minimal quality control
    quality_code = Column(String(2), nullable=False, default='OK')  # 'OK', 'ER', 'ES'
    
    # Relationships
    station = relationship("Station", back_populates="samples")
    parameter = relationship("Parameter", back_populates="samples")
    
    __table_args__ = (
        # Optimized indexes for time-series queries
        Index('idx_sample_station_time', 'station_id', 'timestamp'),
        Index('idx_sample_param_time', 'parameter_id', 'timestamp'),
        Index('idx_sample_time_only', 'timestamp'),
        
        # Constraints based on parameter data type
        CheckConstraint('value_numeric IS NOT NULL OR value_text IS NOT NULL OR value_boolean IS NOT NULL', 
                       name='check_value_present'),
        CheckConstraint("quality_code IN ('OK', 'ER', 'ES')", 
                       name='check_quality_code'),
    )
    
    def get_typed_value(self):
        """Return the value in the correct type based on parameter specification"""
        if self.parameter.value_data_type == 'numeric':
            return self.value_numeric
        elif self.parameter.value_data_type == 'boolean':
            return self.value_boolean
        else:
            return self.value_text
    
    def set_typed_value(self, value):
        """Set the value in the correct column based on parameter specification"""
        # Clear all value columns first
        self.value_numeric = None
        self.value_text = None
        self.value_boolean = None
        
        if self.parameter.value_data_type == 'numeric':
            self.value_numeric = float(value) if value is not None else None
        elif self.parameter.value_data_type == 'boolean':
            self.value_boolean = bool(value) if value is not None else None
        else:
            self.value_text = str(value) if value is not None else None
    
    def __repr__(self):
        return f"<Sample(station_id='{self.station_id}', parameter_id='{self.parameter_id}', timestamp='{self.timestamp}')>"


class Classifier(Base):
    """Environmental classification systems (maatlatten, assessment criteria)"""
    __tablename__ = 'classifiers'
    
    id = Column(String(5), primary_key=True)  # e.g., 'WFD01', 'HAB01'
    parameter_id = Column(String(5), ForeignKey('parameters.id'), nullable=False)
    name = Column(String(100), nullable=False)
    classification_system = Column(String(30), nullable=False)  # 'WFD', 'Natura2000', 'RED_LIST'
    environment_classification = Column(String(20))  # 'M1a', 'H4010', etc.
    
    # Threshold values for classification
    threshold_excellent = Column(Float)
    threshold_good = Column(Float)  
    threshold_moderate = Column(Float)
    threshold_poor = Column(Float)
    threshold_bad = Column(Float)
    
    # Classification labels
    label_excellent = Column(String(30), default='Excellent')
    label_good = Column(String(30), default='Good')
    label_moderate = Column(String(30), default='Moderate') 
    label_poor = Column(String(30), default='Poor')
    label_bad = Column(String(30), default='Bad')
    
    unit = Column(String(20))
    calculation_method = Column(Text)
    source_document = Column(String(100))  # e.g., 'STOWA 2018-49'
    version = Column(String(10))
    effective_from = Column(Date, nullable=False)
    effective_until = Column(Date)
    authority_approved = Column(String(5))  # Which authority approved this classifier
    
    # Relationships
    parameter = relationship("Parameter", back_populates="classifiers")
    
    __table_args__ = (
        Index('idx_classifier_param', 'parameter_id'),
        Index('idx_classifier_system', 'classification_system'),
        Index('idx_classifier_environment', 'environment_classification'),
        Index('idx_classifier_effective', 'effective_from', 'effective_until'),
    )
    
    def classify_value(self, value: float) -> str:
        """Classify a numeric value according to this classifier's thresholds"""
        if value is None:
            return 'Unknown'
        
        thresholds = [
            (self.threshold_excellent, self.label_excellent),
            (self.threshold_good, self.label_good),
            (self.threshold_moderate, self.label_moderate), 
            (self.threshold_poor, self.label_poor),
            (self.threshold_bad, self.label_bad)
        ]
        
        for threshold, label in thresholds:
            if threshold is not None and value >= threshold:
                return label
                
        return self.label_bad or 'Bad'
    
    def __repr__(self):
        return f"<Classifier(id='{self.id}', name='{self.name}')>"


class Assessment(Base):
    """Environmental status assessments per station/region and time period"""
    __tablename__ = 'assessments'
    
    id = Column(String(5), primary_key=True)  # e.g., 'A2024'
    station_id = Column(String(5), ForeignKey('stations.id'))
    region_id = Column(String(5), ForeignKey('regions.id'))
    sample_id = Column(String(5))  # Reference to associated sample (optional)
    assessment_year = Column(Integer, nullable=False)
    assessment_period_start = Column(Date)
    assessment_period_end = Column(Date)
    
    assessment_type = Column(String(30), nullable=False)  # 'WFD_ecological', 'conservation_status', 'red_list'
    classification_system = Column(String(20), nullable=False)  # 'WFD', 'Natura2000'
    
    # Overall status
    overall_status = Column(String(20))  # 'High', 'Good', 'Moderate', 'Poor', 'Bad'
    status_trend = Column(String(15))  # 'improving', 'stable', 'declining'
    confidence_level = Column(String(15))  # 'high', 'moderate', 'low'
    
    # Assessment components
    biological_status = Column(String(20))
    chemical_status = Column(String(20))
    physical_status = Column(String(20))
    pressure_assessment = Column(Text)  # JSON or structured text
    
    # One-out-all-out principle result (for WFD)
    limiting_factor = Column(String(50))  # What caused the assessment result
    improvement_needed = Column(Text)
    
    # Administrative
    assessed_by = Column(String(5))  # Authority/organization code
    assessment_date = Column(Date, nullable=False, default=date.today)
    validated_by = Column(String(5))
    validation_date = Column(Date)
    
    # Relationships
    station = relationship("Station", back_populates="assessments")
    region = relationship("Region", back_populates="assessments")
    
    __table_args__ = (
        Index('idx_assessment_station_year', 'station_id', 'assessment_year'),
        Index('idx_assessment_region_year', 'region_id', 'assessment_year'),
        Index('idx_assessment_type', 'assessment_type'),
        Index('idx_assessment_status', 'overall_status'),
        
        # Ensure either station or region is specified
        CheckConstraint('station_id IS NOT NULL OR region_id IS NOT NULL', 
                       name='check_assessment_target'),
        CheckConstraint("overall_status IN ('Excellent', 'Good', 'Moderate', 'Poor', 'Bad')", 
                       name='check_overall_status'),
    )
    
    def __repr__(self):
        target = self.station_id or self.region_id
        return f"<Assessment(id='{self.id}', target='{target}', year={self.assessment_year})>"