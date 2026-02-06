#!/usr/bin/env python3
"""
RWS Import Tool - Data-driven CSV Importer for Dutch Water Quality Data

Usage:
    imp_rws.py waardes [CSV_FILES...] --db DATABASE [--dry-run] [--drop]
    imp_rws.py objects [CSV_FILES...] --db DATABASE [--dry-run] [--drop]
    imp_rws.py aquo [CSV_FILES...] --db DATABASE [--dry-run] [--drop]
"""

import argparse
import sqlite3
import csv
import re
import sys
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class ColumnDef:
    """Definition of a database column with CSV recognition patterns."""
    db_name: str           # Database column name
    csv_pattern: str       # Regex pattern to match CSV column variants
    data_type: str         # 'TEXT', 'INTEGER', 'REAL'
    handles_bom: bool = False  # Can have UTF-8 BOM prefix
    required: bool = False     # Must be present in CSV
    indexed: bool = False      # Create index on this column
    unique: bool = False       # Create unique index
    index_name: Optional[str] = None  # Custom index name
    
    def matches_csv_column(self, csv_column: str) -> bool:
        """Check if this column definition matches a CSV column name."""
        patterns_to_check = [self.csv_pattern]
        
        # Add BOM variant if this column handles BOM
        if self.handles_bom:
            patterns_to_check.append(f'\\ufeff{self.csv_pattern}')
            
        for pattern in patterns_to_check:
            if re.match(pattern, csv_column, re.IGNORECASE):
                return True
        return False
    
    def convert_value(self, raw_value: str) -> Any:
        """Convert raw CSV value to appropriate type."""
        if not raw_value or raw_value.strip() == '':
            return None
            
        clean_value = raw_value.strip()
        
        if self.data_type == 'INTEGER':
            try:
                return int(clean_value)
            except (ValueError, TypeError):
                return None
        elif self.data_type == 'REAL':
            try:
                return float(clean_value)
            except (ValueError, TypeError):
                return None
        else:  # TEXT
            return clean_value


# ==============================================================================
# TABLE STRUCTURE DEFINITIONS - Data-driven column specifications
# ==============================================================================

COLUMN_DEFINITIONS = {
    'meetwaarden_raw': {
        # Core identification fields
        'Meetjaar': ColumnDef('Meetjaar', r'Meetjaar', 'INTEGER', handles_bom=True, required=True, indexed=True),
        'WaterbeheerderCode': ColumnDef('WaterbeheerderCode', r'WaterbeheerderCode', 'TEXT', indexed=True),
        'WaterbeheerderNaam': ColumnDef('WaterbeheerderNaam', r'WaterbeheerderNaam', 'TEXT'),
        'MeetobjectCode': ColumnDef('MeetobjectCode', r'MeetobjectCode', 'TEXT', required=True, indexed=True),
        'MeetobjectLokaalID': ColumnDef('MeetobjectLokaalID', r'MeetobjectLokaalID', 'TEXT'),
        'MeetobjectNamespace': ColumnDef('MeetobjectNamespace', r'MeetobjectNamespace', 'TEXT'),
        'MeetwaardeLokaalID': ColumnDef('MeetwaardeLokaalID', r'MeetwaardeLokaalID', 'TEXT'),
        'Namespace': ColumnDef('Namespace', r'Namespace', 'TEXT'),
        'Land': ColumnDef('Land', r'Land', 'TEXT'),
        
        # Geographic fields (schema-dependent)
        'ProvincieCode': ColumnDef('ProvincieCode', r'ProvincieCode', 'TEXT'),
        'ProvincieNaam': ColumnDef('ProvincieNaam', r'ProvincieNaam', 'TEXT'),
        'GeometriePuntX_RD': ColumnDef('GeometriePuntX_RD', r'GeometriePuntX_RD', 'REAL'),
        'GeometriePuntY_RD': ColumnDef('GeometriePuntY_RD', r'GeometriePuntY_RD', 'REAL'),
        'PublicatieDatumTijd': ColumnDef('PublicatieDatumTijd', r'PublicatieDatumTijd', 'TEXT'),
        
        # Sampling fields
        'MonsterIdentificatie': ColumnDef('MonsterIdentificatie', r'MonsterIdentificatie', 'TEXT'),
        'MonsterLokaalID': ColumnDef('MonsterLokaalID', r'MonsterLokaalID', 'TEXT'),
        'MonsterCompartimentCode': ColumnDef('MonsterCompartimentCode', r'MonsterCompartimentCode', 'TEXT'),
        'BemonsteringsapparaatCode': ColumnDef('BemonsteringsapparaatCode', r'BemonsteringsapparaatCode', 'TEXT'),
        'BemonsteringsapparaatOmschrijving': ColumnDef('BemonsteringsapparaatOmschrijving', r'BemonsteringsapparaatOmschrijving', 'TEXT'),
        'Monsterophaaldatum': ColumnDef('Monsterophaaldatum', r'Monsterophaaldatum', 'TEXT'),
        'Monsterophaaltijd': ColumnDef('Monsterophaaltijd', r'Monsterophaaltijd', 'TEXT'),
        'AnalyseCompartimentCode': ColumnDef('AnalyseCompartimentCode', r'AnalyseCompartimentCode', 'TEXT'),
        'AnalyseCompartimentOmschrijving': ColumnDef('AnalyseCompartimentOmschrijving', r'AnalyseCompartimentOmschrijving', 'TEXT'),
        
        # Temporal fields with case variations
        'Resultaatdatum': ColumnDef('Resultaatdatum', r'Resultaat[dD]atum', 'TEXT', indexed=True),  # Case variation
        'Resultaattijd': ColumnDef('Resultaattijd', r'Resultaattijd', 'TEXT'),
        'Begindatum': ColumnDef('Begindatum', r'Begindatum', 'TEXT', indexed=True),
        'Begintijd': ColumnDef('Begintijd', r'Begintijd', 'TEXT'),
        'Einddatum': ColumnDef('Einddatum', r'Einddatum', 'TEXT'),
        'Eindtijd': ColumnDef('Eindtijd', r'Eindtijd', 'TEXT'),
        
        # Parameter fields
        'ParameterCode': ColumnDef('ParameterCode', r'ParameterCode', 'TEXT', indexed=True),
        'ParameterOmschrijving': ColumnDef('ParameterOmschrijving', r'ParameterOmschrijving', 'TEXT'),
        'CASNummer': ColumnDef('CASNummer', r'(CASNummer|ParameterCASnummer)', 'TEXT', indexed=True),
        'TyperingCode': ColumnDef('TyperingCode', r'TyperingCode', 'TEXT'),
        'TyperingOmschrijving': ColumnDef('TyperingOmschrijving', r'TyperingOmschrijving', 'TEXT'),
        'GrootheidCode': ColumnDef('GrootheidCode', r'GrootheidCode', 'TEXT'),
        'GrootheidOmschrijving': ColumnDef('GrootheidOmschrijving', r'GrootheidOmschrijving', 'TEXT'),
        'EenheidCode': ColumnDef('EenheidCode', r'EenheidCode', 'TEXT'),
        'EenheidOmschrijving': ColumnDef('EenheidOmschrijving', r'(Eenheid|EenheidOmschrijving)', 'TEXT'),
        'HoedanigheidCode': ColumnDef('HoedanigheidCode', r'HoedanigheidCode', 'TEXT'),
        'HoedanigheidOmschrijving': ColumnDef('HoedanigheidOmschrijving', r'HoedanigheidOmschrijving', 'TEXT'),
        'Limietsymbool': ColumnDef('Limietsymbool', r'Limietsymbool', 'TEXT'),
        'ParameterGroep': ColumnDef('ParameterGroep', r'ParameterGroep', 'TEXT'),
        'Compartiment': ColumnDef('Compartiment', r'Compartiment', 'TEXT'),
        
        # Value fields - CRITICAL: Handle case variations
        'Numeriekewaarde': ColumnDef('Numeriekewaarde', r'(NumeriekeWaarde|Numeriekewaarde)', 'REAL'),
        'Eenheid': ColumnDef('Eenheid', r'Eenheid', 'TEXT'),
        'Alfanumeriekewaarde': ColumnDef('Alfanumeriekewaarde', r'(AlfanumeriekeWaarde|Alfanumeriekewaarde)', 'TEXT'),
        'KwaliteitsoordeelCode': ColumnDef('KwaliteitsoordeelCode', r'KwaliteitsoordeelCode', 'TEXT'),
        'KwaliteitsoordeelOmschrijving': ColumnDef('KwaliteitsoordeelOmschrijving', r'KwaliteitsoordeelOmschrijving', 'TEXT'),
        
        # Biological fields
        'BiotaxonNaam': ColumnDef('BiotaxonNaam', r'BiotaxonNaam', 'TEXT'),
        'BiotaxonNaam_Nederlands': ColumnDef('BiotaxonNaam_Nederlands', r'BiotaxonNaam_Nederlands', 'TEXT'),
        'OrgaanCode': ColumnDef('OrgaanCode', r'OrgaanCode', 'TEXT'),
        'OrgaanOmschrijving': ColumnDef('OrgaanOmschrijving', r'OrgaanOmschrijving', 'TEXT'),
        'OrganismeNaam': ColumnDef('OrganismeNaam', r'OrganismeNaam', 'TEXT'),
        'OrganismeNaam_Nederlands': ColumnDef('OrganismeNaam_Nederlands', r'OrganismeNaam_Nederlands', 'TEXT'),
        'LevensstadiumCode': ColumnDef('LevensstadiumCode', r'LevensstadiumCode', 'TEXT'),
        'LevensstadiumOmschrijving': ColumnDef('LevensstadiumOmschrijving', r'LevensstadiumOmschrijving', 'TEXT'),
        'LengteklasseCode': ColumnDef('LengteklasseCode', r'LengteklasseCode', 'TEXT'),
        'LengteklasseOmschrijving': ColumnDef('LengteklasseOmschrijving', r'LengteklasseOmschrijving', 'TEXT'),
        'GeslachtCode': ColumnDef('GeslachtCode', r'GeslachtCode', 'TEXT'),
        'GeslachtOmschrijving': ColumnDef('GeslachtOmschrijving', r'GeslachtOmschrijving', 'TEXT'),
        'VerschijningsvormCode': ColumnDef('VerschijningsvormCode', r'VerschijningsvormCode', 'TEXT'),
        'VerschijningsvormOmschrijving': ColumnDef('VerschijningsvormOmschrijving', r'VerschijningsvormOmschrijving', 'TEXT'),
        'LevensvormCode': ColumnDef('LevensvormCode', r'LevensvormCode', 'TEXT'),
        'LevensvormOmschrijving': ColumnDef('LevensvormOmschrijving', r'LevensvormOmschrijving', 'TEXT'),
        'GedragCode': ColumnDef('GedragCode', r'GedragCode', 'TEXT'),
        'GedragOmschrijving': ColumnDef('GedragOmschrijving', r'GedragOmschrijving', 'TEXT'),
        
        # Methodology fields
        'WaardebewerkingsmethodeCode': ColumnDef('WaardebewerkingsmethodeCode', r'WaardebewerkingsmethodeCode', 'TEXT'),
        'WaardebewerkingsmethodeOmschrijving': ColumnDef('WaardebewerkingsmethodeOmschrijving', r'WaardebewerkingsmethodeOmschrijving', 'TEXT'),
        'WaardebepalingsmethodeCode': ColumnDef('WaardebepalingsmethodeCode', r'WaardebepalingsmethodeCode', 'TEXT'),
        'WaardebepalingsmethodeOmschrijving': ColumnDef('WaardebepalingsmethodeOmschrijving', r'WaardebepalingsmethodeOmschrijving', 'TEXT'),
        'LocatieTypeWaardeBepalingID': ColumnDef('LocatieTypeWaardeBepalingID', r'LocatieTypeWaardeBepalingID', 'TEXT'),
        'LocatieTypeWaardeBepalingOmschrijving': ColumnDef('LocatieTypeWaardeBepalingOmschrijving', r'LocatieTypeWaardeBepalingOmschrijving', 'TEXT'),
    },
    
    'meetobjecten_raw': {
        # Core identification
        'Meetjaar': ColumnDef('Meetjaar', r'Meetjaar', 'INTEGER', handles_bom=True, required=True, indexed=True),
        'Namespace': ColumnDef('Namespace', r'Namespace', 'TEXT'),
        'Identificatie': ColumnDef('Identificatie', r'Identificatie', 'TEXT', indexed=True),
        'MeetobjectCode': ColumnDef('MeetobjectCode', r'MeetobjectCode', 'TEXT', required=True, indexed=True),
        'Omschrijving': ColumnDef('Omschrijving', r'Omschrijving', 'TEXT'),
        'WaterbeheerderCode': ColumnDef('WaterbeheerderCode', r'WaterbeheerderCode', 'TEXT'),
        'WaterbeheerderNaam': ColumnDef('WaterbeheerderNaam', r'WaterbeheerderNaam', 'TEXT'),
        'ProvincieCode': ColumnDef('ProvincieCode', r'ProvincieCode', 'TEXT'),
        'ProvincieNaam': ColumnDef('ProvincieNaam', r'ProvincieNaam', 'TEXT'),
        'GeometriePuntX_RD': ColumnDef('GeometriePuntX_RD', r'GeometriePuntX_RD', 'REAL'),
        'GeometriePuntY_RD': ColumnDef('GeometriePuntY_RD', r'GeometriePuntY_RD', 'REAL'),
        'Land': ColumnDef('Land', r'Land', 'TEXT'),
        
        # Groundwater specific
        'GrondwaterlichaamCode': ColumnDef('GrondwaterlichaamCode', r'GrondwaterlichaamCode', 'TEXT'),
        'GrondwaterlichaamNaam': ColumnDef('GrondwaterlichaamNaam', r'GrondwaterlichaamNaam', 'TEXT'),
        'BROlocatieCode': ColumnDef('BROlocatieCode', r'BRO-?locatieCode', 'TEXT'),  # Handle optional hyphen
        'NITGCode': ColumnDef('NITGCode', r'NITGCode', 'TEXT'),
        'OLGACode': ColumnDef('OLGACode', r'OLGACode', 'TEXT'),
        'RIVMCode': ColumnDef('RIVMCode', r'RIVMCode', 'TEXT'),
        
        # Water type classification
        'KRWwatertypeCode': ColumnDef('KRWwatertypeCode', r'KRWwatertypeCode', 'TEXT'),
        'KRWwatertypeOmschrijving': ColumnDef('KRWwatertypeOmschrijving', r'KRWwatertypeOmschrijving', 'TEXT'),
        'WatergangCategorieCode': ColumnDef('WatergangCategorieCode', r'WatergangCategorieCode', 'TEXT'),
        'WatergangCategorieOmschrijving': ColumnDef('WatergangCategorieOmschrijving', r'WatergangCategorieOmschrijving', 'TEXT'),
        
        # Monitoring object details
        'MonitoringobjectsoortCode': ColumnDef('MonitoringobjectsoortCode', r'MonitoringobjectsoortCode', 'TEXT'),
        'MonitoringobjectsoortOmschrijving': ColumnDef('MonitoringobjectsoortOmschrijving', r'MonitoringobjectsoortOmschrijving', 'TEXT'),
        'Meetobjectmonitoringtype': ColumnDef('Meetobjectmonitoringtype', r'Meetobjectmonitoringtype', 'TEXT'),
        
        # Filter depth - Handle optional parentheses in CSV names
        'Filterdiepte_bovenkant_MV_m': ColumnDef('Filterdiepte_bovenkant_MV_m', r'Filterdiepte_bovenkant_MV(_m|\(m\))?', 'REAL'),
        'Filterdiepte_onderkant_MV_m': ColumnDef('Filterdiepte_onderkant_MV_m', r'Filterdiepte_onderkant_MV(_m|\(m\))?', 'REAL'),
        'Filterdiepte_bovenkant_NAP_m': ColumnDef('Filterdiepte_bovenkant_NAP_m', r'Filterdiepte_bovenkant_NAP(_m|\(m\))?', 'REAL'),
        'Maaiveld_NAP_m': ColumnDef('Maaiveld_NAP_m', r'Maaiveld_NAP(_m|\(m\))?', 'REAL'),
        'MaaiveldGeschat': ColumnDef('MaaiveldGeschat', r'MaaiveldGeschat', 'TEXT'),
        'Filtergrondwaterleeftijd': ColumnDef('Filtergrondwaterleeftijd', r'Filtergrondwaterleeftijd', 'TEXT'),
        'Toetsdiepte_KRW': ColumnDef('Toetsdiepte_KRW', r'Toetsdiepte_KRW', 'TEXT'),
        
        # Land use
        'LandgebruikCode': ColumnDef('LandgebruikCode', r'LandgebruikCode', 'TEXT'),
        'LandgebruikOmschrijving': ColumnDef('LandgebruikOmschrijving', r'LandgebruikOmschrijving', 'TEXT'),
        'Landgebruik_intrekgebiedCode': ColumnDef('Landgebruik_intrekgebiedCode', r'Landgebruik_intrekgebiedCode', 'TEXT'),
        'Landgebruik_intrekgebiedOmschrijving': ColumnDef('Landgebruik_intrekgebiedOmschrijving', r'Landgebruik_intrekgebiedOmschrijving', 'TEXT'),
        'Bodemsoortcode_maaiveld': ColumnDef('Bodemsoortcode_maaiveld', r'Bodemsoortcode_maaiveld', 'TEXT'),
        'Hydrologie': ColumnDef('Hydrologie', r'Hydrologie', 'TEXT'),
        'Drinkwater_KRW': ColumnDef('Drinkwater_KRW', r'Drinkwater_KRW', 'TEXT'),
        
        # Relationships
        'HoortbijGeoobjectIdentificatie': ColumnDef('HoortbijGeoobjectIdentificatie', r'HoortbijGeoobjectIdentificatie', 'TEXT'),
        'LigtInGeoobjectIdentificatie': ColumnDef('LigtInGeoobjectIdentificatie', r'LigtInGeoobjectIdentificatie', 'TEXT'),
        'Wegingsfactor': ColumnDef('Wegingsfactor', r'Wegingsfactor', 'REAL'),
        'PublicatieDatumTijd': ColumnDef('PublicatieDatumTijd', r'PublicatieDatumTijd', 'TEXT'),
        
        # Additional metadata fields
        'Toelichting': ColumnDef('Toelichting', r'Toelichting', 'TEXT'),
        'GeoobjectCodeVoorganger': ColumnDef('GeoobjectCodeVoorganger', r'GeoobjectCodeVoorganger', 'TEXT'),
        'DatumInGebruikname': ColumnDef('DatumInGebruikname', r'DatumInGebruikname', 'TEXT'),
        'DatumBuitenGebruikname': ColumnDef('DatumBuitenGebruikname', r'DatumBuitenGebruikname', 'TEXT'),
    },
    
    'parameter_raw': {
        # AQUO Parameter reference data columns
        'Pagina': ColumnDef('Pagina', r'Pagina', 'TEXT', handles_bom=True),
        'Id': ColumnDef('Id', r'Id', 'INTEGER', indexed=True),
        'Code': ColumnDef('Code', r'Code', 'TEXT', required=True, indexed=True),
        'Omschrijving': ColumnDef('Omschrijving', r'Omschrijving', 'TEXT'),
        'Groep': ColumnDef('Groep', r'Groep', 'TEXT', indexed=True),
        'CASnummer': ColumnDef('CASnummer', r'CASnummer', 'TEXT', indexed=True),
        'Alternatieve_term': ColumnDef('Alternatieve_term', r'Alternatieve term', 'TEXT'),
        'Begin_geldigheid': ColumnDef('Begin_geldigheid', r'Begin geldigheid', 'TEXT'),
        'Gerelateerd': ColumnDef('Gerelateerd', r'Gerelateerd', 'TEXT'),
    }
}


class RWSImporter:
    """Main importer class using the data-driven column definitions."""
    
    def __init__(self, database_path: str, dry_run: bool = False, drop_table: bool = False, log_file: str = None):
        self.database_path = database_path
        self.dry_run = dry_run
        self.drop_table = drop_table
        self.log_file = log_file
        self.conn = None
        
    def __enter__(self):
        if not self.dry_run:
            self.conn = sqlite3.connect(self.database_path)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def log_message(self, message: str, level: str = "INFO"):
        """Log message to file if log_file is specified, otherwise to stdout."""
        if self.log_file:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"[{timestamp}] [{level}] {message}"
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"{formatted_message}\n")
        else:
            # Only print non-INFO messages to stdout when not logging to file
            if level != "INFO":
                print(f"[{level}] {message}")
            else:
                print(message)
    
    def detect_csv_delimiter(self, csv_file: Path) -> str:
        """Detect CSV delimiter by checking for common delimiters."""
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            # Count occurrences of common delimiters
            semicolon_count = first_line.count(';')
            comma_count = first_line.count(',')
            
            # Return the delimiter with more occurrences
            if semicolon_count > comma_count:
                return ';'
            else:
                return ','
    
    def analyze_csv_columns(self, csv_file: Path, table_name: str) -> Dict[str, str]:
        """Map CSV columns to database columns using regex patterns."""
        column_defs = COLUMN_DEFINITIONS.get(table_name, {})
        csv_to_db_mapping = {}
        
        # Detect CSV delimiter
        delimiter = self.detect_csv_delimiter(csv_file)
        
        # Read CSV header
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=delimiter)
            csv_columns = next(reader)
        
        # Match columns
        for csv_col in csv_columns:
            matched = False
            for db_col, col_def in column_defs.items():
                if col_def.matches_csv_column(csv_col):
                    csv_to_db_mapping[csv_col] = db_col
                    matched = True
                    break
            
            if not matched:
                self.log_message(f"Unmatched CSV column: '{csv_col}' in {csv_file}", "WARNING")
        
        # Check required columns
        required_db_cols = {db_col for db_col, col_def in column_defs.items() if col_def.required}
        mapped_db_cols = set(csv_to_db_mapping.values())
        missing_required = required_db_cols - mapped_db_cols
        
        if missing_required:
            raise ValueError(f"Missing required columns in {csv_file}: {missing_required}")
            
        return csv_to_db_mapping
    
    def detect_schema_type(self, csv_file: Path) -> str:
        """Detect if CSV uses extended or compact schema."""
        delimiter = self.detect_csv_delimiter(csv_file)
        
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=delimiter)
            csv_columns = next(reader)
            
        # Extended schema has ProvincieCode, compact has PublicatieDatumTijd
        has_provincie = any(re.match(r'ProvincieCode', col, re.IGNORECASE) for col in csv_columns)
        has_publicatie = any(re.match(r'PublicatieDatumTijd', col, re.IGNORECASE) for col in csv_columns)
        
        if has_provincie and not has_publicatie:
            return 'extended'
        elif has_publicatie and not has_provincie:
            return 'compact'
        else:
            return 'unknown'
    
    def create_table(self, table_name: str):
        """Create database table from column definitions."""
        column_defs = COLUMN_DEFINITIONS.get(table_name, {})
        if not column_defs:
            raise ValueError(f"No column definitions found for table '{table_name}'")
        
        if self.dry_run:
            if self.drop_table:
                print(f"DRY RUN: Would drop table '{table_name}' if exists")
            print(f"DRY RUN: Would create table '{table_name}'")
            return
        
        # Drop table if requested
        if self.drop_table:
            print(f"Dropping table '{table_name}' if exists...")
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        # Build CREATE TABLE statement
        columns = []
        for db_col, col_def in column_defs.items():
            null_constraint = "NOT NULL" if col_def.required else ""
            columns.append(f"{db_col} {col_def.data_type} {null_constraint}")
        
        # Add metadata columns
        columns.extend([
            "source_file TEXT",
            "import_timestamp TEXT DEFAULT CURRENT_TIMESTAMP"
        ])
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        
        self.conn.execute(create_sql)
        
        # Create indexes based on column definitions
        for db_col, col_def in column_defs.items():
            if col_def.indexed:
                index_name = col_def.index_name or f"idx_{table_name}_{db_col.lower()}"
                unique_clause = "UNIQUE" if col_def.unique else ""
                index_sql = f"CREATE {unique_clause} INDEX IF NOT EXISTS {index_name} ON {table_name}({db_col})"
                self.conn.execute(index_sql)
        
        self.conn.commit()
    
    def import_csv_file(self, csv_file: Path, table_name: str) -> int:
        """Import single CSV file into specified table."""
        print(f"Processing: {csv_file}")
        
        # Analyze columns
        csv_to_db_mapping = self.analyze_csv_columns(csv_file, table_name)
        schema_type = self.detect_schema_type(csv_file)
        delimiter = self.detect_csv_delimiter(csv_file)
        
        print(f"  Schema type: {schema_type}")
        print(f"  Mapped {len(csv_to_db_mapping)} columns")
        
        if self.dry_run:
            # In dry run, read and count rows but don't insert to database
            imported_count = 0
            column_defs = COLUMN_DEFINITIONS[table_name]
            
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                
                for row in reader:
                    # Process row to validate conversion (but don't store)
                    for csv_col, db_col in csv_to_db_mapping.items():
                        raw_value = row.get(csv_col, '')
                        col_def = column_defs[db_col]
                        converted_value = col_def.convert_value(raw_value)
                    imported_count += 1
            
            print(f"  DRY RUN: Would import {imported_count:,} rows")
            return imported_count
        
        # Prepare insert statement
        db_columns = list(csv_to_db_mapping.values()) + ['source_file']
        placeholders = ', '.join(['?'] * len(db_columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(db_columns)}) VALUES ({placeholders})"
        
        # Process rows
        imported_count = 0
        column_defs = COLUMN_DEFINITIONS[table_name]
        
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            batch = []
            
            for row in reader:
                # Convert values using column definitions
                db_values = []
                for csv_col, db_col in csv_to_db_mapping.items():
                    raw_value = row.get(csv_col, '')
                    col_def = column_defs[db_col]
                    converted_value = col_def.convert_value(raw_value)
                    db_values.append(converted_value)
                
                # Add source file
                db_values.append(str(csv_file))
                batch.append(db_values)
                
                # Batch insert
                if len(batch) >= 1000:
                    self.conn.executemany(insert_sql, batch)
                    imported_count += len(batch)
                    batch = []
            
            # Insert remaining rows
            if batch:
                self.conn.executemany(insert_sql, batch)
                imported_count += len(batch)
        
        self.conn.commit()
        print(f"  Imported {imported_count:,} rows")
        return imported_count
    
    def import_files(self, files: List[Path], table_name: str) -> int:
        """Import multiple files into specified table."""
        if not files:
            raise ValueError("No files specified for import")
        
        # Create table
        self.create_table(table_name)
        
        total_imported = 0
        for csv_file in files:
            if not csv_file.exists():
                self.log_message(f"File not found: {csv_file}", "WARNING")
                continue
            
            try:
                count = self.import_csv_file(csv_file, table_name)
                total_imported += count
            except Exception as e:
                self.log_message(f"Processing {csv_file}: {e}", "ERROR")
                continue
        
        print(f"\nTotal imported: {total_imported:,} rows")
        return total_imported


def parse_arguments():
    """Parse command line arguments."""
    script_name = Path(sys.argv[0]).name
    parser = argparse.ArgumentParser(
        prog=script_name,
        description="RWS Water Quality Data Import Tool - Data-driven CSV Importer for Dutch Water Quality Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Import water quality measurements (uses default rws.db)
  {script_name} waardes data/*.csv
  
  # Import with custom database
  {script_name} waardes data/*.csv --db water_quality.db
  
  # Import measurement objects with fresh table
  {script_name} objects meetobjecten_*.csv --drop
  
  # Dry run to validate files and check column mapping
  {script_name} waardes data/*.csv --dry-run
  
  # Test column recognition across all files
  {script_name} waardes meten/*Meetwaarden*.csv --dry-run

Features:
  • Data-driven column mapping with regex patterns
  • Automatic schema detection (extended vs compact)
  • BOM handling for UTF-8 files  
  • Semicolon delimiter support
  • Robust type conversion and validation
  • Batch processing for performance
  • Comprehensive dry-run capabilities

Supported Tables:
  • meetwaarden_raw: Water quality measurement data
  • meetobjecten_raw: Measurement location objects
  • parameter_raw: AQUO parameter reference data

For more information, visit: https://github.com/example/water-quality-import
        """
    )
    
    # Add version information
    parser.add_argument('--version', action='version', version='%(prog)s 2.0.0')
    
    subparsers = parser.add_subparsers(
        dest='command', 
        help='Import commands - use "COMMAND --help" for detailed options',
        metavar='COMMAND'
    )
    
    # Waardes subcommand
    waardes_parser = subparsers.add_parser(
        'waardes', 
        help='Import water quality measurements (meetwaarden)',
        description='Import water quality measurement data from CSV files into meetwaarden_raw table.',
        epilog=f"""Examples:
  {script_name} waardes file1.csv file2.csv
  {script_name} waardes meten/*Meetwaarden*.csv --drop
  {script_name} waardes data/*.csv --db custom.db --dry-run
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    waardes_parser.add_argument('files', nargs='*', help='CSV files to import (supports wildcards)')
    waardes_parser.add_argument('--db', default='rws.db', metavar='FILE', help='SQLite database file path (default: rws.db)')
    waardes_parser.add_argument('--dry-run', action='store_true', help='Preview import without writing data - shows column mapping and row counts')
    waardes_parser.add_argument('--drop', action='store_true', help='Drop existing table before importing (creates fresh table)')
    waardes_parser.add_argument('-l', '--log-file', metavar='FILE', help='Log warnings and errors to file instead of stdout')
    
    # Objects subcommand  
    objects_parser = subparsers.add_parser(
        'objects', 
        help='Import measurement location objects (meetobjecten)',
        description='Import measurement location/object data from CSV files into meetobjecten_raw table.',
        epilog=f"""Examples:
  {script_name} objects locations.csv
  {script_name} objects meten/*Meetobjecten*.csv --drop
  {script_name} objects data/*.csv --db custom.db --dry-run
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    objects_parser.add_argument('files', nargs='*', help='CSV files to import (supports wildcards)')
    objects_parser.add_argument('--db', default='rws.db', metavar='FILE', help='SQLite database file path (default: rws.db)')
    objects_parser.add_argument('--dry-run', action='store_true', help='Preview import without writing data - shows column mapping and row counts')
    objects_parser.add_argument('--drop', action='store_true', help='Drop existing table before importing (creates fresh table)')
    objects_parser.add_argument('-l', '--log-file', metavar='FILE', help='Log warnings and errors to file instead of stdout')
    
    # AQUO subcommand
    aquo_parser = subparsers.add_parser(
        'aquo', 
        help='Import AQUO reference data (parameter definitions)',
        description='Import AQUO parameter reference data from CSV files into parameter_raw table.',
        epilog=f"""Examples:
  {script_name} aquo Parameter.csv
  {script_name} aquo aquo/csvs/Parameter.csv --drop
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    aquo_parser.add_argument('files', nargs='*', help='CSV files to import')
    aquo_parser.add_argument('--db', default='rws.db', metavar='FILE', help='SQLite database file path (default: rws.db)')
    aquo_parser.add_argument('--dry-run', action='store_true', help='Preview import without writing data')
    aquo_parser.add_argument('--drop', action='store_true', help='Drop existing table before importing')
    aquo_parser.add_argument('-l', '--log-file', metavar='FILE', help='Log warnings and errors to file instead of stdout')
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    script_name = Path(sys.argv[0]).name
    
    if not args.command:
        print("🚰 RWS Water Quality Data Import Tool v2.0.0")
        print("=" * 50)
        print("ERROR: No command specified.")
        print("\nAvailable commands:")
        print("  waardes   Import water quality measurements (meetwaarden)")
        print("  objects   Import measurement location objects (meetobjecten)")
        print("  aquo      Import AQUO reference data (future)")
        print("\nFor detailed help:")
        print(f"  {script_name} --help")
        print(f"  {script_name} COMMAND --help")
        print("\nQuick start:")
        print(f"  {script_name} waardes *.csv --dry-run")
        return 1
    
    # Convert file paths
    files = [Path(f) for f in args.files] if args.files else []
    
    # Map commands to table names
    table_mapping = {
        'waardes': 'meetwaarden_raw',
        'objects': 'meetobjecten_raw',
        'aquo': 'parameter_raw'
    }
    
    table_name = table_mapping.get(args.command)
    if not table_name:
        print(f"Error: Unknown command '{args.command}'")
        return 1
    
    # Import files
    try:
        with RWSImporter(args.db, dry_run=args.dry_run, drop_table=args.drop, log_file=getattr(args, 'log_file', None)) as importer:
            # Log command line invocation if logging is enabled
            if getattr(args, 'log_file', None):
                import shlex
                # Reconstruct the command line for logging
                cmd_line = ' '.join(shlex.quote(arg) for arg in sys.argv)
                importer.log_message(f"Command: {cmd_line}", "INFO")
            
            importer.import_files(files, table_name)
        print("Import completed successfully!")
        return 0
        
    except Exception as e:
        print(f"Import failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())