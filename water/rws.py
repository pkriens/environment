#!/usr/bin/env python3
"""
RWS Water Quality Portal (WKP) Data Downloader

This script automates and allows wildcards for the user interface

     https://wkp.rws.nl/downloadmodule

Downloads water quality data from https://wkp.rws.nl/api/v1/data-downloads/download
Supports subject selection, filtering by year/area, and request limiting for testing.


"""

import argparse
import json
import os
import time
import fnmatch
import tempfile
import zipfile
import shutil
from typing import Dict, List, Any, Optional
import requests


# The data structures here define the possible permutations
# of the requests. First level is a code which specifies the
# subject id. Then next is the areaLevel, which defines the 
# areaName. Additionally, there are a number of years. In the UI
# these are selectable. Since we have them in a structure, we can
# allow the user of this script to wildcard. 

# WKP API Data Structure
WKP_DATA = {
    "Waterbeheerder": [
        "Hoogheemraadschap De Stichtse Rijnlanden",
        "Hoogheemraadschap Hollands Noorderkwartier", 
        "Hoogheemraadschap van Delfland",
        "Hoogheemraadschap van Rijnland",
        "Hoogheemraadschap van Schieland en Krimpenerwaard",
        "Rijkswaterstaat",
        "Waterschap Aa en Maas",
        "Waterschap Amstel Gooi en Vecht",
        "Waterschap Brabantse Delta",
        "Waterschap De Dommel",
        "Waterschap Drents Overijsselse Delta",
        "Waterschap Hollandse Delta",
        "Waterschap Hunze en Aa's",
        "Waterschap Limburg",
        "Waterschap Noorderzijlvest",
        "Waterschap Rijn en IJssel",
        "Waterschap Rivierenland",
        "Waterschap Scheldestromen",
        "Waterschap Vallei en Veluwe",
        "Waterschap Vechtstromen",
        "Waterschap Zuiderzeeland",
        "Wetterskip Fryslân"
    ],
    "Provincie": [
        "Drenthe", "Flevoland", "Fryslân", "Gelderland", "Groningen", 
        "Limburg", "Noord-Brabant", "Noord-Holland", "Overijssel", 
        "Utrecht", "Zeeland", "Zuid-Holland"
    ],
    "Stroomgebieddistricten": [
        "Eems", "Maas", "Rijn", "Schelde"
    ],
    "areaLevels": {
        "NSPW": {
            "nederland": [None],
            "stroomgebieddistricten": ["Eems", "Maas", "Rijn", "Schelde"],
            "provincie": ["Drenthe", "Flevoland", "Fryslân", "Gelderland", "Groningen", "Limburg", "Noord-Brabant", "Noord-Holland", "Overijssel", "Utrecht", "Zeeland", "Zuid-Holland"],
            "waterbeheerder": ["Hoogheemraadschap De Stichtse Rijnlanden", "Hoogheemraadschap Hollands Noorderkwartier", "Hoogheemraadschap van Delfland", "Hoogheemraadschap van Rijnland", "Hoogheemraadschap van Schieland en Krimpenerwaard", "Rijkswaterstaat", "Waterschap Aa en Maas", "Waterschap Amstel Gooi en Vecht", "Waterschap Brabantse Delta", "Waterschap De Dommel", "Waterschap Drents Overijsselse Delta", "Waterschap Hollandse Delta", "Waterschap Hunze en Aa's", "Waterschap Limburg", "Waterschap Noorderzijlvest", "Waterschap Rijn en IJssel", "Waterschap Rivierenland", "Waterschap Scheldestromen", "Waterschap Vallei en Veluwe", "Waterschap Vechtstromen", "Waterschap Zuiderzeeland", "Wetterskip Fryslân"]
        },
        "NW": {
            "nederland": [None],
            "waterbeheerder": ["Hoogheemraadschap De Stichtse Rijnlanden", "Hoogheemraadschap Hollands Noorderkwartier", "Hoogheemraadschap van Delfland", "Hoogheemraadschap van Rijnland", "Hoogheemraadschap van Schieland en Krimpenerwaard", "Rijkswaterstaat", "Waterschap Aa en Maas", "Waterschap Amstel Gooi en Vecht", "Waterschap Brabantse Delta", "Waterschap De Dommel", "Waterschap Drents Overijsselse Delta", "Waterschap Hollandse Delta", "Waterschap Hunze en Aa's", "Waterschap Limburg", "Waterschap Noorderzijlvest", "Waterschap Rijn en IJssel", "Waterschap Rivierenland", "Waterschap Scheldestromen", "Waterschap Vallei en Veluwe", "Waterschap Vechtstromen", "Waterschap Zuiderzeeland", "Wetterskip Fryslân"]
        },
        "NSW": {
            "nederland": [None],
            "stroomgebieddistricten": ["Eems", "Maas", "Rijn", "Schelde"],
            "waterbeheerder": ["Hoogheemraadschap De Stichtse Rijnlanden", "Hoogheemraadschap Hollands Noorderkwartier", "Hoogheemraadschap van Delfland", "Hoogheemraadschap van Rijnland", "Hoogheemraadschap van Schieland en Krimpenerwaard", "Rijkswaterstaat", "Waterschap Aa en Maas", "Waterschap Amstel Gooi en Vecht", "Waterschap Brabantse Delta", "Waterschap De Dommel", "Waterschap Drents Overijsselse Delta", "Waterschap Hollandse Delta", "Waterschap Hunze en Aa's", "Waterschap Limburg", "Waterschap Noorderzijlvest", "Waterschap Rijn en IJssel", "Waterschap Rivierenland", "Waterschap Scheldestromen", "Waterschap Vallei en Veluwe", "Waterschap Vechtstromen", "Waterschap Zuiderzeeland", "Wetterskip Fryslân"]
        },
        "PW": {
            "provincie": ["Drenthe", "Flevoland", "Fryslân", "Gelderland", "Groningen", "Limburg", "Noord-Brabant", "Noord-Holland", "Overijssel", "Utrecht", "Zeeland", "Zuid-Holland"],
            "waterbeheerder": ["Hoogheemraadschap De Stichtse Rijnlanden", "Hoogheemraadschap Hollands Noorderkwartier", "Hoogheemraadschap van Delfland", "Hoogheemraadschap van Rijnland", "Hoogheemraadschap van Schieland en Krimpenerwaard", "Rijkswaterstaat", "Waterschap Aa en Maas", "Waterschap Amstel Gooi en Vecht", "Waterschap Brabantse Delta", "Waterschap De Dommel", "Waterschap Drents Overijsselse Delta", "Waterschap Hollandse Delta", "Waterschap Hunze en Aa's", "Waterschap Limburg", "Waterschap Noorderzijlvest", "Waterschap Rijn en IJssel", "Waterschap Rivierenland", "Waterschap Scheldestromen", "Waterschap Vallei en Veluwe", "Waterschap Vechtstromen", "Waterschap Zuiderzeeland", "Wetterskip Fryslân"]
        }
    },
    "themes": {
        "Kaderrichtlijn Water": 1,
        "Ecologie": 2, 
        "Oppervlaktewaterkwaliteit": 3,
        "Grondwaterkwaliteit": 4
    },
    "subjects": {
        # Kaderrichtlijn Water
        "1": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-stroomgebieddistricten-geometrie",
            "subject": "KWSG",
            "years": [2025],
            "areaLevel": "N"
        },
        "2": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-oppervlaktewaterlichamen-geometrie",
            "subject": "KWOG",
            "years": [2025],
            "areaLevel": "N"
        },
        "3": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-grondwaterlichamen-geometrie",
            "subject": "KWGG",
            "years": [2025],
            "areaLevel": "N"
        },
        "4": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-beschermde gebieden-geometrie",
            "subject": "KWBG",
            "years": [2025],
            "areaLevel": "N"
        },
        "5": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-maatregelen",
            "subject": "KWMA",
            "years": list(range(2014, 2026)),
            "areaLevel": "N"
        },
        "6": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-belastingen en impacts",
            "subject": "KWBI",
            "years": [2023, 2024, 2025],
            "areaLevel": "NSPW"
        },
        "7": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-uitzonderingsbepalingen",
            "subject": "KWUB",
            "years": [2023, 2024, 2025],
            "areaLevel": "NSPW"
        },
        "8": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-doelen",
            "subject": "KWDO",
            "years": list(range(2010, 2026)),
            "areaLevel": "NW"
        },
        "9": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-monitoringlocaties-geometrie",
            "subject": "KWMG",
            "years": [2025],
            "areaLevel": "N"
        },
        "10": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-monitoringprogramma OW",
            "subject": "KWMO",
            "years": [2020, 2021, 2022, 2023, 2024, 2025],
            "areaLevel": "NSW"
        },
        "11": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-toestandsoordelen OW",
            "subject": "KWTO",
            "years": [2009, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "areaLevel": "NSPW"
        },
        "12": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-toetsresultaten OW",
            "subject": "KWTO",
            "years": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "areaLevel": "NW"
        },
        "13": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-toestandsoordelen GW",
            "subject": "KWOG",
            "years": [2009, 2014, 2017, 2020, 2021, 2023],
            "areaLevel": "NSPW"
        },
        "14": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-toetsresultaten GW",
            "subject": "KWRO",
            "years": [2014, 2017, 2020, 2023],
            "areaLevel": "NW"
        },
        "17": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-oppervlaktewaterlichamen",
            "subject": "KWOL",
            "years": [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "areaLevel": "NSPW"
        },
        "18": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-oppervlaktewaterlichamen-motivering-status",
            "subject": "KWOM",
            "years": [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "areaLevel": "NSW"
        },
        "19": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-grondwaterlichamen",
            "subject": "KWGL",
            "years": [2025],  # Added default year
            "areaLevel": "NSPW"
        },
        "20": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-monitoringprogramma GW",
            "subject": "KWMG",
            "years": [2020, 2021, 2023],
            "areaLevel": "NSW"
        },
        "21": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-doelbereik OW",
            "subject": "KWDO",
            "years": [3],  # Note: This seems to be a special case
            "areaLevel": "NSPW"
        },
        "22": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-doelbereik GW",
            "subject": "KWDG",
            "years": [3],  # Note: This seems to be a special case
            "areaLevel": "NSPW"
        },
        "23": {
            "theme": "Kaderrichtlijn Water",
            "name": "KRW-meetwaarden OW",
            "subject": "KWMO",
            "years": [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
            "areaLevel": "W"
        },
        # Ecologie
        "50": {
            "theme": "Ecologie",
            "name": "Milieu- en habitatvoorkeuren",
            "subject": "KWMH",
            "years": [2025],
            "areaLevel": "N"
        },
        "51": {
            "theme": "Ecologie",
            "name": "KRW-maatlatten 2018 - Fytoplankton",
            "subject": "ECMF",
            "years": [2026],
            "areaLevel": "N"
        },
        "52": {
            "theme": "Ecologie",
            "name": "KRW-maatlatten 2018 - Overige waterflora",
            "subject": "ECMO",
            "years": [2025, 2026],
            "areaLevel": "N"
        },
        "53": {
            "theme": "Ecologie",
            "name": "KRW-maatlatten 2018 - Macrofauna",
            "subject": "ECMM",
            "years": [2026],
            "areaLevel": "N"
        },
        "54": {
            "theme": "Ecologie",
            "name": "KRW-maatlatten 2018 - Vissen",
            "subject": "ECMV",
            "years": [2026],
            "areaLevel": "N"
        },
        # Oppervlaktewaterkwaliteit
        "15": {
            "theme": "Oppervlaktewaterkwaliteit",
            "name": "OW Meetgegevens",
            "subject": "OKME",
            "years": [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
            "areaLevel": "W"
        },
        # Grondwaterkwaliteit
        "16": {
            "theme": "Grondwaterkwaliteit",
            "name": "GW Meetgegevens",
            "subject": "GKME",
            "years": [1909, 1910, 1912, 1913, 1914, 1915, 1916, 1918, 1919, 1922, 1923, 1924, 1925, 1927, 1928, 1931, 1933, 1934, 1935, 1937, 1940, 1941, 1942, 1943, 1944, 1947, 1948, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
            "areaLevel": "W"
        }
    }
}

#
# DOWNLOADER CLASS
#

class WKPDownloader:
    def __init__(self):
        self.api_url = "https://wkp.rws.nl/api/v1/data-downloads/download"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0',
            'Accept': 'application/zip',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Content-Type': 'application/json',
            'Origin': 'https://wkp.rws.nl',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://wkp.rws.nl/downloadmodule',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Priority': 'u=0',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }

    def get_matching_subjects(self, patterns: List[str]) -> List[str]:
        if not patterns:
            return list(WKP_DATA["subjects"].keys())
        
        matching = set()
        for pattern in patterns:
            # First check for subject code matches
            for subject_id, subject_data in WKP_DATA["subjects"].items():
                subject_code = subject_data.get('subject', '')
                if fnmatch.fnmatch(subject_code, pattern):
                    matching.add(subject_id)
            
            # Then check for subject ID matches
            for subject_id in WKP_DATA["subjects"].keys():
                if fnmatch.fnmatch(subject_id, pattern):
                    matching.add(subject_id)
                    
        return sorted(matching, key=int)

    def expand_requests(self, subject_ids: List[str], year_filter: Optional[List[int]] = None, 
                       level_filter: Optional[List[str]] = None, 
                       name_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        api_requests = []
        
        for subject_id in subject_ids:
            if subject_id not in WKP_DATA["subjects"]:
                print(f"Warning: Unknown subject ID {subject_id}")
                continue
                
            subject = WKP_DATA["subjects"][subject_id]
            
            # Filter years
            years = subject["years"]
            if year_filter:
                years = [y for y in years if y in year_filter]
            
            # Get area levels based on subject's areaLevel configuration
            area_level_type = subject["areaLevel"]
            
            if area_level_type == "N":
                # Only nederland level
                for year in years:
                    api_requests.append({
                        "subjectId": int(subject_id),
                        "year": year,
                        "areaLevel": "nederland",
                        "areaName": None
                    })
            elif area_level_type == "W":
                # Only waterbeheerder level
                for year in years:
                    if level_filter and "waterbeheerder" not in level_filter:
                        continue
                    
                    waterbeheerders = WKP_DATA["Waterbeheerder"]
                    if name_filter:
                        waterbeheerders = [name for name in waterbeheerders 
                                         if any(fnmatch.fnmatch(name, pattern) for pattern in name_filter)]
                    
                    for waterbeheerder in waterbeheerders:
                        api_requests.append({
                            "subjectId": int(subject_id),
                            "year": year,
                            "areaLevel": "waterbeheerder",
                            "areaName": waterbeheerder
                        })
            elif area_level_type in WKP_DATA["areaLevels"]:
                # Use predefined area level combinations (NSPW, NW, NSW, PW)
                area_config = WKP_DATA["areaLevels"][area_level_type]
                
                for year in years:
                    for area_level, area_names in area_config.items():
                        if level_filter and area_level not in level_filter:
                            continue
                        
                        # Handle nederland special case (no area names)
                        if area_level == "nederland":
                            api_requests.append({
                                "subjectId": int(subject_id),
                                "year": year,
                                "areaLevel": area_level,
                                "areaName": None
                            })
                        else:
                            filtered_names = area_names
                            if name_filter:
                                filtered_names = [name for name in area_names 
                                                if any(fnmatch.fnmatch(name, pattern) for pattern in name_filter)]
                            
                            for area_name in filtered_names:
                                api_requests.append({
                                    "subjectId": int(subject_id),
                                    "year": year,
                                    "areaLevel": area_level,
                                    "areaName": area_name
                                })
            else:
                print(f"Warning: Unknown area level type '{area_level_type}' for subject {subject_id}")
        
        return api_requests

    def download_data(self, api_requests: List[Dict[str, Any]], output_dir: str = "./csvs", 
                     limit: Optional[int] = None, dry_run: bool = False, verbose: bool = False, force: bool = False) -> None:
        if limit:
            api_requests = api_requests[:limit]
            print(f"Limited to first {limit} requests")
        
        total_requests = len(api_requests)
        print(f"Total requests to execute: {total_requests}")
        
        if dry_run:
            print("DRY RUN - would execute these requests:")
            print(f"Output directory: {output_dir}")
            print()
            
            # Table header
            print(f"{'Code':<10} {'Name':<35} {'Year':<6} {'Area Level':<15} {'Area Name':<30}")
            print(f"{'-'*10:<10} {'-'*35:<35} {'-'*6:<6} {'-'*15:<15} {'-'*30:<30}")
            
            for i, req in enumerate(api_requests, 1):
                subject_info = WKP_DATA["subjects"][str(req["subjectId"])]
                area_name = req.get('areaName', '')
                if area_name is None:
                    area_name = ''
                
                # Combine code with subject ID
                code_with_id = f"{subject_info['subject']}({req['subjectId']})"
                
                # Truncate long subject names and area names for table display
                display_name = subject_info['name'][:33] + '..' if len(subject_info['name']) > 35 else subject_info['name']
                display_area_name = area_name[:28] + '..' if len(area_name) > 30 else area_name
                
                print(f"{code_with_id:<10} {display_name:<35} {req['year']:<6} {req['areaLevel']:<15} {display_area_name:<30}")
                
                if verbose:
                    # Show detailed HTTP request information
                    print(f"    POST {self.api_url}")
                    print(f"    Content-Type: {self.headers['Content-Type']}")
                    print(f"    User-Agent: {self.headers['User-Agent']}")
                    print(f"    JSON Payload: {json.dumps([req])}")
                    print()
            
            if not verbose:
                print()
            return
        
        os.makedirs(output_dir, exist_ok=True)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            for i, request in enumerate(api_requests, 1):
                print(f"Request {i}/{total_requests}: Subject {request['subjectId']}, Year {request['year']}, "
                      f"Level {request['areaLevel']}, Name {request.get('areaName', 'None')}")
                
                try:
                    http_response = requests.post(
                        self.api_url,
                        headers=self.headers,
                        data=json.dumps([request]),
                        timeout=60
                    )
                    
                    if http_response.status_code == 200:
                        # Generate temporary filename for zip
                        subject_info = WKP_DATA["subjects"][str(request["subjectId"])]
                        safe_name = "".join(c if c.isalnum() or c in ".-_" else "_" for c in subject_info["name"])
                        area_part = f"_{request['areaLevel']}"
                        if request.get('areaName'):
                            safe_area_name = "".join(c if c.isalnum() or c in ".-_" else "_" for c in request['areaName'])
                            area_part += f"_{safe_area_name}"
                        
                        zip_filename = f"subject_{request['subjectId']}_{safe_name}_year_{request['year']}{area_part}.zip"
                        zip_filepath = os.path.join(temp_dir, zip_filename)
                        
                        with open(zip_filepath, 'wb') as f:
                            f.write(http_response.content)
                        print(f"  Downloaded: {zip_filename} ({len(http_response.content)} bytes)")
                        
                        try:
                            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                                extracted_files = zip_ref.namelist()
                                
                                if not force:
                                    existing_files = []
                                    for filename in extracted_files:
                                        target_path = os.path.join(output_dir, filename)
                                        if os.path.exists(target_path):
                                            existing_files.append(filename)
                                    
                                    if existing_files:
                                        print(f"  Warning: {len(existing_files)} file(s) already exist, skipping extraction:")
                                        for filename in existing_files:
                                            print(f"    - {filename} (use --force to overwrite)")
                                        continue
                                
                                zip_ref.extractall(output_dir)
                                print(f"  Extracted {len(extracted_files)} file(s) to {output_dir}")
                                if len(extracted_files) <= 3:
                                    for filename in extracted_files:
                                        print(f"    - {filename}")
                                else:
                                    print(f"    - {extracted_files[0]}")
                                    print(f"    - ... and {len(extracted_files)-1} more")
                        except zipfile.BadZipFile:
                            print(f"  Error: Downloaded file is not a valid zip archive")
                            # Copy the raw file instead
                            raw_filename = zip_filename.replace('.zip', '.dat')
                            shutil.copy2(zip_filepath, os.path.join(output_dir, raw_filename))
                            print(f"  Saved raw file as: {raw_filename}")
                    else:
                        print(f"  Error: HTTP {http_response.status_code} - {http_response.text}")
                
                except Exception as e:
                    print(f"  Error: {e}")
                
                if i < total_requests:
                    time.sleep(1)

    def show_subject_details(self, codes: List[str]) -> None:
        print("Subject Details")
        print("=" * 50)
        
        found_subjects = []
        for sid, subject in WKP_DATA["subjects"].items():
            if subject.get('subject', '') in codes:
                found_subjects.append((int(sid), sid, subject))
        
        if not found_subjects:
            print(f"No subjects found for codes: {', '.join(codes)}")
            return
        
        for _, sid, subject in sorted(found_subjects):
            code_with_id = f"{subject.get('subject', 'N/A')}({sid})"
            print(f"\n{code_with_id}: {subject['name']}")
            print(f"  Theme:{'':<25}{subject['theme']}")
            
            # Show area level sets
            area_level_type = subject['areaLevel']
            total_areas = 0
            
            if area_level_type == 'N':
                print(f"  Area Levels:{'':<19}nederland(1)")
                total_areas = 1
            elif area_level_type == 'W':
                count = len(WKP_DATA['Waterbeheerder'])
                print(f"  Area Levels:{'':<19}waterbeheerder({count})")
                total_areas = count
            elif area_level_type in WKP_DATA['areaLevels']:
                area_config = WKP_DATA['areaLevels'][area_level_type]
                available_levels = []
                for level, names in area_config.items():
                    if level == "nederland":
                        available_levels.append(f"{level}(1)")
                        total_areas += 1
                    elif names and names != [None]:
                        count = len(names)
                        available_levels.append(f"{level}({count})")
                        total_areas += count
                
                # Print first level with label, rest aligned at same column
                if available_levels:
                    print(f"  Area Levels:{'':<19}{available_levels[0]}")
                    for level in available_levels[1:]:
                        print(f"{'':<33}{level}")
            
            # Calculate and show total permutations
            total_requests = total_areas * len(subject['years'])
            print(f"  Total Requests:{'':<16}{total_requests} (areas: {total_areas} × years: {len(subject['years'])})")
            
            # Show sample years
            years = subject['years']
            if len(years) <= 10:
                print(f"  Years:{'':<25}{', '.join(map(str, years))}")
            else:
                print(f"  Years:{'':<25}{', '.join(map(str, years[:5]))}, ..., {', '.join(map(str, years[-3:]))}")


    def show_info(self, name: str) -> None:
        name_lower = name.lower()
        
        print(f"Information: {name}")
        print("=" * (len(name) + 13))
        print()
        
        # Area levels
        if name_lower == 'nederland':
            print("Area Level: nederland")
            print("Description: Nederland level (entire Netherlands)")
            print("Count: 1 area")
            print("Used in area level types: NSPW, NW, NSW")
            
        elif name_lower in ['stroomgebieddistricten', 'stroomgebied', 'basins']:
            print("Area Level: stroomgebieddistricten")
            print("Description: River basin districts")
            print(f"Count: {len(WKP_DATA['Stroomgebieddistricten'])} areas")
            print("Used in area level types: NSPW, NSW")
            print("\nAreas:")
            for area in WKP_DATA['Stroomgebieddistricten']:
                print(f"  - {area}")
                
        elif name_lower in ['provincie', 'provincies', 'provinces']:
            print("Area Level: provincie")
            print("Description: Provincial administrative divisions")
            print(f"Count: {len(WKP_DATA['Provincie'])} areas")
            print("Used in area level types: NSPW, PW")
            print("\nAreas:")
            for area in WKP_DATA['Provincie']:
                print(f"  - {area}")
                
        elif name_lower in ['waterbeheerder', 'waterbeheerders', 'authorities', 'water-authorities']:
            print("Area Level: waterbeheerder")
            print("Description: Waterbeheerders")
            print(f"Count: {len(WKP_DATA['Waterbeheerder'])} areas")
            print("Used in area level types: NSPW, NW, NSW, PW")
            print("\nAreas:")
            for area in WKP_DATA['Waterbeheerder']:
                print(f"  - {area}")
        
        # Area level type combinations
        elif name_lower == 'nspw':
            print("Area Level Type: NSPW")
            print("Description: Nederland + Stroomgebieddistricten + Provincie + Waterbeheerder")
            config = WKP_DATA['areaLevels']['NSPW']
            total = 0
            print("\nIncludes:")
            for level, areas in config.items():
                if level == 'nederland':
                    count = 1
                    print(f"  - {level}: {count} area")
                else:
                    count = len(areas)
                    print(f"  - {level}: {count} areas")
                total += count
            print(f"\nTotal areas: {total}")
            
        elif name_lower == 'nw':
            print("Area Level Type: NW")
            print("Description: Nederland + Waterbeheerder")
            config = WKP_DATA['areaLevels']['NW']
            total = 0
            print("\nIncludes:")
            for level, areas in config.items():
                if level == 'nederland':
                    count = 1
                    print(f"  - {level}: {count} area")
                else:
                    count = len(areas)
                    print(f"  - {level}: {count} areas")
                total += count
            print(f"\nTotal areas: {total}")
            
        elif name_lower == 'nsw':
            print("Area Level Type: NSW")
            print("Description: Nederland + Stroomgebieddistricten + Waterbeheerder")
            config = WKP_DATA['areaLevels']['NSW']
            total = 0
            print("\nIncludes:")
            for level, areas in config.items():
                if level == 'nederland':
                    count = 1
                    print(f"  - {level}: {count} area")
                else:
                    count = len(areas)
                    print(f"  - {level}: {count} areas")
                total += count
            print(f"\nTotal areas: {total}")
            
        elif name_lower == 'pw':
            print("Area Level Type: PW")
            print("Description: Provincie + Waterbeheerder")
            config = WKP_DATA['areaLevels']['PW']
            total = 0
            print("\nIncludes:")
            for level, areas in config.items():
                count = len(areas)
                print(f"  - {level}: {count} areas")
                total += count
            print(f"\nTotal areas: {total}")
            
        elif name_lower in ['n', 'nederland_only', 'nederland-only']:
            print("Area Level Type: N (nederland_only)")
            print("Description: Nederland level data only")
            print("Count: 1 area")
            subjects = [s for s in WKP_DATA['subjects'].values() if s['areaLevel'] == 'N']
            print(f"Used by {len(subjects)} subjects")
            
        elif name_lower in ['w', 'waterbeheerder_only', 'waterbeheerder-only']:
            print("Area Level Type: W (waterbeheerder_only)")
            print("Description: Waterbeheerder level data only")
            print(f"Count: {len(WKP_DATA['Waterbeheerder'])} areas")
            subjects = [s for s in WKP_DATA['subjects'].values() if s['areaLevel'] == 'W']
            print(f"Used by {len(subjects)} subjects")
        
        # General data info
        elif name_lower in ['themes', 'themas']:
            print("Data Themes")
            print("Description: Main categories of water quality data")
            print(f"Count: {len(WKP_DATA['themes'])} themes")
            print("\nThemes:")
            for theme_name, theme_id in WKP_DATA['themes'].items():
                subject_count = len([s for s in WKP_DATA['subjects'].values() if s['theme'] == theme_name])
                print(f"  {theme_id}: {theme_name} ({subject_count} subjects)")
                
        elif name_lower in ['arealevels', 'area-levels', 'levels']:
            print("Area Level Types")
            print("Description: Different geographical scope combinations")
            print("\nTypes:")
            print("  - N: Nederland level only")
            print("  - W: Waterbeheerder level only")
            print("  - NSPW: Nederland + Stroomgebieddistricten + Provincie + Waterbeheerder")
            print("  - NW: Nederland + Waterbeheerder")
            print("  - NSW: Nederland + Stroomgebieddistricten + Waterbeheerder")
            print("  - PW: Provincie + Waterbeheerder")
            
        elif name_lower == 'years':
            print("Year Coverage")
            print("Description: Available data years across all subjects")
            all_years = set()
            for subject in WKP_DATA['subjects'].values():
                all_years.update(subject['years'])
            all_years = sorted(all_years)
            print(f"Range: {min(all_years)} - {max(all_years)}")
            print(f"Total years: {len(all_years)}")
            
        elif name_lower in ['labels', 'abbreviations', 'terms', 'glossary']:
            print("Labels and Abbreviations Used")
            print("Description: Explanation of all terms, codes, and abbreviations")
            print()
            print("=== ORGANIZATIONS ===")
            print("RWS         : Rijkswaterstaat (Dutch national water authority)")
            print("WKP         : Water Quality Portal (wkp.rws.nl)")
            print()
            print("=== REGULATORY FRAMEWORKS ===")
            print("KRW         : Kaderrichtlijn Water (Water Framework Directive)")
            print("AQUO        : Standard for water information exchange")
            print()
            print("=== AREA LEVELS (Geographic Scopes) ===")
            print("nederland               : Nederland level (entire Netherlands)")
            print("stroomgebieddistricten  : River basin districts (Eems, Maas, Rijn, Schelde)")
            print("provincie               : Provincial level (12 provinces)")
            print("waterbeheerder          : Waterbeheerders (22 organizations)")
            print()
            print("=== AREA LEVEL TYPES (Combinations) ===")
            print("N       : Nederland data only")
            print("W       : Waterbeheerder data only")
            print("NSPW    : Nederland + Stroomgebied + Provincie + Waterbeheerder")
            print("NW      : Nederland + Waterbeheerder")
            print("NSW     : Nederland + Stroomgebied + Waterbeheerder")
            print("PW      : Provincie + Waterbeheerder")
            print()
            print("=== DATA THEMES ===")
            print("Kaderrichtlijn Water       : Water Framework Directive compliance data")
            print("Ecologie                   : Ecological assessment data")
            print("Oppervlaktewaterkwaliteit  : Surface water quality measurements")
            print("Grondwaterkwaliteit        : Groundwater quality measurements")
            print()
            print("=== SUBJECT CODE PREFIXES ===")
            print("KW**  : Kaderrichtlijn Water (Water Framework Directive)")
            print("  KWSG: KRW-Stroomgebieddistricten-Geometrie")
            print("  KWOG: KRW-Oppervlaktewaterlichamen-Geometrie")
            print("  KWGG: KRW-Grondwaterlichamen-Geometrie")
            print("  KWBG: KRW-Beschermde Gebieden-Geometrie")
            print("  KWMA: KRW-Maatregelen")
            print("  KWBI: KRW-Belastingen en Impacts")
            print("  KWDO: KRW-Doelen")
            print("  KWTO: KRW-Toestandsoordelen")
            print("  KWRO: KRW-Toetsresultaten")
            print("  KWOL: KRW-Oppervlaktewaterlichamen")
            print("  KWGL: KRW-Grondwaterlichamen")
            print("  KWMO: KRW-Monitoringprogramma/Meetwaarden")
            print("  KWMG: KRW-Monitoringlocaties-Geometrie")
            print("  KWUB: KRW-Uitzonderingsbepalingen")
            print("  KWOM: KRW-Oppervlaktewaterlichamen-Motivering")
            print("  KWDG: KRW-Doelbereik GW")
            print("  KWMH: KRW-Milieu- en Habitatvoorkeuren")
            print()
            print("EC**  : Ecologie (Ecological assessments)")
            print("  ECMF: Ecologie-Maatlatten-Fytoplankton")
            print("  ECMO: Ecologie-Maatlatten-Overige waterflora")
            print("  ECMM: Ecologie-Maatlatten-Macrofauna")
            print("  ECMV: Ecologie-Maatlatten-Vissen")
            print()
            print("OKME  : Oppervlaktewaterkwaliteit-Meetgegevens")
            print("GKME  : Grondwaterkwaliteit-Meetgegevens")
            print()
            print("=== TECHNICAL ABBREVIATIONS ===")
            print("OW    : Oppervlaktewater (Surface water)")
            print("GW    : Grondwater (Groundwater)")
            print("API   : Application Programming Interface")
            print("JSON  : JavaScript Object Notation (data format)")
            print("CSV   : Comma-Separated Values (file format)")
            print()
            print("=== DUTCH WATER AUTHORITIES ===")
            print("Hoogheemraadschap  : High water authority (historical term)")
            print("Waterschap         : Water authority/board")
            print("Wetterskip         : Frisian term for water authority")
            print("Rijkswaterstaat    : National water management agency")
            print()
            print("=== COMMON DUTCH TERMS ===")
            print("meetgegevens       : measurement data")
            print("geometrie          : geometry/geographic boundaries")
            print("maatregelen        : measures/actions")
            print("belastingen        : pressures/impacts")
            print("doelen             : objectives/targets")
            print("toestandsoordelen  : state assessments")
            print("toetsresultaten    : test/evaluation results")
            print("monitoringlocaties : monitoring locations")
            print("waterlichamen      : water bodies")
            print("maatlatten         : assessment criteria/standards")
            
        # Check if it's a theme name
        elif name in WKP_DATA['themes']:
            theme_id = WKP_DATA['themes'][name]
            subjects = {sid: subj for sid, subj in WKP_DATA['subjects'].items() if subj['theme'] == name}
            print(f"Theme {theme_id}: {name}")
            print(f"Description: Water quality data theme")
            print(f"Subjects: {len(subjects)}")
            print("\nSubjects:")
            for sid, subject in sorted(subjects.items(), key=lambda x: int(x[0])):
                code = subject.get('subject', 'N/A')
                print(f"  {code}({sid}): {subject['name']}")
        
        # Check if it's a subject code
        else:
            found_subjects = []
            for sid, subject in WKP_DATA['subjects'].items():
                if subject.get('subject', '').upper() == name.upper():
                    found_subjects.append((int(sid), sid, subject))
            
            if found_subjects:
                # Show subject details
                for _, sid, subject in sorted(found_subjects):
                    print(f"Subject Code: {subject.get('subject', 'N/A')}")
                    print(f"ID: {sid}")
                    print(f"Name: {subject['name']}")
                    print(f"Theme: {subject['theme']}")
                    print(f"Area Level Type: {subject['areaLevel']}")
                    print(f"Years: {len(subject['years'])} available ({min(subject['years'])}-{max(subject['years'])})")
                    break  # Only show first match for brevity
            else:
                print(f"No information found for '{name}'")
                print("\nTry one of these:")
                print("  Area levels: nederland, stroomgebieddistricten, provincie, waterbeheerder")
                print("  Area types: NSPW, NW, NSW, PW, N, W")
                print("  General: themes, arealevels, years, labels")
                print("  Themes: Kaderrichtlijn Water, Ecologie, Oppervlaktewaterkwaliteit, Grondwaterkwaliteit")
                print("  Subject codes: OKME, KWSG, ECMF, etc.")


def main():
    parser = argparse.ArgumentParser(
        description="Download data from RWS Water Quality Portal for multiple years and areas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s help                               # Show getting started guide
  %(prog)s subjects                           # List all available subjects
  %(prog)s subjects OKME KWSG                 # Show details for specific codes
  
  %(prog)s dl KWSG KWOG OKME                  # Download subjects by code
  %(prog)s dl 1 5 15                         # Download subjects by ID
  %(prog)s dl "KW*"                          # Download all KRW subjects
  %(prog)s dl --years 2020-2023 --theme "Kaderrichtlijn Water" "KW*"
  %(prog)s dl --limit 5 --dry-run OKME       # Show first 5 requests for OKME
  %(prog)s dl --levels waterbeheerder --names "*Rijnland*" KWMO
  %(prog)s dl -d ./my_data OKME               # Download to specific directory
  %(prog)s dl -f KWSG                        # Force overwrite existing files
  
  %(prog)s info nederland                     # Show nederland area level details
  %(prog)s info NSPW                          # Show NSPW area type combination
  %(prog)s info waterbeheerder                # List all water authorities
  %(prog)s info themes                        # Show all data themes
  %(prog)s info labels                        # Show all abbreviations and terms
  %(prog)s info OKME                          # Show details for subject code OKME
        """
    )
    
    # Add subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Subjects subcommand - for listing and browsing subjects
    subjects_parser = subparsers.add_parser('subjects', help='List and browse available subjects')
    subjects_parser.add_argument("codes", nargs="*", 
                               help="Subject codes to show details for (e.g., OKME KWSG)")
    
    # Help subcommand - for showing getting started guide
    help_parser = subparsers.add_parser('help', help='Show getting started guide and common usage patterns')
    
    # Info subcommand - for showing detailed information
    info_parser = subparsers.add_parser('info', help='Show detailed information about data components')
    info_parser.add_argument("name", 
                           help="Name to look up (e.g., 'nederland', 'NSPW', 'themes', 'waterbeheerder', 'OKME')")
    
    # Download subcommand - for downloading data
    dl_parser = subparsers.add_parser('dl', 
                                     help='Download subject data (use "rws.py dl --help" for all options)',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    dl_parser.add_argument("subjects", nargs="+", 
                          help="Subject codes or IDs (e.g., 'KWSG', 'OKME', '15') or patterns (supports globbing, e.g., 'KW*', '1*')")
    dl_parser.add_argument("--theme", 
                          choices=list(WKP_DATA["themes"].keys()),
                          help="Filter by theme name")
    dl_parser.add_argument("--years", 
                          help="Year range (e.g., '2020-2023') or comma-separated list")
    dl_parser.add_argument("--levels", nargs="+", 
                          choices=["nederland", "stroomgebieddistricten", "provincie", "waterbeheerder"],
                          help="Filter by area levels")
    dl_parser.add_argument("--names", nargs="+",
                          help="Filter area names (supports globbing)")
    dl_parser.add_argument("--directory", "-d", default="./csvs",
                          help="Output directory for extracted files (default: ./csvs)")
    dl_parser.add_argument("--force", "-f", action="store_true",
                          help="Force overwrite existing files")
    dl_parser.add_argument("--limit", type=int,
                          help="Limit number of requests for testing")
    dl_parser.add_argument("--dry-run", action="store_true",
                          help="Show requests without downloading")
    dl_parser.add_argument("--verbose", "-v", action="store_true",
                          help="Show detailed HTTP request information with --dry-run")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    downloader = WKPDownloader()
    
    if args.command == 'subjects':
        if args.codes:
            downloader.show_subject_details(args.codes)
        else:
            print("Available Subjects")
            print()
            
            print(f"{'Code':<10} {'Name':<45} {'Area Type':<20}")
            print(f"{'-'*10:<10} {'-'*45:<45} {'-'*20:<20}")
            
            all_subjects = []
            for sid, subject in WKP_DATA["subjects"].items():
                all_subjects.append((int(sid), sid, subject))
            
            for _, sid, subject in sorted(all_subjects):
                code_with_id = f"{subject['subject']}({sid})"
                display_name = subject['name'][:43] + '..' if len(subject['name']) > 45 else subject['name']
                
                print(f"{code_with_id:<10} {display_name:<45} {subject['areaLevel']:<20}")
            
            print()
    
    elif args.command == 'help':
        print("RWS Water Quality Portal Downloader - Getting Started")
        print("=" * 55)
        print()
        print("This tool downloads water quality data from the Dutch RWS Water Quality Portal.")
        print("Data is organized by subjects, years, and geographical areas.")
        print()
        print("🔍 DISCOVERING DATA")
        print("  rws.py subjects                    # List all 28 available subjects")
        print("  rws.py subjects | grep OKME        # Find surface water quality data")
        print("  rws.py subjects | grep KRW         # Find Water Framework Directive data")
        print("  rws.py subjects OKME               # Show details for specific subject")
        print()
        print("📋 GETTING INFORMATION")
        print("  rws.py info waterbeheerder         # List all 22 water authorities")
        print("  rws.py info themes                 # Show data categories")
        print("  rws.py info labels                 # Explain abbreviations and terms")
        print("  rws.py info nederland              # Show area level details")
        print()
        print("💾 DOWNLOADING DATA")
        print("  rws.py dl OKME --years 2024        # Download latest surface water data")
        print("  rws.py dl KWSG                     # Download KRW stream basin geometry")
        print("  rws.py dl 'KW*' --limit 5 --dry-run  # Preview KRW downloads")
        print("  rws.py dl OKME --levels waterbeheerder --names '*Rijnland*'")
        print("  rws.py dl OKME -d ./data --years 2020-2023")
        print("  rws.py dl OKME -f                  # Force overwrite existing files")
        print()
        print("📋 DOWNLOAD OPTIONS")
        print("  --theme THEME        Filter by data theme (Kaderrichtlijn Water, Ecologie, etc.)")
        print("  --years YEARS        Year range like '2020-2023' or list like '2020,2021,2024'")
        print("  --levels LEVELS      Area levels: nederland, stroomgebieddistricten, provincie, waterbeheerder")
        print("  --names NAMES        Filter area names with globbing: '*Rijnland*', 'Waterschap*'")
        print("  -d, --directory DIR  Output directory (default: ./csvs)")
        print("  -f, --force          Force overwrite existing files")
        print("  --limit LIMIT        Limit requests for testing (useful with large datasets)")
        print("  --dry-run            Preview requests without downloading")
        print("  -v, --verbose        Show detailed HTTP info (use with --dry-run)")
        print()
        print("🏗️  COMMON PATTERNS")
        print("  • Surface water quality: OKME (subject 15) - 16 years, waterbeheerder level")
        print("  • Groundwater quality: GKME (subject 16) - 100+ years, waterbeheerder level")
        print("  • KRW compliance data: KW* subjects - various years and area levels")
        print("  • Use --dry-run to preview requests before downloading")
        print("  • Use --limit to test with smaller datasets first")
        print("  • Pipe subjects output to grep for filtering")
        print()
        print("💡 TIPS")
        print("  • Data files include both CSV measurements and PDF documentation")
        print("  • Area levels: nederland (1), waterbeheerder (22), provincie (12), stroomgebied (4)")
        print("  • Years range from 1909 (groundwater) to 2024-2025 (surface water)")
        print("  • Use quotes for patterns with special characters: 'KW*', '*Rijnland*'")
        print("  • Check file sizes - some downloads can be 100+ MB per request")
        print()
    
    # Handle info subcommand
    elif args.command == 'info':
        downloader.show_info(args.name)
    
    # Handle download subcommand  
    elif args.command == 'dl':
        # Parse year filter
        year_filter = None
        if args.years:
            if '-' in args.years:
                start, end = map(int, args.years.split('-'))
                year_filter = list(range(start, end + 1))
            else:
                year_filter = [int(y.strip()) for y in args.years.split(',')]
        
        # Get matching subjects
        subject_ids = downloader.get_matching_subjects(args.subjects)
        if not subject_ids:
            print("Error: No subjects matched the given patterns.")
            return
        
        # Filter by theme if specified
        if args.theme:
            subject_ids = [sid for sid in subject_ids 
                          if WKP_DATA["subjects"][sid]["theme"] == args.theme]
        
        if not subject_ids:
            print(f"Error: No subjects found for theme {args.theme}")
            return
        
        print(f"Selected subjects: {', '.join(subject_ids)}")
        
        # Expand to requests
        requests = downloader.expand_requests(
            subject_ids, 
            year_filter=year_filter,
            level_filter=args.levels,
            name_filter=args.names
        )
        
        if not requests:
            print("No requests generated with the given filters.")
            return
        
        # Download
        downloader.download_data(
            requests,
            output_dir=args.directory,
            limit=args.limit,
            dry_run=args.dry_run,
            verbose=getattr(args, 'verbose', False),
            force=args.force
        )

if __name__ == "__main__":
    main()