#!/usr/bin/env python3
"""
CSV naar GeoJSON Converter voor Waterkwaliteitsportaal data
Versie 8.0 - Alle parameters opslaan voor dynamische viewer

Gebruik:
    python csv_to_geojson_v8.py --input <csv_map> --output <output.geojson>
"""

import csv
import json
import os
import sys
import argparse
import urllib.request
import urllib.error
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Constanten
WFS_URL = "https://geo.rijkswaterstaat.nl/services/ogc/wkp/wkp/wfs"
WFS_PARAMS = "service=WFS&version=2.0.0&request=GetFeature&typeName=wkp:LEWmeetobjecten&outputFormat=application/json"
CACHE_FILE = "lew_locaties_cache.json"

# Bekende bestrijdingsmiddelen codes (voor categorisatie)
BESTRIJDINGSMIDDELEN_CODES = {
    'glyfst', 'imdcpd', 'MCPA', 'SmtlCl', 'metlCl', 'atzne', 'simzne', 'Durn',
    'bentzn', 'Clidzn', 'metbzn', 'iptrn', 'AMPA', '24D', '24DB', '24DP',
    'desiC3yatzne', 'desC2yatzne', 'BrOxnl', 'Brpplt', 'C1oxfnzde', 'C1yBrfs',
    # Voeg meer codes toe indien nodig
}

# Parameter categorieën
PARAMETER_CATEGORIES = {
    'nutrienten': {
        'codes': {'NO3', 'Ntot', 'N', 'NH4', 'NH3', 'NKj', 'sNO3NO2', 'NO2', 
                  'PO4', 'Ptot', 'P'},
        'label': 'Nutriënten'
    },
    'zuurstof': {
        'codes': {'O2', 'OS', 'BZV5a'},
        'label': 'Zuurstof'
    },
    'metalen': {
        'codes': {'Fe', 'FEOa', 'Al', 'As', 'Ba', 'Be', 'Ca', 'Mg', 'Ag', 'B'},
        'label': 'Metalen'
    },
    'organisch': {
        'codes': {'Corg', 'CHLFa'},
        'label': 'Organisch'
    }
}

# RD naar WGS84 transformatie parameters
X0 = 155000
Y0 = 463000
PHI0 = 52.15517440
LAM0 = 5.38720621

Kp = [0, 2, 0, 2, 0, 2, 1, 4, 2, 4, 1]
Kq = [1, 0, 2, 1, 3, 2, 0, 0, 3, 1, 1]
Kpq = [3235.65389, -32.58297, -0.24750, -0.84978, -0.06550, -0.01709, -0.00738, 0.00530, -0.00039, 0.00033, -0.00012]

Lp = [1, 1, 1, 3, 1, 3, 0, 3, 1, 0, 2, 5]
Lq = [0, 1, 2, 0, 3, 1, 1, 2, 4, 2, 0, 0]
Lpq = [5260.52916, 105.94684, 2.45656, -0.81885, 0.05594, -0.05607, 0.01199, -0.00256, 0.00128, 0.00022, -0.00022, 0.00026]


def rd_to_wgs84(x, y):
    """Converteer RD-coördinaten naar WGS84 (lat, lon)."""
    dx = (x - X0) * 1e-5
    dy = (y - Y0) * 1e-5
    
    phi = PHI0
    for i in range(len(Kp)):
        phi += Kpq[i] * (dx ** Kp[i]) * (dy ** Kq[i]) / 3600
    
    lam = LAM0
    for i in range(len(Lp)):
        lam += Lpq[i] * (dx ** Lp[i]) * (dy ** Lq[i]) / 3600
    
    return phi, lam


def normalize_meetobject_code(code):
    """Normaliseer meetobjectcode door jaarsuffix te verwijderen."""
    if not code:
        return code
    match = re.match(r'^(NL\d+_.+?)_(\d{4})$', code)
    if match:
        return match.group(1)
    return code


def is_bestrijdingsmiddel(param_code, eenheid):
    """Bepaal of een parameter een bestrijdingsmiddel is."""
    if param_code in BESTRIJDINGSMIDDELEN_CODES:
        return True
    # Veel bestrijdingsmiddelen worden gemeten in µg/L
    if eenheid and 'µg' in eenheid.lower():
        return True
    return False


def get_parameter_category(param_code):
    """Bepaal de categorie van een parameter."""
    for cat_name, cat_info in PARAMETER_CATEGORIES.items():
        if param_code in cat_info['codes']:
            return cat_name
    return 'overig'


def download_lew_locaties(cache_path):
    """Download LEW meetlocaties van de RWS GeoServer WFS service."""
    print("Downloaden van meetlocaties van RWS GeoServer...")
    print("Dit kan enkele minuten duren...")
    
    url = f"{WFS_URL}?{WFS_PARAMS}"
    
    try:
        with urllib.request.urlopen(url, timeout=300) as response:
            data = json.loads(response.read().decode('utf-8'))
    except urllib.error.URLError as e:
        print(f"Fout bij downloaden: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Fout bij parsen van JSON: {e}")
        return None
    
    features = data.get('features', [])
    print(f"Ontvangen: {len(features)} meetlocaties")
    
    locaties = {}
    for feature in features:
        props = feature.get('properties', {})
        code = props.get('MeetobjectCode', '')
        if code:
            geom = feature.get('geometry', {})
            coords = geom.get('coordinates', [])
            if len(coords) >= 2:
                lat, lon = rd_to_wgs84(coords[0], coords[1])
                locaties[code] = {
                    'x_rd': coords[0],
                    'y_rd': coords[1],
                    'lat': lat,
                    'lon': lon,
                    'omschrijving': props.get('Omschrijving', ''),
                    'waterbeheerder': props.get('WaterbeheerderNaam', ''),
                    'watertype': props.get('KRWWatertypeOmschrijving', '')
                }
    
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'count': len(locaties),
        'locaties': locaties
    }
    
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, ensure_ascii=False)
    
    print(f"Cache opgeslagen: {cache_path} ({len(locaties)} locaties)")
    return locaties


def load_lew_locaties(cache_path, force_update=False):
    """Laad LEW locaties uit cache of download opnieuw."""
    if not force_update and os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            timestamp = cache_data.get('timestamp', 'onbekend')
            locaties = cache_data.get('locaties', {})
            print(f"Locatie-cache geladen: {len(locaties)} locaties (van {timestamp})")
            return locaties
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Cache beschadigd, opnieuw downloaden: {e}")
    
    return download_lew_locaties(cache_path)


def parse_csv_files(input_path, locaties):
    """Parse alle CSV-bestanden en verzamel alle parameters."""
    csv_files = []
    
    if os.path.isfile(input_path):
        csv_files = [input_path]
    elif os.path.isdir(input_path):
        csv_files = list(Path(input_path).glob('*.csv'))
        csv_files.extend(Path(input_path).glob('*.CSV'))
    
    if not csv_files:
        print(f"Geen CSV-bestanden gevonden in: {input_path}")
        return {}, {}
    
    print(f"Gevonden: {len(csv_files)} CSV-bestand(en)")
    
    meetpunten = {}
    alle_parameters = {}  # code -> {naam, eenheid, categorie, is_bestrijdingsmiddel}
    total_rows = 0
    matched_rows = 0
    
    for csv_file in csv_files:
        print(f"  Verwerken: {os.path.basename(csv_file)}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                
                for row in reader:
                    total_rows += 1
                    
                    raw_code = row.get('MeetobjectCode', '').strip()
                    if not raw_code:
                        continue
                    
                    meetobject_code = normalize_meetobject_code(raw_code)
                    
                    if meetobject_code not in meetpunten:
                        meetpunten[meetobject_code] = {
                            'code': meetobject_code,
                            'raw_codes': set(),
                            'namespace': row.get('MeetobjectNamespace', ''),
                            'waterbeheerder': row.get('WaterbeheerderNaam', ''),
                            'metingen': [],  # Alle metingen
                            'parameters': defaultdict(list),  # Per parameter
                            'jaren': set(),
                            'x_rd': None,
                            'y_rd': None
                        }
                        
                        try:
                            x_rd = row.get('GeometriePuntX_RD', '')
                            y_rd = row.get('GeometriePuntY_RD', '')
                            if x_rd and y_rd:
                                meetpunten[meetobject_code]['x_rd'] = float(x_rd.replace(',', '.'))
                                meetpunten[meetobject_code]['y_rd'] = float(y_rd.replace(',', '.'))
                        except (ValueError, TypeError):
                            pass
                    
                    meetpunten[meetobject_code]['raw_codes'].add(raw_code)
                    
                    try:
                        waarde = row.get('Numeriekewaarde', '')
                        if waarde:
                            matched_rows += 1
                            
                            param_code = row.get('ParameterCode', '')
                            param_naam = row.get('ParameterOmschrijving', '') or row.get('Grootheid', '') or param_code
                            eenheid = row.get('Eenheid', '') or row.get('EenheidCode', '')
                            datum = row.get('Monsterophaaldatum', '') or row.get('Begindatum', '')
                            jaar = row.get('Meetjaar', '')
                            if not jaar and datum and len(datum) >= 4:
                                jaar = datum[:4]
                            
                            # Voeg jaar toe
                            if jaar:
                                meetpunten[meetobject_code]['jaren'].add(jaar)
                            
                            # Registreer parameter info
                            if param_code and param_code not in alle_parameters:
                                is_bestr = is_bestrijdingsmiddel(param_code, eenheid)
                                alle_parameters[param_code] = {
                                    'code': param_code,
                                    'naam': param_naam,
                                    'eenheid': eenheid,
                                    'categorie': 'bestrijdingsmiddelen' if is_bestr else get_parameter_category(param_code),
                                    'is_bestrijdingsmiddel': is_bestr
                                }
                            
                            meting = {
                                'parameter': param_code,
                                'parameter_naam': param_naam,
                                'waarde': float(waarde.replace(',', '.')),
                                'eenheid': eenheid,
                                'datum': datum,
                                'jaar': jaar
                            }
                            
                            meetpunten[meetobject_code]['metingen'].append(meting)
                            meetpunten[meetobject_code]['parameters'][param_code].append(meting)
                            
                    except (ValueError, TypeError):
                        pass
                        
        except Exception as e:
            print(f"    Fout bij verwerken: {e}")
    
    print(f"\n  Totaal rijen verwerkt: {total_rows}")
    print(f"  Rijen met numerieke waarde: {matched_rows}")
    print(f"  Unieke parameters: {len(alle_parameters)}")
    
    # Tel bestrijdingsmiddelen
    bestr_count = sum(1 for p in alle_parameters.values() if p['is_bestrijdingsmiddel'])
    print(f"  Waarvan bestrijdingsmiddelen: {bestr_count}")
    
    return meetpunten, alle_parameters


def create_geojson(meetpunten, locaties, alle_parameters, output_path):
    """Maak GeoJSON bestand met alle parameters."""
    features = []
    stats = {
        'totaal': len(meetpunten),
        'met_locatie': 0,
        'zonder_locatie': 0,
        'locatie_uit_csv': 0,
        'locatie_uit_wfs': 0,
        'totaal_metingen': 0
    }
    
    for code, mp in meetpunten.items():
        lat, lon = None, None
        locatie_bron = 'onbekend'
        
        if mp['x_rd'] and mp['y_rd']:
            lat, lon = rd_to_wgs84(mp['x_rd'], mp['y_rd'])
            locatie_bron = 'csv'
            stats['locatie_uit_csv'] += 1
        elif code in locaties:
            loc = locaties[code]
            lat, lon = loc['lat'], loc['lon']
            mp['x_rd'], mp['y_rd'] = loc['x_rd'], loc['y_rd']
            locatie_bron = 'wfs'
            stats['locatie_uit_wfs'] += 1
            if not mp['waterbeheerder'] and loc.get('waterbeheerder'):
                mp['waterbeheerder'] = loc['waterbeheerder']
        else:
            for raw_code in mp.get('raw_codes', []):
                if raw_code in locaties:
                    loc = locaties[raw_code]
                    lat, lon = loc['lat'], loc['lon']
                    mp['x_rd'], mp['y_rd'] = loc['x_rd'], loc['y_rd']
                    locatie_bron = 'wfs'
                    stats['locatie_uit_wfs'] += 1
                    break
        
        if lat and lon:
            stats['met_locatie'] += 1
        else:
            stats['zonder_locatie'] += 1
            continue
        
        stats['totaal_metingen'] += len(mp['metingen'])
        
        # Bereken statistieken per parameter
        parameter_stats = {}
        tijdreeksen = {}
        
        for param_code, metingen in mp['parameters'].items():
            waarden = [m['waarde'] for m in metingen]
            if waarden:
                parameter_stats[param_code] = {
                    'gemiddelde': sum(waarden) / len(waarden),
                    'maximum': max(waarden),
                    'minimum': min(waarden),
                    'aantal': len(waarden)
                }
                
                # Maak tijdreeks
                tijdreeksen[param_code] = [
                    {'datum': m['datum'], 'waarde': m['waarde'], 'jaar': m['jaar']}
                    for m in sorted(metingen, key=lambda x: x['datum'])
                ]
        
        # Bereken som bestrijdingsmiddelen per datum
        bestr_per_datum = defaultdict(lambda: {'waarden': [], 'jaar': ''})
        for param_code, metingen in mp['parameters'].items():
            if alle_parameters.get(param_code, {}).get('is_bestrijdingsmiddel'):
                for m in metingen:
                    bestr_per_datum[m['datum']]['waarden'].append(m['waarde'])
                    bestr_per_datum[m['datum']]['jaar'] = m['jaar']
        
        # Maak som bestrijdingsmiddelen tijdreeks
        som_bestr_tijdreeks = []
        som_bestr_waarden = []
        for datum, data in sorted(bestr_per_datum.items()):
            som = sum(data['waarden'])
            som_bestr_waarden.append(som)
            som_bestr_tijdreeks.append({
                'datum': datum,
                'waarde': som,
                'jaar': data['jaar'],
                'aantal_stoffen': len(data['waarden'])
            })
        
        # Voeg som bestrijdingsmiddelen toe aan stats
        if som_bestr_waarden:
            parameter_stats['SOM_BESTR'] = {
                'gemiddelde': sum(som_bestr_waarden) / len(som_bestr_waarden),
                'maximum': max(som_bestr_waarden),
                'minimum': min(som_bestr_waarden),
                'aantal': len(som_bestr_waarden)
            }
            tijdreeksen['SOM_BESTR'] = som_bestr_tijdreeks
        
        # Sorteer jaren
        jaren_list = sorted(list(mp['jaren']))
        
        # Maak GeoJSON feature
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [lon, lat]
            },
            'properties': {
                'code': code,
                'namespace': mp['namespace'],
                'waterschap': mp['waterbeheerder'],
                'waterbeheerder': mp['waterbeheerder'],
                'x_rd': mp['x_rd'],
                'y_rd': mp['y_rd'],
                'locatie_bron': locatie_bron,
                'jaren': jaren_list,
                'totaal_metingen': len(mp['metingen']),
                
                # Alle parameter statistieken
                'parameter_stats': parameter_stats,
                
                # Alle tijdreeksen
                'tijdreeksen': tijdreeksen,
                
                # Legacy velden voor backwards compatibility
                'nitraat_gemiddelde': parameter_stats.get('NO3', parameter_stats.get('Ntot', {})).get('gemiddelde', 0),
                'nitraat_maximum': parameter_stats.get('NO3', parameter_stats.get('Ntot', {})).get('maximum', 0),
                'nitraat_aantal': parameter_stats.get('NO3', parameter_stats.get('Ntot', {})).get('aantal', 0),
                'fosfaat_gemiddelde': parameter_stats.get('PO4', parameter_stats.get('Ptot', {})).get('gemiddelde', 0),
                'fosfaat_maximum': parameter_stats.get('PO4', parameter_stats.get('Ptot', {})).get('maximum', 0),
                'fosfaat_aantal': parameter_stats.get('PO4', parameter_stats.get('Ptot', {})).get('aantal', 0),
                'bestrijdingsmiddelen_gemiddelde': parameter_stats.get('SOM_BESTR', {}).get('gemiddelde', 0),
                'bestrijdingsmiddelen_maximum': parameter_stats.get('SOM_BESTR', {}).get('maximum', 0),
                'bestrijdingsmiddelen_aantal': parameter_stats.get('SOM_BESTR', {}).get('aantal', 0),
                
                # Legacy tijdreeksen
                'tijdreeks_nitraat': tijdreeksen.get('NO3', tijdreeksen.get('Ntot', [])),
                'tijdreeks_fosfaat': tijdreeksen.get('PO4', tijdreeksen.get('Ptot', [])),
                'tijdreeks_bestrijdingsmiddelen': tijdreeksen.get('SOM_BESTR', [])
            }
        }
        features.append(feature)
    
    # Maak parameter catalogus
    param_catalogus = {}
    for code, info in alle_parameters.items():
        param_catalogus[code] = {
            'code': code,
            'naam': info['naam'],
            'eenheid': info['eenheid'],
            'categorie': info['categorie'],
            'is_bestrijdingsmiddel': info['is_bestrijdingsmiddel']
        }
    
    # Voeg som bestrijdingsmiddelen toe aan catalogus
    param_catalogus['SOM_BESTR'] = {
        'code': 'SOM_BESTR',
        'naam': 'Som bestrijdingsmiddelen',
        'eenheid': 'µg/L',
        'categorie': 'bestrijdingsmiddelen',
        'is_bestrijdingsmiddel': True,
        'is_som': True
    }
    
    # Maak GeoJSON FeatureCollection
    geojson = {
        'type': 'FeatureCollection',
        'metadata': {
            'gegenereerd': datetime.now().isoformat(),
            'bron': 'Waterkwaliteitsportaal CSV export',
            'locaties_bron': 'RWS GeoServer WFS (LEWmeetobjecten)',
            'statistieken': stats
        },
        'parameters': param_catalogus,
        'features': features
    }
    
    # Schrijf naar bestand
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='Converteer WKP CSV naar GeoJSON')
    parser.add_argument('--input', '-i', required=True, help='Map met CSV-bestanden')
    parser.add_argument('--output', '-o', default='meetpunten.geojson', help='Output bestand')
    parser.add_argument('--update', '-u', action='store_true', help='Update locatie-cache')
    parser.add_argument('--cache', '-c', default=CACHE_FILE, help='Cache bestand')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("WKP CSV naar GeoJSON Converter v8.0")
    print("Alle parameters worden opgeslagen")
    print("=" * 60)
    print()
    
    locaties = load_lew_locaties(args.cache, args.update)
    if not locaties:
        print("FOUT: Kon geen locaties laden")
        sys.exit(1)
    
    print()
    
    meetpunten, alle_parameters = parse_csv_files(args.input, locaties)
    if not meetpunten:
        print("FOUT: Geen meetpunten gevonden")
        sys.exit(1)
    
    print(f"\nTotaal unieke meetpunten: {len(meetpunten)}")
    print()
    
    print("Genereren van GeoJSON...")
    stats = create_geojson(meetpunten, locaties, alle_parameters, args.output)
    
    print()
    print("=" * 60)
    print("RESULTAAT")
    print("=" * 60)
    print(f"Totaal meetpunten:       {stats['totaal']}")
    print(f"Met locatie:             {stats['met_locatie']} ({100*stats['met_locatie']/stats['totaal']:.1f}%)")
    print(f"  - uit CSV:             {stats['locatie_uit_csv']}")
    print(f"  - uit WFS:             {stats['locatie_uit_wfs']}")
    print(f"Zonder locatie:          {stats['zonder_locatie']}")
    print(f"Totaal metingen:         {stats['totaal_metingen']}")
    print(f"Unieke parameters:       {len(alle_parameters)}")
    print()
    print(f"Output: {args.output}")
    print(f"Grootte: {os.path.getsize(args.output) / 1024 / 1024:.1f} MB")


if __name__ == '__main__':
    main()
