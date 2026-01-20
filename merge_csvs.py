#!/usr/bin/env python3
"""
Script om alle CSV bestanden uit de csvs/ directory te mergen in één groot CSV bestand
Handelt verschillende kolommen af door alle kolommen te combineren
"""

import pandas as pd
import os
import glob
from pathlib import Path

def merge_all_csvs(csvs_dir="csvs"):
    """Merge alle CSV bestanden in de opgegeven directory"""
    
    output_file = f"merged_{os.path.basename(csvs_dir)}_data.csv"
    
    # Controleer of csvs directory bestaat
    if not os.path.exists(csvs_dir):
        print(f"Error: {csvs_dir} directory niet gevonden!")
        return
    
    # Zoek alle CSV bestanden
    csv_files = glob.glob(os.path.join(csvs_dir, "*.csv"))
    
    if not csv_files:
        print(f"Geen CSV bestanden gevonden in {csvs_dir}/")
        return
    
    print(f"Gevonden {len(csv_files)} CSV bestanden om te mergen...")
    
    # Lijst om alle DataFrames op te slaan
    dataframes = []
    all_columns = set()
    
    # Eerste ronde: lees alle bestanden en verzamel alle kolommen
    print("\nStap 1: Analyseren van alle bestanden...")
    file_info = []
    
    for i, csv_file in enumerate(csv_files, 1):
        try:
            print(f"  [{i}/{len(csv_files)}] Analyseren: {os.path.basename(csv_file)}")
            
            # Lees CSV bestand
            df = pd.read_csv(csv_file, low_memory=False)
            
            # Voeg source kolom toe om te weten uit welk bestand elke rij komt
            source_name = os.path.basename(csv_file).replace('.csv', '')
            df['source_file'] = source_name
            
            # Verzamel kolommen
            file_columns = set(df.columns)
            all_columns.update(file_columns)
            
            # Bewaar info
            file_info.append({
                'file': csv_file,
                'dataframe': df,
                'columns': file_columns,
                'rows': len(df)
            })
            
            print(f"    - {len(df)} rijen, {len(file_columns)} kolommen")
            
        except Exception as e:
            print(f"    ✗ Fout bij lezen van {csv_file}: {e}")
            continue
    
    if not file_info:
        print("Geen geldige CSV bestanden gevonden!")
        return
    
    print(f"\nStap 2: Mergen van {len(file_info)} bestanden...")
    print(f"Totaal aantal unieke kolommen: {len(all_columns)}")
    
    # Tweede ronde: normaliseer alle DataFrames naar dezelfde kolommen
    for i, info in enumerate(file_info, 1):
        df = info['dataframe']
        file_columns = info['columns']
        missing_columns = all_columns - file_columns
        
        if missing_columns:
            print(f"  [{i}/{len(file_info)}] {os.path.basename(info['file'])}: +{len(missing_columns)} ontbrekende kolommen")
            # Voeg ontbrekende kolommen toe met NaN waarden
            for col in missing_columns:
                df[col] = pd.NA
        else:
            print(f"  [{i}/{len(file_info)}] {os.path.basename(info['file'])}: alle kolommen aanwezig")
        
        dataframes.append(df)
    
    # Merge alle DataFrames
    print(f"\nStap 3: Samenvoegen van alle data...")
    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
    
    # Sorteer kolommen alfabetisch (behalve source_file als laatste)
    columns_sorted = sorted([col for col in merged_df.columns if col != 'source_file'])
    if 'source_file' in merged_df.columns:
        columns_sorted.append('source_file')
    merged_df = merged_df[columns_sorted]
    
    # Statistieken
    total_rows = len(merged_df)
    total_columns = len(merged_df.columns)
    
    print(f"\nStap 4: Opslaan naar {output_file}...")
    
    # Schrijf naar CSV
    merged_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # Resultaat samenvatting
    print(f"\n=== Merge Resultaat ===")
    print(f"Input bestanden: {len(file_info)}")
    print(f"Totaal rijen: {total_rows:,}")
    print(f"Totaal kolommen: {total_columns}")
    print(f"Output bestand: {output_file}")
    
    # Toon verdeling per bron
    print(f"\n=== Verdeling per bron (top 10) ===")
    if 'source_file' in merged_df.columns:
        source_counts = merged_df['source_file'].value_counts().head(10)
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} rijen")
    
    # Toon eerste paar kolommen als voorbeeld
    print(f"\n=== Voorbeeld kolommen ===")
    for i, col in enumerate(merged_df.columns[:10]):
        non_null_count = merged_df[col].notna().sum()
        percentage = (non_null_count / total_rows) * 100
        print(f"  {col}: {non_null_count:,}/{total_rows:,} gevuld ({percentage:.1f}%)")
    
    if len(merged_df.columns) > 10:
        print(f"  ... en {len(merged_df.columns) - 10} meer kolommen")
    
    print(f"\n✓ Klaar! Merged bestand opgeslagen als: {output_file}")

def show_csv_overview(csvs_dir="csvs"):
    """Toon overzicht van alle CSV bestanden zonder te mergen"""
    
    if not os.path.exists(csvs_dir):
        print(f"Error: {csvs_dir} directory niet gevonden!")
        return
    
    csv_files = glob.glob(os.path.join(csvs_dir, "*.csv"))
    
    if not csv_files:
        print(f"Geen CSV bestanden gevonden in {csvs_dir}/")
        return
    
    print(f"=== Overzicht van {len(csv_files)} CSV bestanden ===")
    
    all_columns = set()
    total_rows = 0
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            file_columns = set(df.columns)
            all_columns.update(file_columns)
            total_rows += len(df)
            
            print(f"{os.path.basename(csv_file)}: {len(df):,} rijen, {len(file_columns)} kolommen")
            
        except Exception as e:
            print(f"{os.path.basename(csv_file)}: FOUT - {e}")
    
    print(f"\nTotaal: {total_rows:,} rijen, {len(all_columns)} unieke kolommen")

if __name__ == "__main__":
    import sys
    
    # Parse command line argumenten
    csvs_dir = "csvs"  # default
    show_overview = False
    
    # Help functie
    def show_help():
        print("Gebruik:")
        print(f"  {sys.argv[0]} [directory] [overview]")
        print("")
        print("Argumenten:")
        print("  directory  - Pad naar de CSV directory (default: csvs)")
        print("  overview   - Toon alleen overzicht, merge niet")
        print("")
        print("Voorbeelden:")
        print(f"  {sys.argv[0]}              # Merge csvs/ directory")
        print(f"  {sys.argv[0]} data/        # Merge data/ directory")
        print(f"  {sys.argv[0]} csvs overview  # Toon overzicht van csvs/")
        print(f"  {sys.argv[0]} overview     # Toon overzicht van csvs/ (default)")
    
    # Parse argumenten
    if len(sys.argv) > 1:
        if sys.argv[1] in ["-h", "--help", "help"]:
            show_help()
            sys.exit(0)
        elif sys.argv[1] == "overview":
            show_overview = True
        elif os.path.exists(sys.argv[1]) or sys.argv[1].endswith('/'):
            csvs_dir = sys.argv[1].rstrip('/')
            # Check voor overview als tweede argument
            if len(sys.argv) > 2 and sys.argv[2] == "overview":
                show_overview = True
        else:
            print(f"Error: Directory '{sys.argv[1]}' niet gevonden!")
            print(f"Gebruik '{sys.argv[0]} help' voor meer informatie.")
            sys.exit(1)
    
    # Check voor overview als tweede argument bij default directory
    if len(sys.argv) > 2 and sys.argv[2] == "overview":
        show_overview = True
    
    print(f"Directory: {csvs_dir}")
    print(f"Mode: {'Overview' if show_overview else 'Merge'}")
    print("")
    
    if show_overview:
        show_csv_overview(csvs_dir)
    else:
        merge_all_csvs(csvs_dir)