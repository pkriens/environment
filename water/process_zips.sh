#!/bin/bash

# Script om alle zip bestanden uit te pakken en CSV bestanden te kopiëren naar csvs directory
# Verwerkt alle zip bestanden in de zips/ directory

zips_dir="zips"
csvs_dir="csvs"
temp_dir="temp_unzip"

# Controleer of zips directory bestaat
if [[ ! -d "$zips_dir" ]]; then
    echo "Error: $zips_dir directory niet gevonden!"
    exit 1
fi

# Maak directories aan als ze niet bestaan
mkdir -p "$csvs_dir"
mkdir -p "$temp_dir"

echo "Start verwerken van zip bestanden in $zips_dir/..."

# Tel het aantal zip bestanden
zip_count=$(find "$zips_dir" -name "*.zip" | wc -l)
echo "Gevonden $zip_count zip bestanden om te verwerken"

if [[ $zip_count -eq 0 ]]; then
    echo "Geen zip bestanden gevonden in $zips_dir/"
    exit 0
fi

# Verwerk alle zip bestanden
processed=0
for zipfile in "$zips_dir"/*.zip; do
    if [[ -f "$zipfile" ]]; then
        ((processed++))
        zip_basename=$(basename "$zipfile")
        echo "[$processed/$zip_count] Verwerken: $zip_basename"
        
        # Maak unieke tijdelijke directory
        zip_name_no_ext="${zip_basename%.zip}"
        unzip_dir="${temp_dir}/${zip_name_no_ext}"
        mkdir -p "$unzip_dir"
        
        # Pak zip bestand uit
        if unzip -q "$zipfile" -d "$unzip_dir"; then
            echo "  📦 Uitgepakt: $zip_basename"
            
            # Zoek en kopieer CSV bestanden
            csv_count=0
            while IFS= read -r -d '' csv_file; do
                if [[ -f "$csv_file" ]]; then
                    # Gebruik gewoon de originele CSV naam (zonder zip naam prefix)
                    csv_basename=$(basename "$csv_file")
                    safe_csv_name=$(echo "$csv_basename" | tr ' ' '_' | tr -cd '[:alnum:]_.')
                    
                    # Gebruik alleen de CSV naam (geen zip prefix nodig)
                    final_csv_name="${safe_csv_name}"
                    
                    # Kopieer CSV bestand
                    if cp "$csv_file" "${csvs_dir}/${final_csv_name}"; then
                        echo "    📄 CSV gekopieerd: $final_csv_name"
                        ((csv_count++))
                    else
                        echo "    ✗ Fout bij kopiëren van: $csv_basename"
                    fi
                fi
            done < <(find "$unzip_dir" -name "*.csv" -type f -print0)
            
            if [[ $csv_count -eq 0 ]]; then
                echo "    ⚠ Geen CSV bestanden gevonden in $zip_basename"
            else
                echo "    ✓ $csv_count CSV bestanden verwerkt"
            fi
            
            # Ruim tijdelijke unzip directory op
            rm -rf "$unzip_dir"
        else
            echo "    ✗ Fout bij uitpakken van: $zip_basename"
        fi
    fi
done

# Ruim de tijdelijke directory op
rm -rf "$temp_dir"

echo ""
echo "=== Samenvatting ==="
echo "Verwerkte zip bestanden: $processed"
final_csv_count=$(find "$csvs_dir" -name "*.csv" | wc -l)
echo "Totaal CSV bestanden in $csvs_dir: $final_csv_count"
echo "Klaar met verwerken!"