#!/bin/bash

# Script om zip files op te halen voor alle waterschappen
# Leest waterschappen uit waterschappen.txt en voert curl uit voor elk waterschap en elk jaar (2009-2024)

input_file="waterschappen.txt"
zips_dir="zips"

if [[ ! -f "$input_file" ]]; then
    echo "Error: $input_file niet gevonden!"
    exit 1
fi

# Maak zips directory aan als die niet bestaat
mkdir -p "$zips_dir"

echo "Start downloaden van zip files voor alle waterschappen (2009-2024)..."

# Lees elke regel van het bestand
while IFS= read -r waterschap; do
    # Skip lege regels
    if [[ -z "$waterschap" ]]; then
        continue
    fi
    
    echo "=== Downloaden voor waterschap: $waterschap ==="
    
    # Loop door alle jaren van 2009 tot 2024
    for jaar in {2009..2024}; do
        # Maak een veilige filename door spaties en speciale tekens te vervangen
        safe_name=$(echo "$waterschap" | tr ' ' '_' | tr -cd '[:alnum:]_')
        output_file="${zips_dir}/${safe_name}_${jaar}.zip"
        
        echo "  Downloaden jaar $jaar -> $(basename "$output_file")"
        
        # Voer curl uit met de huidige waterschap naam en jaar
        curl 'https://wkp.rws.nl/api/v1/data-downloads/download' \
            -X POST \
            -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0' \
            -H 'Accept: application/zip' \
            -H 'Accept-Language: en-US,en;q=0.5' \
            -H 'Accept-Encoding: gzip, deflate, br, zstd' \
            -H 'Content-Type: application/json' \
            -H 'Origin: https://wkp.rws.nl' \
            -H 'DNT: 1' \
            -H 'Connection: keep-alive' \
            -H 'Referer: https://wkp.rws.nl/downloadmodule' \
            -H 'Sec-Fetch-Dest: empty' \
            -H 'Sec-Fetch-Mode: cors' \
            -H 'Sec-Fetch-Site: same-origin' \
            -H 'Priority: u=0' \
            -H 'Pragma: no-cache' \
            -H 'Cache-Control: no-cache' \
            --data-raw "[{\"subjectId\":15,\"year\":$jaar,\"areaLevel\":\"waterbeheerder\",\"areaName\":\"$waterschap\"}]" \
            --output "$output_file" \
            --silent
        
        # Controleer of download succesvol was
        if [[ $? -eq 0 ]]; then
            # Controleer of het bestand groter is dan 1KB (anders waarschijnlijk leeg/fout)
            if [[ -s "$output_file" ]] && [[ $(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null) -gt 1024 ]]; then
                echo "    ✓ Succesvol gedownload: $(basename "$output_file")"
            else
                echo "    ⚠ Bestand te klein of leeg, mogelijk geen data voor $jaar: $(basename "$output_file")"
                # Verwijder lege bestanden
                rm -f "$output_file"
            fi
        else
            echo "    ✗ Fout bij downloaden van: $waterschap ($jaar)"
        fi
        
        # Korte pauze tussen requests om server niet te overbelasten
        sleep 2
    done
    
done < "$input_file"

echo "Klaar met downloaden! Zip bestanden opgeslagen in $zips_dir/"

echo "Klaar met downloaden!"