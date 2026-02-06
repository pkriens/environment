# WKP Portal CSV data to geojson file for viewer

The python script converts csv files downloaded from the RWS WKP portal into geojson files that can be used in the viewer. The script is designed to be flexible and can handle different types of data, as long as the csv files have the required columns.

## Usage

First download the csv files from the RWS WKP portal using the provided s
cripts in `scripts/rws`. You need to downoad the OKME subject. This downloads
the CSV files for meetobjecten en meetwaardes.
Then you can run the conversion script.

```bash
rws subjects
...
rws dl OKME -o csvs
python csv_to_geojson.py --input csvs --output geojsons.json
```
 Downloading all csvs files will result in about 4,5 Gb of the geojson file.
 You can limit the years and authorities in the `rws` command to 
 minimize the data.
