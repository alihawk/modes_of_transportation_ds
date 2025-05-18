#!/bin/bash

echo "Downloading Slovenian railway data in GeoJSON format..."

# Define the Overpass API URL
OVERPASS_URL="https://overpass-api.de/api/interpreter"

# Define the Overpass query for railways
QUERY='[out:json];
area["name"="Slovenija"]->.searchArea;
(
  way["railway"](area.searchArea);
  relation["railway"](area.searchArea);
);
out body;
>;
out skel qt;'

# Create output filename
OUTPUT_FILE="slovenia_railways.geojson"

# Make the request and save the response
curl -X POST "$OVERPASS_URL" --data-urlencode "data=$QUERY" -o "$OUTPUT_FILE"

# Check if the request was successful
if [ $? -eq 0 ] && [ -s "$OUTPUT_FILE" ]; then
    echo "Railway data successfully downloaded to $OUTPUT_FILE"
else
    echo "Error downloading railway data"
    exit 1
fi