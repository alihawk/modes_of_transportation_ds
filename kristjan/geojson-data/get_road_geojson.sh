#!/bin/bash

echo "Downloading Slovenian road data in GeoJSON format..."
# Download Slovenia data
wget https://download.geofabrik.de/europe/slovenia-latest.osm.pbf

echo "Trying to run osmium tool..."
# Install osmium tool if you don't have it (on Manjaro)
sudo apt-get install osmium-tool
# Convert to GeoJSON (roads only)
osmium tags-filter slovenia-latest.osm.pbf w/highway -o slovenia-roads.osm.pbf
osmium export slovenia-roads.osm.pbf -f geojsonseq -o slovenia-roads.geojson