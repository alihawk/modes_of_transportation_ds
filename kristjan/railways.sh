mkdir -p railway-data
cd railway-data

# Create a query file for railways in Slovenia
cat > railway_query.overpassql << EOF
[out:json];
area["ISO3166-1"="SI"][admin_level=2];
(
  way["railway"="rail"](area);
  way["railway"="light_rail"](area);
  way["railway"="narrow_gauge"](area);
  relation["railway"="rail"](area);
);
out body;
>;
out skel qt;
EOF

# Run the query and convert to GeoJSON
wget -O slovenia_railways.json --post-file=railway_query.overpassql "https://overpass-api.de/api/interpreter"

# Install osmtogeojson locally in this directory
npm init -y
npm install osmtogeojson

# Convert to GeoJSON using locally installed package
npx osmtogeojson slovenia_railways.json > slovenia_railways.geojson