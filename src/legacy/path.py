import pandas as pd
import folium

# Load the dataset from the Parquet file
df = pd.read_parquet("training_set/20230327.parquet")

# Print the first few rows to see the full device IDs
print(df.head())

# Extract the first device ID and corresponding date from the DataFrame
first_device_id = df.iloc[0]['deviceid']
date_to_plot = df.iloc[0]['date']

# Filter the data for the first device and its date
df_sub = df[(df['deviceid'] == first_device_id) & (df['date'] == date_to_plot)]

# Check if the filtered DataFrame is empty
if df_sub.empty:
    raise ValueError("No data found for the specified device and date. Please check your filter values.")

# Drop rows with NaN values in lat or lon, if any
df_sub = df_sub.dropna(subset=['lat', 'lon'])

# Sort by time to get the correct sequence
df_sub = df_sub.sort_values("time")

# Compute the center of the map based on the average of latitudes and longitudes
mean_lat = df_sub['lat'].mean()
mean_lon = df_sub['lon'].mean()

if pd.isna(mean_lat) or pd.isna(mean_lon):
    raise ValueError("Calculated mean latitude or longitude is NaN. Verify that the 'lat' and 'lon' columns are valid.")

# Create a folium map centered at the computed coordinates
m = folium.Map(location=[mean_lat, mean_lon], zoom_start=13)

# Gather the coordinates into a list of (lat, lon) tuples
coordinates = list(zip(df_sub['lat'], df_sub['lon']))

# Draw a polyline to visualize the device's trajectory
folium.PolyLine(locations=coordinates, color="blue", weight=2.5, opacity=1).add_to(m)

# Optionally add markers for each point with time as a popup
for index, row in df_sub.iterrows():
    folium.Marker(location=[row['lat'], row['lon']], popup=str(row['time'])).add_to(m)

# Save the map to an HTML file and let the user know
m.save("trajectory_map.html")
print("Map saved as trajectory_map.html.")
