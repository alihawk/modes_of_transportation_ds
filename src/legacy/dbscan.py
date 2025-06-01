import pandas as pd
import numpy as np
import folium
from sklearn.cluster import DBSCAN
from datetime import datetime
import math
import time


# -----------------------------
# Define a vectorized Haversine function for distance calculation
# -----------------------------
def haversine_vectorized(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    Vectorized version using numpy for speed.
    """
    # convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    R = 6371000  # Radius of earth in meters
    return R * c


# -----------------------------
# 1. Data Loading and Filtering
# -----------------------------
start_time = time.time()
print("Loading dataset...")
df = pd.read_parquet("training_set/20230327.parquet")

# OPTIONAL: Reduce the dataset size for faster processing during experimentation.
# Uncomment the following line to sample a subset of data:
# df = df.head(10000)  # Process only the first 10,000 rows

print("Dataset loaded. Total rows:", len(df))

# Extract the first device id and corresponding date from the dataset
first_device_id = df.iloc[0]['deviceid']
date_to_plot = df.iloc[0]['date']

print(f"Selected device ID: {first_device_id}")
print(f"Selected date: {date_to_plot}")

# Filter the data for the selected device and date
df_sub = df[(df['deviceid'] == first_device_id) & (df['date'] == date_to_plot)].copy()
if df_sub.empty:
    raise ValueError("No data found for the specified device and date. Please check your filter values.")

print("Filtered dataset for device and date. Rows after filtering:", len(df_sub))

# Drop rows with missing lat or lon
df_sub.dropna(subset=['lat', 'lon'], inplace=True)
print("Dropped rows with NaN coordinates. Remaining rows:", len(df_sub))

# Fix the date/time concatenation: Convert to string first (in case they are categorical)
df_sub['datetime'] = pd.to_datetime(df_sub['date'].astype(str) + ' ' + df_sub['time'].astype(str),
                                    format="%d.%m.%Y %H:%M:%S")
df_sub.sort_values("datetime", inplace=True)
df_sub.reset_index(drop=True, inplace=True)
print("Datetime conversion and sorting done.")

# -------------------------------
# 2. Detect Stops with DBSCAN
# -------------------------------
print("Running DBSCAN for stop detection...")
coords = df_sub[['lat', 'lon']].to_numpy()
# For haversine metric, convert coords to radians
coords_rad = np.radians(coords)

# DBSCAN parameters - tune these for your use case
eps_val = 0.001  # in radians, adjust based on expected scale (roughly ~111m at the equator)
min_samples = 5

db = DBSCAN(eps=eps_val, min_samples=min_samples, metric='haversine')
cluster_labels = db.fit_predict(coords_rad)
df_sub['cluster'] = cluster_labels
print("DBSCAN clustering complete. Unique clusters (including noise -1):", np.unique(cluster_labels))

# Here, we mark points in a cluster (cluster != -1) as a stop; noise (cluster == -1) is treated as movement.
df_sub['segment_type'] = np.where(df_sub['cluster'] == -1, 'Movement', 'Stop')
print("Segment types assigned based on DBSCAN results.")

# --------------------------------
# 3. Segment the Trajectory
# --------------------------------
print("Segmenting the trajectory...")
# Create a helper column to detect changes in segment type
df_sub['segment_change'] = (df_sub['segment_type'] != df_sub['segment_type'].shift(1)).cumsum()

segments = df_sub.groupby('segment_change')
segment_summaries = []

for seg_id, group in segments:
    seg_type = group['segment_type'].iloc[0]
    start_ts = group['datetime'].iloc[0]
    end_ts = group['datetime'].iloc[-1]
    duration_sec = (end_ts - start_ts).total_seconds()

    if seg_type == 'Stop':
        mean_lat = group['lat'].mean()
        mean_lon = group['lon'].mean()
        info = {
            'segment': seg_id,
            'type': seg_type,
            'start_time': start_ts,
            'end_time': end_ts,
            'duration_sec': duration_sec,
            'mean_lat': mean_lat,
            'mean_lon': mean_lon,
            'n_points': len(group)
        }
    else:  # Movement segments
        coords_arr = group[['lat', 'lon']].to_numpy()
        # Compute distance using vectorized haversine on consecutive points
        if len(coords_arr) > 1:
            lat1 = coords_arr[:-1, 0]
            lon1 = coords_arr[:-1, 1]
            lat2 = coords_arr[1:, 0]
            lon2 = coords_arr[1:, 1]
            distances = haversine_vectorized(lat1, lon1, lat2, lon2)
            total_distance = np.sum(distances)
        else:
            total_distance = 0.0

        avg_speed = total_distance / duration_sec if duration_sec > 0 else np.nan
        info = {
            'segment': seg_id,
            'type': seg_type,
            'start_time': start_ts,
            'end_time': end_ts,
            'duration_sec': duration_sec,
            'distance_m': total_distance,
            'avg_speed_m_s': avg_speed,
            'n_points': len(group)
        }
    segment_summaries.append(info)

segments_df = pd.DataFrame(segment_summaries)
print("Segmenting complete. Summary of segments:")
print(segments_df)
print(f"Segmentation took {time.time() - start_time:.2f} seconds.")

# -------------------------------------------
# 4. Visualization on a Geographic Map
# -------------------------------------------
print("Generating map visualization...")
# Center the map on the overall average coordinates of the subset
center_lat = df_sub['lat'].mean()
center_lon = df_sub['lon'].mean()
m = folium.Map(location=[center_lat, center_lon], zoom_start=13)

# Draw the overall trajectory as a gray polyline
trajectory_coords = list(zip(df_sub['lat'], df_sub['lon']))
folium.PolyLine(locations=trajectory_coords, color="gray", weight=2.5, opacity=0.7).add_to(m)

# Mark Stop segments with red circles
for seg in segments_df[segments_df['type'] == 'Stop'].itertuples():
    folium.CircleMarker(
        location=[seg.mean_lat, seg.mean_lon],
        radius=8,
        color="red",
        fill=True,
        fill_color="red",
        popup=(f"Stop: Duration {seg.duration_sec:.1f} sec, Points: {seg.n_points}")
    ).add_to(m)

# Mark the start and end points of Movement segments
for seg in segments_df[segments_df['type'] == 'Movement'].itertuples():
    seg_group = segments.get_group(seg.segment)
    start_pt = seg_group.iloc[0]
    end_pt = seg_group.iloc[-1]
    folium.Marker(
        location=[start_pt['lat'], start_pt['lon']],
        popup=f"Movement start: {start_pt['datetime']}"
    ).add_to(m)
    folium.Marker(
        location=[end_pt['lat'], end_pt['lon']],
        popup=f"Movement end: {end_pt['datetime']}"
    ).add_to(m)

m.save("trajectory_segments_map.html")
print("Map visualization saved as trajectory_segments_map.html.")
print(f"Total runtime: {time.time() - start_time:.2f} seconds.")
