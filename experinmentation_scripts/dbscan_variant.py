import logging
import dask.dataframe as dd
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import folium

# -----------------------------
# CONFIGURE LOGGING
# -----------------------------
logger = logging.getLogger("DBSCAN_Comparison")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('dbscan_variant.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# -----------------------------
# HAVERSINE MATRIX FUNCTION
# -----------------------------
def haversine_matrix(lat_lon):
    R = 6371000  # Earth radius in meters
    lat = lat_lon[:, 0][:, None]
    lon = lat_lon[:, 1][:, None]
    dlat = lat - lat.T
    dlon = lon - lon.T
    a = np.sin(dlat / 2)**2 + np.cos(lat) * np.cos(lat.T) * np.sin(dlon / 2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

# -----------------------------
# MAIN WORKFLOW
# -----------------------------
def main():
    # 1) Load & sample
    logger.info("Loading dataset with Dask...")
    ddf = dd.read_parquet("training_set/20230327.parquet")
    df = ddf.compute().iloc[:10000]  # sample for demo
    logger.info(f"Sample loaded: {len(df)} rows")

    # 2) Parse datetime
    df['datetime'] = pd.to_datetime(
        df['date'].astype(str) + ' ' + df['time'].astype(str),
        format="%d.%m.%Y %H:%M:%S"
    )
    df.sort_values("datetime", inplace=True)
    logger.info("Parsed and sorted datetime")

    # 3) Prepare coords
    coords = df[['lat', 'lon']].to_numpy()
    coords_rad = np.radians(coords)

    # 4) Spatial-only DBSCAN
    logger.info("Running spatial-only DBSCAN...")
    db_sp = DBSCAN(eps=0.001, min_samples=5, metric='haversine')
    df['cluster_spatial'] = db_sp.fit_predict(coords_rad)
    n_sp = len(set(df['cluster_spatial'])) - (1 if -1 in df['cluster_spatial'] else 0)
    logger.info(f"Spatial clusters found: {n_sp}")

    # 5) Spatio-temporal DBSCAN hack
    logger.info("Computing spatio-temporal distances...")
    spatial_d = haversine_matrix(coords_rad)
    times = df['datetime'].astype(np.int64) // 10**9
    dt = np.abs(times[:, None] - times[None, :])
    alpha = 1000  # tune based on domain
    combined = np.sqrt(spatial_d**2 + (alpha * dt)**2)

    logger.info("Running spatio-temporal DBSCAN...")
    db_st = DBSCAN(eps=500, min_samples=5, metric='precomputed')
    df['cluster_spatiotemporal'] = db_st.fit_predict(combined)
    n_st = len(set(df['cluster_spatiotemporal'])) - (1 if -1 in df['cluster_spatiotemporal'] else 0)
    logger.info(f"Spatio-temporal clusters found: {n_st}")

    # 6) Visualize spatial-only clusters
    logger.info("Visualizing spatial-only clusters on map...")
    m_sp = folium.Map(location=[df['lat'].mean(), df['lon'].mean()], zoom_start=13)
    for _, row in df.iterrows():
        color = 'gray' if row['cluster_spatial'] == -1 else f"#{hex(0x100000 + (row['cluster_spatial']*123456)%0xFFFFFF)[2:]}"
        folium.CircleMarker(
            location=(row['lat'], row['lon']),
            radius=3, color=color, fill=True, fill_color=color
        ).add_to(m_sp)
    m_sp.save("spatial_clusters_map.html")
    logger.info("Saved spatial_clusters_map.html")

    # 7) Visualize spatio-temporal clusters
    logger.info("Visualizing spatio-temporal clusters on map...")
    m_st = folium.Map(location=[df['lat'].mean(), df['lon'].mean()], zoom_start=13)
    for _, row in df.iterrows():
        color = 'gray' if row['cluster_spatiotemporal'] == -1 else f"#{hex(0x100000 + (row['cluster_spatiotemporal']*789012)%0xFFFFFF)[2:]}"
        folium.CircleMarker(
            location=(row['lat'], row['lon']),
            radius=3, color=color, fill=True, fill_color=color
        ).add_to(m_st)
    m_st.save("spatiotemporal_clusters_map.html")
    logger.info("Saved spatiotemporal_clusters_map.html")

if __name__ == "__main__":
    main()
