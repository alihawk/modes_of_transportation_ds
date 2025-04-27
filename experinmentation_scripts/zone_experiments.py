import logging
import dask.dataframe as dd
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from sklearn.mixture import GaussianMixture
import folium

# -----------------------------
# CONFIGURE LOGGING
# -----------------------------
zone_logger = logging.getLogger("zone_experiments")
zone_logger.setLevel(logging.INFO)
zh = logging.FileHandler('zone_experiments.log')
zh.setLevel(logging.INFO)
zh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
zone_logger.addHandler(zh)
zone_logger.addHandler(logging.StreamHandler())

# -----------------------------
# MAIN WORKFLOW
# -----------------------------
def main():
    zone_logger.info("Loading dataset for zone experiments...")
    zddf = dd.read_parquet("training_set/20230327.parquet")
    zdf = zddf.compute().iloc[:10000]
    zone_logger.info(f"Loaded {len(zdf)} rows")

    # parse datetime
    zdf['datetime'] = pd.to_datetime(
        zdf['date'].astype(str) + ' ' + zdf['time'].astype(str),
        format="%d.%m.%Y %H:%M:%S"
    )
    zdf.sort_values("datetime", inplace=True)

    # compute speeds
    def hav(lat1, lon1, lat2, lon2):
        R = 6371000
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dl = np.radians(lon2 - lon1)
        a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dl/2)**2
        return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1-a))

    zdf['distance'] = hav(zdf['lat'].shift(), zdf['lon'].shift(),
                          zdf['lat'], zdf['lon'])
    zdf['time_diff'] = zdf['datetime'].diff().dt.total_seconds()
    zdf['speed_m_s'] = zdf['distance'] / zdf['time_diff']
    zone_logger.info("Computed speed_m_s for each ping")

    # load towers and compute distances
    towers = pd.read_csv("cell_towers.csv")  # columns: lat, lon
    tree = cKDTree(towers[['lat','lon']])
    dists, idxs = tree.query(zdf[['lat','lon']], k=3)
    zdf[['d1','d2','d3']] = dists
    zone_logger.info("Computed distances to 3 nearest towers")

    # filter outliers
    zf = zdf[zdf['d1'] < 3000]
    zone_logger.info(f"Filtered outliers, remaining {len(zf)} rows")

    # optional: per-zone GMM clustering
    zf['zone'] = idxs[:,0]
    for zid, grp in zf.groupby('zone'):
        coords = grp[['lat','lon']].to_numpy()
        try:
            gmm = GaussianMixture(n_components=2).fit(coords)
            labels = gmm.predict(coords)
            zone_logger.info(f"Zone {zid} GMM clusters: {len(set(labels))}")
        except Exception as e:
            zone_logger.error(f"Zone {zid} GMM error: {e}")

    # visualize speeds & towers
    zone_logger.info("Visualizing speeds and towers on map...")
    m_z = folium.Map(location=[zf['lat'].mean(), zf['lon'].mean()], zoom_start=12)
    for _, row in zf.iterrows():
        sp = row['speed_m_s']
        color = 'blue' if sp < 1 else 'green' if sp < 5 else 'red'
        folium.CircleMarker(
            location=(row['lat'], row['lon']),
            radius=3, color=color, fill=True, fill_color=color
        ).add_to(m_z)

    for _, t in towers.iterrows():
        folium.Marker(
            location=(t['lat'], t['lon']),
            icon=folium.Icon(color='black', icon='tower'),
            popup="Tower"
        ).add_to(m_z)

    m_z.save("zone_speed_map.html")
    zone_logger.info("Saved zone_speed_map.html")

if __name__ == "__main__":
    main()
