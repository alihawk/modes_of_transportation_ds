from scripts.common_imports import *

# ==================== Denoising Methods ====================

def compute_angle(p1, p2, p3):
    a = np.array([p1['lat'] - p2['lat'], p1['lon'] - p2['lon']])
    b = np.array([p3['lat'] - p2['lat'], p3['lon'] - p2['lon']])
    if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
        return 0
    return np.degrees(np.arccos(np.clip(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)), -1.0, 1.0)))

def zhang_denoise_device(df, speed_thres=50, angle_thres=30, time_thres=timedelta(seconds=10)):
    "speed_threshold is in m/s, 50m/s = 180 km/h"
    if "datetime" not in df.columns:
        raise ValueError(f"Missing 'datetime' column in input data! Columns found: {df.columns.tolist()}")
    df = df.sort_values('datetime').reset_index(drop=True)
    filtered = []
    for i in range(1, len(df) - 1):
        p1, p2, p3 = df.iloc[i-1], df.iloc[i], df.iloc[i+1]
        dt = (p2['datetime'] - p1['datetime']).total_seconds()
        dist = geodesic((p1['lat'], p1['lon']), (p2['lat'], p2['lon'])).meters
        speed = dist / dt if dt > 0 else 0
        angle = compute_angle(p1, p2, p3)
        if speed < speed_thres and (angle > angle_thres or dt > time_thres.total_seconds()):
            filtered.append(p2)
    return pd.DataFrame(filtered)

def sliding_window_denoise_device(df, window_size=5, speed_thres=50):
    df = df.sort_values('datetime').reset_index(drop=True)
    half_window = window_size // 2
    filtered_indices = []

    for i in range(len(df)):
        window_start = max(0, i - half_window)
        window_end = min(len(df), i + half_window + 1)
        window = df.iloc[window_start:window_end]

        if len(window) < 2:
            continue  # Not enough points to compute speed

        speeds = []
        for j in range(1, len(window)):
            prev, curr = window.iloc[j-1], window.iloc[j]
            dt = (curr['datetime'] - prev['datetime']).total_seconds()
            dist = geodesic((prev['lat'], prev['lon']), (curr['lat'], curr['lon'])).meters
            if dt > 0:
                speeds.append(dist / dt)

        median_speed = np.median(speeds) if speeds else 0
        if median_speed < speed_thres:
            filtered_indices.append(i)

    return df.iloc[filtered_indices].reset_index(drop=True)

def haversine_np(lat1, lon1, lat2, lon2):
    R = 6371000  # Earth radius in meters
    lat1 = np.radians(lat1)
    lat2 = np.radians(lat2)
    dlat = lat2 - lat1
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))

def sliding_window_denoise_device_vectorized(df, window_size=5, speed_thres=50):
    df = df.sort_values('datetime').reset_index(drop=True)
    half_window = window_size // 2
    lat = df['lat'].values
    lon = df['lon'].values
    time = df['datetime'].values.astype('datetime64[s]').astype(np.int64)

    n = len(df)
    keep = np.zeros(n, dtype=bool)

    for i in range(n):
        start = max(0, i - half_window)
        end = min(n, i + half_window + 1)

        lat_window = lat[start:end]
        lon_window = lon[start:end]
        time_window = time[start:end]

        if len(lat_window) < 2:
            continue

        # Compute distances between consecutive points
        dists = haversine_np(lat_window[:-1], lon_window[:-1], lat_window[1:], lon_window[1:])
        dts = np.diff(time_window)

        with np.errstate(divide='ignore', invalid='ignore'):
            speeds = np.where(dts > 0, dists / dts, 0)

        median_speed = np.median(speeds)

        if median_speed < speed_thres:
            keep[i] = True

    return df[keep].reset_index(drop=True)



# ==================== Dask Processing ====================

def denoise_partition(df_partition, method="sliding"):
    results = []
    for device_id, group in df_partition.groupby('deviceid', observed=False):
        if method == "zhang":
            cleaned = zhang_denoise_device(group)
        elif method == "sliding":
            cleaned = sliding_window_denoise_device_vectorized(group)
        else:
            raise ValueError("Unsupported method: choose 'zhang' or 'sliding'")
        if not cleaned.empty:
            results.append(cleaned)
    if results:
        return pd.concat(results)
    else:
        return pd.DataFrame(columns=df_partition.columns)