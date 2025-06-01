from scripts.common_imports import *
from scripts.binning.params import MAX_SPEED_KMH

def analyze_speed_bins(df, zone_col='zone_id'):
    """
    Compute median and std of speeds for each spatial bin.
    Assumes 'speed_kmh' and 'zone_id' columns exist.
    """
    if 'speed_kmh' not in df.columns or zone_col not in df.columns:
        raise ValueError("Missing required columns for speed analysis")

    grouped = df.groupby(zone_col)['speed_kmh']
    stats = grouped.agg(['median', 'std', 'count']).reset_index()
    stats = stats.rename(columns={'median': 'speed_median', 'std': 'speed_std', 'count': 'count_points'})
    return stats

def add_speed_id(df):
    """Compute speed between consecutive points."""
    df = df.sort_values(['deviceid', 'datetime'])
    df['lat_shift'] = df.groupby('deviceid', observed=False)['lat'].shift()
    df['lon_shift'] = df.groupby('deviceid', observed=False)['lon'].shift()
    df['time_shift'] = df.groupby('deviceid', observed=False)['datetime'].shift()
    
    def compute_row_speed(row):
        if pd.isna(row['lat_shift']):
            return np.nan
        d = geodesic((row['lat'], row['lon']), (row['lat_shift'], row['lon_shift'])).meters
        t = (row['datetime'] - row['time_shift']).total_seconds()
        if t == 0:
            return np.nan
        return (d / t) * 3.6  # m/s to km/h

    df['speed_kmh'] = df.apply(compute_row_speed, axis=1)
    return df

def filter_high_speeds(df, max_speed=MAX_SPEED_KMH):
    """Filter transitions with too high speeds."""
    return df[df['speed_kmh'] <= max_speed]