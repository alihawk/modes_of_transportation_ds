from pathlib import Path
import logging
import pandas as pd
import numpy as np

# ───────────────────────── config ─────────────────────────
SRC_DIR   = Path("data")            # original source files
DST_DIR   = Path("data_denoised")   # final output
SKIP      = {"slovenia_towers.parquet"}

R = 6_371_000.0                     # Earth radius (m)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ───────────── helpers: speed / denoise ─────────────────
def sequential_deltas(df: pd.DataFrame) -> pd.DataFrame:
    """Add device_change, dist_m, dt, speed_m_s (in-place)."""
    df["date"]     = df["date"].astype(str)
    df["time"]     = df["time"].astype(str)
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"],
                                    dayfirst=True)
    
    df.sort_values(["deviceid", "datetime"], inplace=True, ignore_index=True)

    dc = (df["deviceid"] != df["deviceid"].shift()).to_numpy()
    df["device_change"] = dc

    lat = np.radians(df["lat"].to_numpy())
    lon = np.radians(df["lon"].to_numpy())
    t   = (df["datetime"].astype("int64").to_numpy() //
           1_000_000_000)

    lat_prev = np.roll(lat, 1); lon_prev = np.roll(lon, 1); t_prev = np.roll(t, 1)
    lat_prev[dc] = lat[dc]; lon_prev[dc] = lon[dc]; t_prev[dc] = t[dc]

    dlat = lat - lat_prev
    dlon = lon - lon_prev
    a    = np.sin(dlat/2)**2 + np.cos(lat)*np.cos(lat_prev)*np.sin(dlon/2)**2
    dist = R * (2*np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    dt   = (t - t_prev).clip(min=1)

    dist[dc] = 0.0; dt[dc] = 0.0
    speed    = np.divide(dist, dt, out=np.zeros_like(dist), where=dt > 0)

    df["dist_m"]    = dist
    df["dt"]        = dt
    df["speed_m_s"] = speed
    df["lat_rad"]   = lat          # for angle later
    df["lon_rad"]   = lon
    return df


def _bearing(lat1, lon1, lat2, lon2):
    dλ = lon2 - lon1
    x  = np.sin(dλ) * np.cos(lat2)
    y  = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dλ)
    return (np.degrees(np.arctan2(x, y)) + 360.0) % 360.0


def zhang_denoise(df, speed_th=30, angle_th=30, time_th=10):
    lat = df["lat_rad"].to_numpy()
    lon = df["lon_rad"].to_numpy()
    t   = df["datetime"].astype("int64").to_numpy() // 1_000_000_000
    t   = t.astype("float64")
    spd = df["speed_m_s"].to_numpy()
    dc  = df["device_change"].to_numpy()

    lat_prev = np.roll(lat, 1); lon_prev = np.roll(lon, 1); t_prev = np.roll(t, 1)
    lat_next = np.roll(lat, -1); lon_next = np.roll(lon, -1)

    lat_prev[dc] = lon_prev[dc] = t_prev[dc] = np.nan
    last = np.roll(dc, -1); last[-1] = True
    lat_next[last] = lon_next[last] = np.nan

    b1 = _bearing(lat_prev, lon_prev, lat, lon)
    b2 = _bearing(lat, lon, lat_next, lon_next)
    ang = np.abs(b2 - b1)
    ang = np.where(ang > 180, 360 - ang, ang)

    dt = t - t_prev
    keep = (spd < speed_th) & ((ang > angle_th) | (dt > time_th))
    keep &= ~np.isnan(lat_prev) & ~np.isnan(lat_next)
    return df[keep].reset_index(drop=True)


def sliding_window_denoise(df, window=5, speed_th=40, min_pts=3):
    spd = df["speed_m_s"].to_numpy()
    dc  = df["device_change"].to_numpy()
    keep = np.ones(len(df), dtype=bool)

    start = 0
    for i in range(1, len(df) + 1):
        if i == len(df) or dc[i]:
            med = pd.Series(spd[start:i]).rolling(
                window, center=True, min_periods=1).median().to_numpy()
            keep[start:i] &= med < speed_th
            start = i

    valid = (df.loc[keep, "deviceid"]
               .value_counts()
               .loc[lambda s: s > min_pts]
               .index)
    keep &= df["deviceid"].isin(valid)
    return df[keep].reset_index(drop=True)


# ─────────────────────── main loop ───────────────────────
def main():
    DST_DIR.mkdir(parents=True, exist_ok=True)

    for fp in SRC_DIR.glob("*.parquet"):
        if fp.name in SKIP:
            logging.info("Skipping %s", fp)
            continue

        logging.info("Processing %s", fp)
        df = pd.read_parquet(fp)
        
        # cast coords to float32
        df[["lat", "lon"]] = df[["lat", "lon"]].astype("float32")
        
        codes, _ = pd.factorize(df['deviceid'], sort=False)
        df['deviceid'] = codes.astype('int32')

        logging.info("computing deltas")
        df = sequential_deltas(df)

        logging.info("Zhang denoise")
        df = zhang_denoise(df)

        logging.info("sliding-window denoise")
        df = sliding_window_denoise(df)

        df.drop(columns=["lat_rad", "lon_rad"], inplace=True)

        out_path = DST_DIR / fp.name
        df.to_parquet(out_path, index=False, compression="snappy")
        logging.info("  wrote %s", out_path)

if __name__ == "__main__":
    main()