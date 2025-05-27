#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.strtree import STRtree
from shapely.geometry import Point

# ───────────────────────────── config ──────────────────────────────
IN_DIR            = Path("data_denoised")     # denoised input
OUT_DIR           = Path("data_binned")       # enriched output
GRID_FILE         = Path("maps/minimalist_coning.geojson")
TIME_BIN_MINUTES  = 60
# TOWER_FILE        = Path("data/slovenia_towers.parquet")
# MAX_RADIUS_M      = 300    # accept towers within this radius

# # ─────────────────────── towers (once) ────────────────────────────
# tower_df  = pd.read_parquet(TOWER_FILE)
# tower_gdf = gpd.GeoDataFrame(
#     tower_df,
#     geometry=gpd.points_from_xy(tower_df.LON, tower_df.LAT),
#     crs="EPSG:4326"
# ).to_crs("EPSG:3857")

# tower_gdf["tower_id"] = np.arange(len(tower_gdf), dtype="int32")

# # ─────────────────── helper: add nearest tower ────────────────────
# def add_tower_id(df: pd.DataFrame,
#                  max_radius_m: float = MAX_RADIUS_M) -> pd.DataFrame:
#     # build point GeoDataFrame in metres
#     pts = gpd.GeoDataFrame(
#         df,
#         geometry=gpd.points_from_xy(df.lon, df.lat),
#         crs="EPSG:4326"
#     ).to_crs("EPSG:3857")

#     joined = gpd.sjoin_nearest(
#         pts,
#         tower_gdf[["tower_id", "geometry"]],
#         how="left",
#         max_distance=max_radius_m,
#         distance_col="tower_dist_m"
#     )

#     # ── collapse possible duplicates (one row per point) ──────────
#     first = (joined
#              .sort_index()                       # keep stable order
#              .groupby(level=0)                   # original point index
#              .first())

#     df["tower_id"]     = first["tower_id"].to_numpy()
#     df["tower_dist_m"] = first["tower_dist_m"].to_numpy()
#     df["tower_ok"]     = first["tower_id"].notna()

#     return df

# ───────────────────────── logging setup ──────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ───────────────────── other helper functions ─────────────────────
CHUNK = 5_000_000                       # rows per slice

_grid_gdf = gpd.read_file(GRID_FILE)
if "zone_id" not in _grid_gdf.columns:
    _grid_gdf["zone_id"] = np.arange(len(_grid_gdf), dtype="int32")
_grid_gdf = _grid_gdf.to_crs("EPSG:4326")      # WGS-84

def add_zone_id(df: pd.DataFrame, chunk: int = 5_000_000) -> pd.DataFrame:
    n = len(df)
    zone_out = np.full(n, -1, dtype=np.int32)        # -1  → no polygon

    for start in range(0, n, chunk):
        end = min(start + chunk, n)

        # build GeoDataFrame slice
        pts = gpd.GeoDataFrame(
            df.iloc[start:end],
            geometry=gpd.points_from_xy(df.lon.iloc[start:end],
                                        df.lat.iloc[start:end]),
            crs="EPSG:4326"
        )

        joined = gpd.sjoin(
            pts,
            _grid_gdf[["zone_id", "geometry"]],
            how="left",
            predicate="within"
        )

        # first match per original point
        z = (joined
             .groupby(level=0)["zone_id"]
             .first()
             .dropna())                       # drop NaNs (unmatched)

        idx_absolute = z.index.to_numpy(dtype=np.intp)
        zone_out[idx_absolute] = z.to_numpy(dtype=np.int32)

    df["zone_id"] = zone_out
    return df


def add_time_bin(df: pd.DataFrame,
                 minutes: int = TIME_BIN_MINUTES) -> pd.DataFrame:
    mins = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    df["time_bin"] = (mins // minutes).astype("int16")
    return df

# ─────────────────────────── main loop ────────────────────────────
def main() -> None:
    grid = gpd.read_file(GRID_FILE)
    if "zone_id" not in grid.columns:
        grid["zone_id"] = np.arange(len(grid), dtype="int32")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for f in IN_DIR.glob("*.parquet"):
        logging.info("Processing %s", f)
        df = pd.read_parquet(f)

        logging.info("Adding spatial bin")
        df = add_zone_id(df)          

        logging.info("Adding time bin")
        df = add_time_bin(df)

        print(f"df.colums= {df.columns}")

        out_path = OUT_DIR / f.name
        df.to_parquet(out_path, index=False, compression="snappy")
        logging.info("Wrote %s", out_path)

if __name__ == "__main__":
    main()


# In[4]:


#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from pprint import pprint

RAW_DIR = Path("data")
BIN_DIR = Path("data_binned")

def columns_in_file(path):
    return pq.ParquetFile(path).schema_arrow.names

def build_datetime(df):
    if "datetime" in df.columns:
        return df
    if {"date", "time"}.issubset(df.columns):
        ts = pd.to_datetime(df["date"].astype(str) + " " +
                            df["time"].astype(str),
                            errors="coerce")
        df = df.assign(datetime=ts)
    return df.dropna(subset=["datetime"])

def stats(df):
    n_pts = len(df)
    n_dev = df["deviceid"].nunique()
    hrs   = (df["datetime"].max() - df["datetime"].min()
            ).total_seconds()/3600
    return dict(
        n_points=n_pts,
        n_devices=n_dev,
        avg_pts_per_device=round(n_pts/n_dev, 2) if n_dev else 0,
        hours_covered=round(hrs, 2)
    )

def compare(a, b):
    out = {}
    for k in a:
        before, after = a[k], b[k]
        red = round((before - after) / before * 100, 2) if before else None
        out[k] = {"before": before, "after": after, "reduction_%": red}
    return out

def main():
    for raw in sorted(RAW_DIR.glob("*.parquet")):
        binned = BIN_DIR / raw.name
        if not binned.exists():
            print(f"⚠️  missing binned file for {raw.name}")
            continue

        want_cols = {"deviceid", "date", "time", "lon", "lat", "datetime"}
        raw_cols  = want_cols.intersection(columns_in_file(raw))

        df_raw = pd.read_parquet(raw, columns=list(raw_cols))
        df_raw = build_datetime(df_raw)

        df_bin = pd.read_parquet(
            binned,
            columns=["deviceid", "datetime", "lon", "lat"]
        )

        s_raw, s_bin = stats(df_raw), stats(df_bin)
        comp = compare(s_raw, s_bin)

        print(f"\n=== {raw.stem} ===")
        pprint({"raw": s_raw, "binned": s_bin, "comparison": comp})

if __name__ == "__main__":
    main()

