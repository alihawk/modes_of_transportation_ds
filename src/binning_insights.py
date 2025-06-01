#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import logging
import io
from pathlib import Path

# ───────────────────────── Config & Logging ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Directories
BINS_DIR  = Path("data_binned")
FEATS_DIR = Path("data_bin_features")
FEATS_DIR.mkdir(exist_ok=True, parents=True)

# Constants
R = 6_371_000.0  # Earth radius (m)

def shannon_entropy(counts: np.ndarray) -> float:
    p = counts / counts.sum()
    return float(-(p * np.log2(p)).sum())

def process_file(fp: Path):
    stem = fp.stem
    logger.info(f"▶ Processing {fp.name}")
    df = pd.read_parquet(
        fp,
        columns=["deviceid","datetime","lat","lon","zone_id","time_bin"],
    )
    logger.info(f"   • Loaded {len(df):,} rows across "
                f"{df['zone_id'].nunique():,} zones and {df['time_bin'].nunique():,} time bins")

    # ───────────────────────── Compute Deltas ─────────────────────────
    df = df.sort_values(["deviceid","datetime"]).reset_index(drop=True)
    dc = df["deviceid"] != df["deviceid"].shift(1)

    lat = np.radians(df["lat"].values)
    lon = np.radians(df["lon"].values)
    t   = (df["datetime"].astype("int64") // 1_000_000_000).to_numpy()

    lat_prev = np.roll(lat, 1)
    lon_prev = np.roll(lon, 1)
    t_prev   = np.roll(t,   1)
    lat_prev[dc] = lat[dc]
    lon_prev[dc] = lon[dc]
    t_prev[dc]   = t[dc]

    dlat = lat - lat_prev
    dlon = lon - lon_prev
    a    = np.sin(dlat/2)**2 + np.cos(lat)*np.cos(lat_prev)*np.sin(dlon/2)**2
    dist = R * (2*np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    dt   = np.clip(t - t_prev, a_min=1, a_max=None)

    dist[dc] = 0.0
    dt[dc]   = 0.0

    df["speed_m_s"] = dist / dt
    logger.info("   • Computed per-ping speed_m_s")

    # ───────────────────────── Feature Aggregation ─────────────────────────
    grp = df.groupby(["zone_id","time_bin"], sort=False)

    speed_stats = grp["speed_m_s"].agg(
        speed_mean="mean",
        speed_median="median",
        speed_min="min",
        speed_max="max",
        speed_var="var",
        speed_q25=lambda x: x.quantile(0.25),
        speed_q75=lambda x: x.quantile(0.75),
    ).reset_index()
    logger.info("   • Speed stats computed")

    density = grp["deviceid"].agg(
        ping_count="size",
        unique_devs=lambda x: x.nunique(),
    ).reset_index()
    density["pings_per_dev"] = density["ping_count"] / density["unique_devs"]

    entropy = (
        df.groupby(["zone_id","time_bin","deviceid"], sort=False).size()
          .groupby(level=[0,1], sort=False)
          .apply(lambda s: shannon_entropy(s.values))
          .reset_index(name="dev_entropy")
    )
    logger.info("   • Density & entropy computed")

    dwell = (
        df.groupby(["zone_id","time_bin","deviceid"], sort=False)["datetime"]
          .agg(start="min", end="max")
          .reset_index()
    )
    dwell["dwell_s"] = (dwell["end"] - dwell["start"]).dt.total_seconds()
    dwell_stats = dwell.groupby(["zone_id","time_bin"], sort=False)["dwell_s"].agg(
        dwell_mean="mean",
        dwell_median="median",
        dwell_min="min",
        dwell_max="max",
    ).reset_index()
    logger.info("   • Dwell time stats computed")

    df["prev_zone"] = df.groupby("deviceid", sort=False)["zone_id"].shift(1)
    df["next_zone"] = df.groupby("deviceid", sort=False)["zone_id"].shift(-1)

    in_mask  = df["zone_id"] != df["prev_zone"]
    out_mask = df["zone_id"] != df["next_zone"]
    in_stats = df[in_mask].groupby("zone_id", sort=False)["speed_m_s"].agg(
        in_count="count", in_speed_mean="mean",
    ).reset_index()
    out_stats = df[out_mask].groupby("zone_id", sort=False)["speed_m_s"].agg(
        out_count="count", out_speed_mean="mean",
    ).reset_index()

    trans = (
        df[out_mask]
          .groupby(["zone_id","next_zone"], sort=False)
          .size().reset_index(name="cnt")
    )
    total = trans.groupby("zone_id", sort=False)["cnt"].sum().reset_index(name="total")
    trans = trans.merge(total, on="zone_id")
    trans["p"] = trans["cnt"] / trans["total"]
    trans_entropy = (
        trans.groupby("zone_id", sort=False)
             .apply(lambda g: -(g["p"] * np.log2(g["p"])).sum())
             .reset_index(name="trans_entropy")
    )
    logger.info("   • Transition profiles computed")

    time_bins = np.sort(df["time_bin"].unique())
    time_flags = pd.DataFrame({
        "time_bin": time_bins,
        "is_morning_commute": np.isin(time_bins, [7,8,9]),
        "is_evening_commute": np.isin(time_bins, [16,17,18]),
        "is_late_night":      (time_bins <= 5),
    })
    logger.info("   • Temporal flags set")

    zone_priors = speed_stats[["zone_id"]].drop_duplicates().assign(
        prior_walk=0.5, prior_car=0.5
    )
    logger.info("   • Zone-mode priors loaded")

    feat = (
        speed_stats
        .merge(density,      on=["zone_id","time_bin"], how="left")
        .merge(entropy,      on=["zone_id","time_bin"], how="left")
        .merge(dwell_stats,  on=["zone_id","time_bin"], how="left")
        .merge(in_stats,     on="zone_id",             how="left")
        .merge(out_stats,    on="zone_id",             how="left")
        .merge(trans_entropy,on="zone_id",             how="left")
        .merge(time_flags,   on="time_bin",            how="left")
        .merge(zone_priors,  on="zone_id",             how="left")
    )
    feat.fillna(0, inplace=True)
    logger.info(f"✔ Assembled features: {feat.shape[0]:,} rows × {feat.shape[1]} cols")

    # ───────────────────────── Summarize & Preview ─────────────────────────
    buf = io.StringIO()
    feat.info(buf=buf)
    logger.info("▶ Feature schema & non-null counts:\n" + buf.getvalue())


    logger.info("▶ First 200 rows (sorted by zone_id):")
    pd.set_option('display.max_columns', None)
    logger.info("\n" + feat.sort_values("zone_id").head(200).to_string(index=False))
    pd.reset_option('display.max_columns')

    # ───────────────────────── Save Output ─────────────────────────
    out_fp = FEATS_DIR / f"{stem}_features.parquet"
    feat.to_parquet(out_fp, index=False)
    logger.info(f"✔ Saved features to {out_fp}\n\n")


if __name__ == "__main__":
    for fp in BINS_DIR.glob("*.parquet"):
        process_file(fp)
