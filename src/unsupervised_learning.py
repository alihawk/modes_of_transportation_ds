#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.cluster import KMeans
from hmmlearn import hmm

# ───────────────────────── Config ─────────────────────────
FEATURE_FILE = Path("data_bin_features") / "all_days_features.parquet"
OUT_DIR      = FEATURE_FILE.parent / "mode_inference_simple"
OUT_DIR.mkdir(exist_ok=True)
MODE_NAMES   = ["Walk", "Bike", "Car", "Others"]
N_CLUSTERS   = len(MODE_NAMES)

# ───────────────────── Load & Filter ─────────────────────
df = pd.read_parquet(FEATURE_FILE)
# drop any bins with non‐positive speeds (if you like) or keep all:
df = df[df.speed_mean >= 0.0].reset_index(drop=True)

# ────────────────────── Build Feature Matrix ──────────────────────
# we work in log‐space to tame heavy tails
X = np.column_stack([
    np.log1p(df["speed_mean"].values),
    np.log1p(df["dwell_mean"].values),
])

# ───────────────────────── K-Means ─────────────────────────
km = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
labels_km = km.fit_predict(X)

# map each cluster ID → a transport mode by sorting on centroid speed
centers = km.cluster_centers_[:, 0]             # first dimension = log-speed
order   = np.argsort(centers)                   # slowest→fastest
cluster_to_mode = { int(order[i]): MODE_NAMES[i] for i in range(N_CLUSTERS) }
df["mode_kmeans"] = [cluster_to_mode[c] for c in labels_km]

# ──────────────────── HMM Smoothing ────────────────────
model = hmm.CategoricalHMM(n_components=N_CLUSTERS, init_params="")
p_self = 0.9
cross  = (1 - p_self) / (N_CLUSTERS - 1)
tm     = np.full((N_CLUSTERS, N_CLUSTERS), cross)
np.fill_diagonal(tm, p_self)

model.startprob_    = np.full(N_CLUSTERS, 1.0 / N_CLUSTERS)
model.transmat_     = tm
model.emissionprob_ = np.eye(N_CLUSTERS)

labels_hmm = model.predict(labels_km.reshape(-1,1))
df["mode_hmm"] = [cluster_to_mode[c] for c in labels_hmm]

# ─────────────────────── Plotting ───────────────────────
def make_pretty_bar(counts, title, out_svg):
    fig, ax = plt.subplots(figsize=(6,4))
    bars = ax.bar(MODE_NAMES, counts, edgecolor="black")
    ax.set_ylabel("Percent of bins")
    ax.set_title(title)
    ax.set_ylim(0, 100)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    # label each bar
    for rect, pct in zip(bars, counts):
        ax.text(rect.get_x() + rect.get_width()/2, pct+1,
                f"{pct:.1f}%", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_svg)    # SVG for crisp lines
    plt.close(fig)
    print(f"✔ Saved {out_svg}")

# compute and plot
for col, svg, title in [
    ("mode_kmeans", OUT_DIR/"global_kmeans.svg", "Global K-Means Mode Distribution"),
    ("mode_hmm",     OUT_DIR/"global_hmm.svg",     "Global HMM-Smoothed Mode Distribution")
]:
    pct = (df[col]
           .value_counts(normalize=True)
           .reindex(MODE_NAMES, fill_value=0) * 100)
    make_pretty_bar(pct.values, title, svg)
