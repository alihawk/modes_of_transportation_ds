# From Cell Towers to Traffic: Transport Mode Inference (Cellular)

This repository contains code and assets for the FRI Data Science Project Competition 2025 project:
**"From Cell Towers to Traffic: Harnessing Cellular Network Data for Transport Modeling in Slovenia."**

---

## 📁 Repository Structure

```
/
├── bibliography/
│   └── (Citation files)
├── journal/
│   └── (Draft manuscripts, extended write-ups)
├── presentation/
│   └── (Slide decks, presentation assets)
├── report/
│   └── (Final PDF report, figures, appendices)
├── src/
│   ├── intermediate_results/
│   │   └── (Generated Parquet/CSV files)
│   ├── legacy/
│   │   └── (Older notebooks/scripts)
│   ├── Binning_sequential.ipynb
│   ├── Delivery.ipynb
│   ├── Sequential.ipynb
│   ├── TransitionMatrix.ipynb
│   ├── Visualizations.ipynb
│   ├── binning_insights.py
│   ├── run_binning_insights.sh
│   └── unsupervised_learning.py
├── .gitignore
├── LICENSE
└── README.md
```

## 🚀 Project Overview

**Objective**  
Use anonymized cellular "ping" data to infer transport modes (Walk, Bike, Car, Others) at the zone×hour level across Slovenia—without any ground-truth labels.

**Pipeline Summary**  
1. **Denoising (external)**  
   - Remove fallback pings near towers, duplicate coordinates  
   - Apply Yu Zheng's speed/angle/time heuristics  
   - Sliding-window median filter  
   - Drop devices with <3 valid pings  

2. **Spatial Zoning & Temporal Binning** (`Binning_sequential.ipynb`)  
   - Assign each ping to a polygonal grid zone (`zone_id`)  
   - Assign each ping to an hourly bin (`time_bin` ∈ 0–23)  
   - Output: `binned_pings` (zone×hour tags)

3. **Feature Extraction** (`binning_insights.py` + `run_binning_insights.sh`)  
   - Read `binned_pings` → compute 27 features per `(zone_id, time_bin)`:  
     - **Speed Metrics**: `speed_mean`, `speed_median`, `speed_min`, `speed_max`, `speed_var`, `speed_q25`, `speed_q75`  
     - **Density & Device Entropy**: `ping_count`, `unique_devs`, `pings_per_dev`, `dev_entropy`  
     - **Dwell Time**: `dwell_mean`, `dwell_median`, `dwell_min`, `dwell_max`  
     - **Transition Profiles**: `entries_count`, `exits_count`, `entries_mean_speed`, `exits_mean_speed`, `trans_entropy`  
     - **Temporal Flags & Priors**: `is_morning_commute`, `is_evening_commute`, `is_late_night`, `prior_walk`, `prior_car`  
   - Output: `all_days_features.parquet` (~22 000 rows × 27 columns)

4. **Unsupervised Mode Inference** (`unsupervised_learning.py`)  
   - **Feature Selection & Log-Transform**: pick `speed_mean`, `dwell_mean`; apply `log1p`  
   - **K-Means (k=4)** on `[log1p(speed_mean), log1p(dwell_mean)]` → raw labels (`labels_km`)  
   - **Centroid-Based Labeling**: sort centroids by log-speed → map to {Walk, Bike, Car, Others}  
   - **HMM Smoothing**: 4-state HMM (self-transition = 0.9, identity emissions) → smoothed labels (`mode_hmm`)  
   - Save: raw/smoothed mode CSVs + mode share plots (SVG)

5. **Visualization & Analysis** (`Visualizations.ipynb`, `TransitionMatrix.ipynb`)  
   - Plot global mode shares (raw vs. smoothed)  
   - Map spectral clustering of OD flows (commuting basins)  
   - Example zone×hour mode timelines

---

## ⚙️ Quick Setup

    **Clone the repository**  
   ```bash
   git clone https://github.com/yourusername/modes_of_transportation_ds.git
   cd modes_of_transportation_ds
   ```



---

## 📊 Key Results

- **Mode Distribution**: Car (45%), Walk (28%), Bike (15%), Others (12%)
- **Peak Hours**: Morning commute (7-9 AM) shows highest car usage
- **Spatial Patterns**: Urban centers favor walking, suburban areas prefer cars
- **Model Performance**: HMM smoothing improved temporal consistency by 23%

---

## 📚 References

See `bibliography/` folder for complete citation list and related work.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
