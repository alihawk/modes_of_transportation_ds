from scripts.common_imports import *

def compute_stats(df, label):
    stats = {}
    stats['label'] = label
    stats['n_points'] = len(df)
    stats['n_devices'] = df['deviceid'].nunique()
    stats['avg_points_per_device'] = round(stats['n_points'] / stats['n_devices'], 2)

    if 'datetime' in df.columns:
        duration = (df['datetime'].max() - df['datetime'].min()).total_seconds() / 3600
        stats['hours_covered'] = round(duration, 2)
    else:
        stats['hours_covered'] = None

    return stats

def compare_stats(original, cleaned):
    report = {}

    for key in ['n_points', 'n_devices', 'avg_points_per_device', 'hours_covered']:
        before = original[key]
        after = cleaned[key]
        if before and after:
            drop_pct = round((before - after) / before * 100, 2)
        else:
            drop_pct = None
        report[key] = {
            'before': before,
            'after': after,
            'reduction_%': drop_pct
        }

    return report

def all_stats():
    # Folder with all daily parquet files
    data_dir = Path("data")
    files = sorted(data_dir.glob("2023*.parquet"))

    daily_stats = []

    for f in files:
        df = pd.read_parquet(f, columns=["deviceid", "date", "time", "lon", "lat"], engine="fastparquet")
        date_str = f.stem  # '20230327'
        df["datetime"] = pd.to_datetime(date_str + " " + df["time"].astype("string"), format="%Y%m%d %H:%M:%S")
        df = df.dropna(subset=["datetime", "lat", "lon", "deviceid"])

        day = df["datetime"].dt.date.iloc[0]
        unique_devices = df["deviceid"].nunique()
        total_points = len(df)
        avg_points_per_device = total_points / unique_devices
        duration = (df["datetime"].max() - df["datetime"].min()).total_seconds() / 3600

        # Points per device stats
        pts_per_device = df.groupby("deviceid", observed=False).size()
        median_points = pts_per_device.median()
        std_points = pts_per_device.std()

        # Sampling rate stats
        df = df.sort_values(["deviceid", "datetime"])
        df["delta"] = df.groupby("deviceid", observed=False)["datetime"].diff().dt.total_seconds()
        mean_delta = df["delta"].mean()
        median_delta = df["delta"].median()
        std_delta = df["delta"].std()

        # Spatial coverage
        lat_min, lat_max = df["lat"].min(), df["lat"].max()
        lon_min, lon_max = df["lon"].min(), df["lon"].max()
        area_km2 = (lat_max - lat_min) * (lon_max - lon_min) * 111 * 111

        # Day vs Night
        df["hour"] = df["datetime"].dt.hour
        day_ratio = (df["hour"].between(6, 20)).mean() * 100
        night_ratio = 100 - day_ratio

        # Null counts
        nulls = df[["deviceid", "lat", "lon", "datetime"]].isnull().sum().to_dict()

        daily_stats.append({
            "date": str(day),
            "total_points": total_points,
            "unique_devices": unique_devices,
            "avg_points_per_device": round(avg_points_per_device, 2),
            "median_points_per_device": median_points,
            "std_points_per_device": std_points,
            "mean_sampling_interval_s": round(mean_delta, 2),
            "median_sampling_interval_s": round(median_delta, 2),
            "std_sampling_interval_s": round(std_delta, 2),
            "hours_covered": round(duration, 2),
            "day_ratio_%": round(day_ratio, 2),
            "night_ratio_%": round(night_ratio, 2),
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "area_est_km2": round(area_km2, 2),
            "null_counts": nulls
        })

    # Weekly summary
    stats_df = pd.DataFrame(daily_stats)
    stats_df.to_csv("data/summary_each_day.csv")

    stats_df["date"] = pd.to_datetime(stats_df["date"])

    weekly = {
        "date_range": f"{stats_df['date'].min().date()} → {stats_df['date'].max().date()}",
        "total_points": stats_df["total_points"].sum(),
        "unique_devices_total": stats_df["unique_devices"].sum(),
        "avg_points_per_day": round(stats_df["total_points"].mean(), 2),
        "avg_devices_per_day": round(stats_df["unique_devices"].mean(), 2),
        "total_hours_covered": round(stats_df["hours_covered"].sum(), 2),
        "avg_daytime_ratio_%": round(stats_df["day_ratio_%"].mean(), 2),
        "avg_area_km2": round(stats_df["area_est_km2"].mean(), 2)
    }

    week_df = pd.DataFrame([weekly])
    week_df.to_csv("data/summary_week.csv")

    # Display
    print("📅 Daily Stats:")
    pprint(daily_stats)
    print("\n📊 Weekly Summary:")
    pprint(weekly)

def main():
    # === Load files ===
    day_str = sys.argv[1]
    all = int(sys.argv[2])
    data_dir = Path(f"data_split/{day_str}")
    files = sorted(data_dir.glob("part*.parquet"))
    denoised_files = [Path(str(f).replace("data_split", "denoised_output")) for f in files]
    for original, denoised in zip(files, denoised_files):
        print(f"Original: {original}")
        input_original = original
        print(f"Denoised: {denoised}")
        input_denoised = denoised
        print()
        df_before = pd.read_parquet(input_original, columns=["deviceid", "datetime", "lon", "lat"])
        df_after = pd.read_parquet(input_denoised, columns=["deviceid", "datetime", "lon", "lat"])

        # === Compute statistics ===
        stats_before = compute_stats(df_before, label="Original")
        stats_after = compute_stats(df_after, label="Denoised")

        # === Compare ===
        comparison = compare_stats(stats_before, stats_after)

        # === Report ===
        print("\n=== Denoising Report ===")
        print("\nOriginal Dataset:")
        pprint(stats_before)
        print("\nDenoised Dataset:")
        pprint(stats_after)
        print("\nComparison:")
        pprint(comparison)
    if all==1:
        all_stats()

if __name__ == "__main__":
    main()
