zone_ids = df["zone_id"].to_numpy()
time_bins = df["time_bin"].to_numpy()
dc = df["device_change"].to_numpy()

# Create arrays for the next row's values using roll
zone_next = np.roll(zone_ids, -1)
time_bin_next = np.roll(time_bins, -1)