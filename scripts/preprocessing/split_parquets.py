from scripts.common_imports import *

def split_parquet(input_file: Path, output_folder: Path, n_splits: int = 32):
    if not input_file.exists():
        logging.error(f"Input file {input_file} does not exist.")
        return
    day_str = input_file.stem
    formatted_date = f"{day_str[:4]}-{day_str[4:6]}-{day_str[6:]}"
    
    logging.info("Reading .Parquet file")
    df = pd.read_parquet(input_file, columns=["deviceid", "time", "lon", "lat"])
    
    logging.info("Merging date and time into datetime")
    df["datetime"] = pd.to_datetime(
        formatted_date + " " + df["time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )
    
    logging.info("Dropping 'time' column")
    df = df.drop(columns=["time"])
    parts = split_by_device(df, n_parts=32)
    output_folder.mkdir(parents=True, exist_ok=True)

    for idx, part in enumerate(parts):
        part_path = output_folder / f"part_{idx}.parquet"
        part.to_parquet(part_path, index=False)
        logging.info(f"Saved split part {idx} to {part_path}")
        
def split_by_device(df, n_parts=32):
    # Sort properly    
    df = df.sort_values(["deviceid", "datetime"])

    # Group by device
    groups = list(df.groupby("deviceid", observed=False))

    partitions = [[] for _ in range(n_parts)]
    sizes = [0] * n_parts  # Track partition sizes

    for device_id, group in groups:
        # Assign device group to the partition with currently least rows
        idx = sizes.index(min(sizes))
        partitions[idx].append(group)
        sizes[idx] += len(group)

    # Combine lists
    final_parts = [pd.concat(part) for part in partitions]
    return final_parts

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    input_dir = Path("data")
    output_root = Path("data_split")
    files = list(input_dir.glob("*.parquet"))

    if not files:
        logging.error("No Parquet files found in data/. Exiting.")
        return

    for file in files:
        day_str = file.stem
        logging.info(f"Splitting day {day_str}")
        output_folder = output_root / day_str
        split_parquet(file, output_folder, n_splits=32)
    logging.info("Finished splitting all days.")

if __name__ == "__main__":
    main()
