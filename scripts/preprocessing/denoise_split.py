from scripts.common_imports import *
from scripts.preprocessing.denoise import denoise_partition

def main():
    logging.basicConfig(
        stream=sys.stdout, 
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    if len(sys.argv) != 3:
        logging.error("Usage: python denoise_split.py <day_str> <part_id>")
        sys.exit(1)

    day_str = sys.argv[1]
    part_id = int(sys.argv[2])

    input_path = Path(f"data_split/{day_str}/part_{part_id}.parquet")
    output_dir = Path(f"denoised_output/{day_str}")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"part_{part_id}.parquet"

    if not input_path.exists():
        logging.error(f"Input file {input_path} does not exist!")
        sys.exit(1)

    logging.info(f"Loading {input_path}")
    ddf = dd.read_parquet(input_path, columns=["deviceid", "datetime", "lon", "lat"])

    logging.info(f"Applying denoise to part {part_id} with map_partitions")
    cleaned_ddf = ddf.map_partitions(denoise_partition)

    schema = pa.schema([
            ("deviceid", pa.string()),
            ("lon", pa.float64()),
            ("lat", pa.float64()),
            ("datetime", pa.timestamp('ns'))
        ])

    # Compute first to memory
    cleaned_df = cleaned_ddf.compute()

    # Save with pandas
    cleaned_df.to_parquet(output_file, index=False, engine="pyarrow", schema=schema)
    logging.info(f"Saved cleaned file to {output_file}")

if __name__ == "__main__":
    main()
