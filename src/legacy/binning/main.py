from scripts.common_imports import *
from scripts.binning.spatial_binning import add_zone_id
from scripts.binning.time_binning import add_time_bin
from scripts.binning.speed_binning import add_speed_id, analyze_speed_bins
from scripts.binning.transition_matrix import build_transition_matrix

# Constants
TOTAL_PARTS = 32
BINNING_GRID_PATH = "maps/binning_2000_m2.geojson"  # Spatial bins
TIME_INTERVAL_MINUTES = 30                      # Temporal binning
# MAX_SPEED_THRESHOLD = 120                       # km/h for filtering


def process_one_part(day_str, part_id, grid_gdf):
    input_path = Path(f"denoised_output/{day_str}/denoised_part_{part_id}.parquet")
    if not input_path.exists():
        logging.warning(f"Missing file {input_path}, skipping...")
        return None

    ddf = dd.read_parquet(input_path)

    # Add spatial and temporal bins
    ddf = add_zone_id(ddf, grid_gdf)
    ddf = add_time_bin(ddf, interval_minutes=TIME_INTERVAL_MINUTES)

    return ddf


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if len(sys.argv) != 2:
        print("Usage: python main_binning.py <day_str>")
        sys.exit(1)

    day_str = sys.argv[1]

    logging.info(f"==== Starting full binning pipeline for {day_str} ====")

    grid_gdf = gpd.read_file(BINNING_GRID_PATH)

    partial_results = []
    for part_id in range(TOTAL_PARTS):
        logging.info(f"Processing part {part_id}")
        ddf = process_one_part(day_str, part_id, grid_gdf)
        if ddf is not None:
            partial_results.append(ddf)

    if not partial_results:
        logging.error("No valid parts found. Exiting.")
        sys.exit(1)

    logging.info("Concatenating all parts...")
    full_ddf = dd.concat(partial_results)

    logging.info("Computing speeds and filtering...")
    filtered_ddf = add_speed_id(full_ddf)

    logging.info("Building transition matrix...")
    transition_df = build_transition_matrix(filtered_ddf)

    output_dir = Path(f"binning_output/{day_str}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # transition_df.to_parquet(output_dir / "transition_matrix.parquet", index=False)

    logging.info(f"Finished full binning pipeline for {day_str}")


if __name__ == "__main__":
    main()
