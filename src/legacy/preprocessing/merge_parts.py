from common_imports import *

def merge_day(day_str):
    input_dir = Path(f"denoised_output/{day_str}")
    parts = sorted(input_dir.glob("denoised_part_*.parquet"))

    if not parts:
        print(f"No parts found for {day_str}. Skipping.")
        return

    dfs = []
    for p in parts:
        dfs.append(pd.read_parquet(p))
    
    merged = pd.concat(dfs, ignore_index=True)
    output_file = Path(f"denoised_output/{day_str}_full.parquet")
    merged.to_parquet(output_file, index=False)
    print(f"Merged {len(parts)} parts into {output_file}")

def main():
    days = ["20230327", "20230328", "20230329", "20230330", "20230331", "20230401", "20230402"]
    for day in days:
        merge_day(day)

if __name__ == "__main__":
    main()
