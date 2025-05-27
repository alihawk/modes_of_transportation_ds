#!/bin/bash
#SBATCH --job-name=denoise_day
#SBATCH --output=logs/denoise_day_%A_%a.log
#SBATCH --error=logs/denoise_day_%A_%a.log
#SBATCH --array=0-31    # <-- 32 jobs total!
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --mem=40G
#SBATCH --time=02:00:00
#SBATCH --reservation=fri

# ========== Config ==========
TARGET_DAY=$1   # Day to be used Example: 20230327
TOTAL_PARTS=32  # Splits done to the data, consistent with scripts.preprocessing.split_parquets.py

# ========== Sanity Check ==========
if [ -z "$TARGET_DAY" ]; then
    echo "ERROR: No target day passed! Usage: sbatch run_denoise_day.sh 20230327"
    exit 1
fi

if [ -z "$SLURM_ARRAY_TASK_ID" ]; then
    echo "ERROR: SLURM_ARRAY_TASK_ID not set."
    exit 1
fi

PART_ID=$SLURM_ARRAY_TASK_ID

echo "[INFO] Processing TARGET_DAY=$TARGET_DAY, PART_ID=$PART_ID"

# ========== Run Python ==========
singularity exec modes.sif \
    env PYTHONPATH=. python scripts/preprocessing/denoise_split.py "$TARGET_DAY" "$PART_ID"
