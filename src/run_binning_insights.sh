#!/bin/bash
#SBATCH --job-name=binning_insights
#SBATCH --cpus-per-task=1       
#SBATCH --mem=64G                
#SBATCH --time=02:00:00

module load Anaconda3 
python binning_insights.py

echo "===== JOB END ($EXIT) ===== at $(date)"
exit $EXIT
