#!/bin/bash
#SBATCH --job-name=run_notebooks
#SBATCH --output=notebooks_output_%j.log
#SBATCH --error=notebooks_error_%j.log
#SBATCH --time=10:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G

# Go to project directory
cd /d/hpc/home/ks4681/modes_of_transportation_ds

# Build singularity container if it doesn't exist
if [ ! -f modes.sif ]; then
    echo "Building Singularity container..."
    singularity build modes.sif modes.def
fi

# Run notebooks directly using jupyter nbconvert --execute
echo "Running Sequential.ipynb..."
singularity exec modes.sif jupyter nbconvert --to notebook --execute --inplace Sequential.ipynb
if [ $? -ne 0 ]; then
    echo "Error running Sequential.ipynb"
    exit 1
fi

echo "Running Binning_sequential.ipynb..."
singularity exec modes.sif jupyter nbconvert --to notebook --execute --inplace Binning_sequential.ipynb
if [ $? -ne 0 ]; then
    echo "Error running Binning_sequential.ipynb"
    exit 1
fi

echo "All notebooks completed successfully!"