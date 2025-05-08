#Create sif: 
##Barebones, not enough for the project:
singularity build modes.sif docker://continuumio/anaconda3 
##Better build, with possibility of adding packages via environment.yml:
singularity build modes.sif modes.def
#Access sif shell
singularity shell modes.sif

#Scripts:
##Split_parquets (uses login-node...):
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/split_parquets.py 
##Denoise_batch_split calls: 
sbatch scripts/preprocessing/denoise_batch_split.sh 20230327
sbatch scripts/preprocessing/denoise_batch_split.sh 20230328
sbatch scripts/preprocessing/denoise_batch_split.sh 20230329
sbatch scripts/preprocessing/denoise_batch_split.sh 20230330
sbatch scripts/preprocessing/denoise_batch_split.sh 20230331
sbatch scripts/preprocessing/denoise_batch_split.sh 20230401
sbatch scripts/preprocessing/denoise_batch_split.sh 20230402
###Checking the results of denoising + Stats of original data:
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230327 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230328 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230329 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230330 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230331 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230401 0
singularity exec modes.sif env PYTHONPATH=. python scripts/preprocessing/stats.py 20230402 1

##Merge_parts:

##Creating spatial-grid:
singularity exec modes.sif env PYTHONPATH=. python scripts/binning/

