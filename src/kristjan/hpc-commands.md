# build .sif, you need to create .def first.
singularity build modes.sif modes.def

# shell
singularity shell modes.sif

# run sbatch command
sbatch run_notebooks.sh
(you get back the job ID)

# check job
squeue -j `job_id`

R-running
PD-pending

squeue -u $USER

scancel JOB_ID

srun --partition=all --time=2:00:00 --mem=80G --pty bash wn104
srun --partition=all --time=2:00:00 --ntasks=1 --cpus-per-task=8 --mem=80G --pty bash

ssh -N -L 8891:wn106:8891 ks4681@hpc-login.arnes.si

jupyter notebook --no-browser --port=8891 --ip=0.0.0.0