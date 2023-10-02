#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH --ntasks=4
#SBATCH --ntasks-per-core=1

export PYTHONUNBUFFERED=TRUE
srun sage -python ~/cornifer/scripts/test.py