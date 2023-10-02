#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time=00:00:10
#SBATCH --ntasks=4
#SBATCH --ntasks-per-core=1
#SBATCH --error=~/error_test.txt

srun sage -python -u ~/cornifer/scripts/test.py > ~/output_test.txt

