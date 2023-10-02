#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1

srun sage -python ~/cornifer/scripts/test.py