#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH -n 4
#SBATCH -c 1

srun sage -python ~/cornifer/scripts/test.py