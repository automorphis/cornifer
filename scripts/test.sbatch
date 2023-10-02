#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH -n 1
#SBATCH -c 4

srun sage -python ~/cornifer/scripts/test.py