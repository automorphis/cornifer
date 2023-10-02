#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH -n 4

srun sage -python ~/cornifer/scripts/test.py