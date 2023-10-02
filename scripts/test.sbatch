#!/bin/bash
#SBATCH --time=00:00:20
#SBATCH --ntasks=4
#SBATCH --ntasks-per-core=1

sage -python ~/cornifer/scripts/test.py