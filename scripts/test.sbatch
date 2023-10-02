#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time=00:00:20
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --error=~/error_test.txt

sage -python ~/cornifer/scripts/test.py

