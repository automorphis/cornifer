#!/bin/bash
#SBATCH -n 1

export PYTHONUNBUFFERED=TRUE
sage -python ~/cornifer/scripts/test.py