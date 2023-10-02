#!/bin/bash
#SBATCH -n 4

export PYTHONUNBUFFERED=TRUE
sage -python ~/cornifer/scripts/test.py