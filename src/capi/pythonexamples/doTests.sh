#!/bin/bash
#

set -x
set -o errexit

python sample.py
./run_pythonsample2.sh
./run_pythonsample4.sh
