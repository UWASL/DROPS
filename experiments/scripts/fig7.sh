#!/usr/bin/env bash

# delete old results if exists
rm -rf ./fig7
mkdir ./fig7

# choose config file
CONFIG="./config/fig7.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig7-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig7/log.txt

# plot results
python3 ./scripts/fig7a_plt.py --failure_csv ./fig7/cost.csv --cost_dir ./fig7/ --out ./fig7a.pdf
python3 ./scripts/fig7b_plt.py --failure_csv ./fig7/cost.csv --cost_dir ./fig7/ --out ./fig7b.pdf
