#!/usr/bin/env bash

# delete old results if exists
rm -rf ./fig12
mkdir ./fig12

CONFIG="./config/fig12.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig12-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig12/log.txt

# remove unneeded results
rm -rf ./fig12/*sub*

# plot results
python3 ./scripts/fig12a_plt.py --failure_csv ./fig12/cost.csv --cost_dir ./fig12/ --out ./fig12a.pdf
