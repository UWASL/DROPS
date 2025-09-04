#!/usr/bin/env bash

# remove old results
rm -rf ./fig10b
mkdir ./fig10b

CONFIG="./config/fig10b.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig10b-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig10b/log.txt

# plot results
python3 ./scripts/fig10b_plt.py --failure_csv ./fig10b/cost.csv --cost_dir ./fig10b/ --out ./fig10b.pdf