#!/usr/bin/env bash

# delete old results if exists
rm -rf ./fig6
mkdir ./fig6

# choose config file
CONFIG="./config/fig6.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig6-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig6/log.txt

# remove unneeded results
rm -rf ./fig6/*sub*

# plot results
python3 ./scripts/fig6a_plt.py --failure_csv ./fig6/cost.csv --cost_dir ./fig6/ --out ./fig6a.pdf
python3 ./scripts/fig6b_plt.py --latency_dir ./fig6/ --out ./fig6b.pdf --logx
