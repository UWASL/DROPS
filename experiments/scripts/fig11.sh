#!/usr/bin/env bash

# delete old results if exists
rm -rf ./fig11
mkdir ./fig11

CONFIG="./config/fig11.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig11-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig11/log.txt

# remove unneeded results
rm -rf ./fig11/*sub*

# plot results
python3 ./scripts/fig11a_plt.py --failure_csv ./fig11/cost.csv --cost_dir ./fig11/ --out ./fig11a.pdf
python3 ./scripts/fig11b_plt.py --latency_dir ./fig11/ -o ./fig11b.pdf --logx
