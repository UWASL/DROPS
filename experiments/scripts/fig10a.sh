#!/usr/bin/env bash
# fig 10a
# delete old results if exists
rm -rf ./fig10a
mkdir ./fig10a

# choose config file
CONFIG="./config/fig10a.json"
if [[ "${1-}" == "fast" ]]; then
    CONFIG="./config/fig10a-fast.json"
fi

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./fig10a/log.txt

# plot results
python3 ./scripts/fig10a_plt.py --failure_csv ./fig10a/cost.csv --cost_dir ./fig10a/ --out ./fig10a.pdf
