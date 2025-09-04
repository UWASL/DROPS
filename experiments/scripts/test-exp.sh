#!/usr/bin/env bash

# delete old results if exists
rm -rf ./test-exp
mkdir ./test-exp

# choose config file
CONFIG="./config/test.json"

# run experiment
./scripts/run-experiment.sh "$CONFIG" &> ./test-exp/log.txt

# remove unneeded results
rm -rf ./test-exp/*sub*

# plot results
python3 ./scripts/fig6a_plt.py --failure_csv ./test-exp/cost.csv --cost_dir ./test-exp/ --out ./test-exp/test_failure_rate.pdf
python3 ./scripts/fig6b_plt.py --latency_dir ./test-exp/ --out ./test-exp/test_latency.pdf --logx
