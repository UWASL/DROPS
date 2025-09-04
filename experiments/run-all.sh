#!/usr/bin/env bash

run() {
    script=$1

    echo "Starting the experiment: $script"
    start_time=$(date +%s)

    ./$script

    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "Experiment $script finished in ${duration} seconds."
    echo
}

# remove old figures
rm -rf ./*.pdf

run "scripts/fig6.sh"
run "scripts/fig7.sh"
run "scripts/fig10a.sh"
run "scripts/fig10b.sh"
run "scripts/fig11.sh"
run "scripts/fig12.sh"


