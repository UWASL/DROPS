#!/usr/bin/env bash


if [ $# -lt 1 ]; then
    echo "Usage: $0 exp.json"
    exit 1
fi

echo $1

../drops/build/drops "$1"
