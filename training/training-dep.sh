#!/usr/bin/env bash

# -------- OS check --------
OS_ID=""; OS_VER=""
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  OS_ID="${ID:-}"; OS_VER="${VERSION_ID:-}"
fi
if [ "$OS_ID" != "ubuntu" ] || [ "$OS_VER" != "22.04" ]; then
  echo "WARNING: Intended for Ubuntu 22.04. Detected: ${OS_ID:-unknown} ${OS_VER:-unknown}"
fi


apt update

pip install -U pip
pip install -U setuptools wheel
pip install autogluon --extra-index-url https://download.pytorch.org/whl/cpu

pip3 install --user numpy
pip3 install --user pandas



