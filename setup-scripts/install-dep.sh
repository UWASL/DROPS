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

sudo apt update

# dotnet sdk
sudo apt install -y software-properties-common
sudo add-apt-repository -y ppa:dotnet/backports
sudo apt update
sudo apt install -y dotnet-sdk-9.0

# python deps
sudo apt install -y python3-pip
pip3 install --user numpy
pip3 install --user pandas
pip3 install --user matplotlib

