#!/bin/bash
set -euo pipefail

cd ~/pt-llm-bot

git stash
git pull --rebase
git stash pop || true

export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate pt-llm

python bot.py