#!/bin/bash
set -euo pipefail

cd ~/pt-llm-bot 2>/dev/null || cd /Users/wxp/dev/pt-llm-bot

git pull --rebase

export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate pt-llm

python bot.py
