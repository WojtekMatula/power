#!/bin/bash

# Define icons for success and error
CHECK="✅"
CROSS="❌"

# Define the environment name
ENV_NAME="power"
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ${ENV_NAME}
echo "Feature engineering..."

python3 ./scripts/feature_engineering.py

