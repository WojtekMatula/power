#!/bin/bash

# Define icons for success and error
CHECK="✅"
CROSS="❌"

# Define the environment name
ENV_NAME="power"
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ${ENV_NAME}
# Merge dataframes
echo "Merging dataframes..."
python3 ./scripts/merge_dataframes.py
# Check if Python script failed (non-zero exit code)
if [ $? -ne 0 ]; then
    echo "${CROSS} Merging dataframes failed - duplicates detected!"
    exit 1  # Exit with error status
fi
echo "${CHECK} Dataframes merge completed."

echo "Feature engineering..."

python3 ./scripts/feature_engineering.py

