#!/bin/bash

# Define icons for success and error
CHECK="✅"
CROSS="❌"

echo "Starting data download..."

# Define the environment name
ENV_NAME="power"
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ${ENV_NAME}
# Run kse_load_forecast
echo "Running kse_load_forecast.py..."
python3 ./scripts/kse_load_forecast.py
if [ $? -eq 0 ]; then
    echo "${CHECK} kse_load_forecast.py completed successfully."
else
    echo "${CROSS} kse_load_forecast.py failed."
    exit 1
fi

# Run peak_hours
echo "Running peak_hours_actual.py..."
python3 ./scripts/peak_hours_actual.py
if [ $? -eq 0 ]; then
    echo "${CHECK} peak_hours_actual.py completed successfully."
else
    echo "${CROSS} peak_hours_actual.py failed."
    exit 1
fi

# Run pk5y_actual
echo "Running pk5y_actual.py..."
python3 ./scripts/pk5y_actual.py
if [ $? -eq 0 ]; then
    echo "${CHECK} pk5y_actual.py completed successfully."
else
    echo "${CROSS} pk5y_actual.py failed."
    exit 1
fi

# Run pk5y_forecast
echo "Running pk5y_forecast.py..."
python3 ./scripts/pk5y_forecast.py
if [ $? -eq 0 ]; then
    echo "${CHECK} pk5y_forecast.py completed successfully."
else
    echo "${CROSS} pk5y_forecast.py failed."
    exit 1
fi

# Run prices
echo "Running prices_pse.py..."
python3 ./scripts/prices_pse.py
if [ $? -eq 0 ]; then
    echo "${CHECK} prices_pse.py completed successfully."
else
    echo "${CROSS} prices_pse.py failed."

    exit 1
fi

echo "All data download completed successfully."
