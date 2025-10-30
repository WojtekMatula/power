#!/bin/bash

# Define icons for success and error
CHECK="✅"
CROSS="❌"

# Define the environment name
ENV_NAME="power"

source $(conda info --base)/etc/profile.d/conda.sh
conda activate ${ENV_NAME}

# Interactive prompts for parameters with validation

# Target

echo "--------------------------------------------------------------------------------------"
echo "This script will train the model with provided configuration and evaluate the results."
echo "Features used to train the model are defined in *** features_map.csv *** file."
echo "--------------------------------------------------------------------------------------"
echo "Available targets: spread, bilans_price (default: bilans_price)"
while true; do
    read -p "Enter target: " target
    if [ -z "$target" ]; then
        target="bilans_price"
        break
    elif [[ "$target" == "spread" || "$target" == "bilans_price" ]]; then
        break
    else
        echo "Invalid target. Please enter 'spread' or 'bilans_price'."
    fi
done

# Train days
echo "Train days: positive integer (default: 90)"
while true; do
    read -p "Enter train_days: " train_days
    if [ -z "$train_days" ]; then
        train_days=90
        break
    elif [[ "$train_days" =~ ^[0-9]+$ ]] && [ "$train_days" -gt 0 ]; then
        break
    else
        echo "Invalid input. Please enter a positive integer."
    fi
done

# Weight type
echo "Available weight_types: linear, exp, none (default: exp)"
while true; do
    read -p "Enter weight_type: " weight_type
    if [ -z "$weight_type" ]; then
        weight_type="exp"
        break
    elif [[ "$weight_type" == "linear" || "$weight_type" == "exp" || "$weight_type" == "none" ]]; then
        break
    else
        echo "Invalid weight_type. Please enter 'linear', 'exp', or 'none'."
    fi
done

# Charts
echo "Generate charts? y/n (default: y)"
while true; do
    read -p "Enter (y/n): " chart_input
    chart_input=${chart_input:-y}
    if [[ $chart_input =~ ^[Yy]$ ]]; then
        charts_flag="--charts"
        show_charts=true
        break
    elif [[ $chart_input =~ ^[Nn]$ ]]; then
        charts_flag=""
        show_charts=false
        break
    else
        echo "Invalid input. Please enter y or n."
    fi
done

# Run the training model script
echo "Running model training..."
python3 ./scripts/train_model.py --target $target --train_days $train_days --weight_type $weight_type

# Check if Python script failed (non-zero exit code)
if [ $? -ne 0 ]; then
    echo "${CROSS} Model training failed!"
    exit 1  # Exit with error status
fi
echo "${CHECK} Model training completed."

# If charts were requested, run the validation/show script
if [ "$show_charts" = true ]; then
    python3 ./scripts/evaluate_model.py --charts
    if [ $? -ne 0 ]; then
        echo "${CROSS} Model evaluate failed!"
        exit 1
    fi
  else
    python3 ./scripts/evaluate_model.py
    if [ $? -ne 0 ]; then
        echo "${CROSS} Model evaluate failed!"
        exit 1
    fi
fi
