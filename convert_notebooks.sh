#!/bin/bash
# Define icons for success and error
CHECK="✅"
CROSS="❌"
# Define the environment name
ENV_NAME="power"
# Script name and purpose
SCRIPT_NAME="convert_notebooks.sh"
echo "Running $SCRIPT_NAME - Converting Jupyter notebooks to Python scripts"                                                                                                                                                                                      
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ${ENV_NAME}
# Check if jupytext is installed in the activated environment
if command -v jupytext &> /dev/null; then
    # Check if there are any .ipynb files to convert
    if ls ./notebooks/*.ipynb 1> /dev/null 2>&1; then

        # Find all .ipynb files and convert them silently
        find ./notebooks -name "*.ipynb" -type f -print0 | while IFS= read -r -d '' file; do
            filename=$(basename "$file" .ipynb)
            output_path="./scripts/${filename}.py"
            # Suppress output for each conversion
            jupytext --to py "$file" -o "$output_path" > /dev/null 2>&1
        done

        echo "${CHECK} Notebooks converted to python successfully."
    else
        echo "${CROSS} No Jupyter notebooks (.ipynb) found in ./notebooks to convert."
    fi
else
    echo "${CROSS} jupytext is not found in the '${ENV_NAME}' environment."
    echo "    Please install it with: conda install -c conda-forge jupytext"
fi

