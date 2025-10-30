#!/bin/bash

# Define icons for success and error
CHECK="✅"
CROSS="❌"

echo "Setting up project environment..."

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo "${CROSS} Conda is not installed. Please install Anaconda or Miniconda first."
    exit 1
fi

echo "${CHECK} Conda found."

# Define the environment name
ENV_NAME="power"

# Check if the environment already exists
if conda env list | grep -q "^${ENV_NAME}\s"; then
    echo "${CHECK} Conda environment '${ENV_NAME}' already exists."
    # Suppress all output from conda update
    conda env update -n ${ENV_NAME} -f environment.yml > /dev/null 2>&1
    ACTION="updated"
else
    # Suppress all output from conda create
    conda env create -f environment.yml > /dev/null 2>&1
    echo "${CHECK} Conda environment '${ENV_NAME}' created from environment.yml."
    ACTION="created"
fi

# Check if the last command (create or update) was successful
if [ $? -eq 0 ]; then
    echo "${CHECK} Conda environment '${ENV_NAME}' ${ACTION} successfully."

    # =================================================================
    # NEW: Setup .env file
    # =================================================================
    echo "Setting up project configuration (.env file)..."
    ENV_FILE=".env"

    # Check if .env already exists and ask if the user wants to overwrite
    if [ -f "$ENV_FILE" ]; then
        echo "${CROSS} A .env file already exists."
        read -p "Do you want to overwrite it? (y/n): " overwrite
        if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
            echo "Skipping .env file creation."
        else
            # Proceed with creation
            SETUP_ENV=true
        fi
    else
        SETUP_ENV=true
    fi

    if [ "$SETUP_ENV" = true ]; then
        echo "Please provide the following values for your .env file:"
        read -p "Enter MC_FUNCTION_APP_URL: " MC_FUNCTION_APP_URL
        read -p "Enter MC_FUNCTION_CODE: " MC_FUNCTION_CODE
        read -p "Enter JWM_USERNAME: " JWM_USERNAME
        read -p "Enter JWM_PASSWORD: " JWM_PASSWORD
        echo # Newline after password input

        # Create the .env file, overwriting if it exists
        echo "# Environment variables for the project" > "$ENV_FILE"
        echo "MC_FUNCTION_APP_URL=\"${MC_FUNCTION_APP_URL}\"" >> "$ENV_FILE"
        echo "MC_FUNCTION_CODE=\"${MC_FUNCTION_CODE}\"" >> "$ENV_FILE"
        echo "JWM_USERNAME=\"${JWM_USERNAME}\"" >> "$ENV_FILE"
        echo "JWM_PASSWORD=\"${JWM_PASSWORD}\"" >> "$ENV_FILE"

        echo "${CHECK} .env file created successfully."

        # HELPER: Add .env to .gitignore if not present
        GITIGNORE_FILE=".gitignore"
        if [ -f "$GITIGNORE_FILE" ]; then
            if ! grep -q "^\.env$" "$GITIGNORE_FILE"; then
                echo ".env" >> "$GITIGNORE_FILE"
                echo "${CHECK} Added '.env' to .gitignore."
            fi
        else
            echo ".env" > "$GITIGNORE_FILE"
            echo "${CHECK} Created .gitignore and added '.env' to it."
        fi
    fi
    # =================================================================
    # END: .env file setup
    # =================================================================

    # Create output directory if it doesn't exist
    mkdir -p ./scripts
    mkdir -p ./out
    mkdir -p ./out/html
    echo "${CHECK} Created required catalog structure."

    echo "Setup complete. Your environment is ready to use."
    echo "Run 'conda activate ${ENV_NAME}' to activate it."
else
    echo "${CROSS} Failed to ${ACTION%ed} environment."
    exit 1
fi
