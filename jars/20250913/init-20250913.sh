#!/bin/bash

# Variables
ARTIFACTORY_URL="https://jfrog.clalit.org.il:443/artifactory/vsc_extensions" # Replace with your Artifactory URL
# ARTIFACTORY_REPO="vsc_extensions" # Replace with your Artifactory repository
EXTENSIONS_PATH="fastpi" # Path in Artifactory where vsix files are stored

# Create a temporary directory for downloading files
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR" || { echo "Failed to change directory to $TEMP_DIR"; exit 1; }

# Fetch the list of .vsix files from the specified Artifactory folder
response=$(curl -s -k -L "$ARTIFACTORY_URL/$EXTENSIONS_PATH")

# Extract file names using grep
file_list=$(echo "$response" | grep -oP '(?<=href=")[^"]*.vsix')

# Check if any files were found
if [ -z "$file_list" ]; then
    echo "No .vsix files found or empty response."
    exit 1
fi

# Download each .vsix file
for file_name in $file_list; do
    file_url="$ARTIFACTORY_URL/$EXTENSIONS_PATH/$file_name"
    echo "Downloading $file_url ..."
    curl -s -LOk "$file_url" || { echo "Failed to download $file_url"; exit 1; }
done

# Install each .vsix file
for vsix_file in *.vsix; do
    if [ -f "$vsix_file" ]; then
        echo "Installing $vsix_file ..."
        ~/.vscode-server*/bin/*/bin/code-server --install-extension "$vsix_file"
        rm "$vsix_file" # Clean up the .vsix file after installation
    else
        echo "No .vsix files found to install."
    fi
done

# Clean up the temporary directory
cd ..
rm -rf "$TEMP_DIR"

echo "All done!"