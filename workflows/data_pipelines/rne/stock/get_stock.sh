#!/bin/bash

# Check if the required arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <TMP_FOLDER> <FTP_URL>"
    exit 1
fi

# Assign arguments to variables
TMP_FOLDER="$1"
FTP_URL="$2"

# Use lftp to connect to the FTP server and download the file
lftp -e "get '$FTP_URL' -o '$TMP_FOLDER/stock_rne.zip'; quit"

# Check if the FTP operation was successful
if [ $? -eq 0 ]; then
    echo "File downloaded successfully to $TMP_FOLDER."
else
    echo "FTP download failed."
    exit 1
fi
