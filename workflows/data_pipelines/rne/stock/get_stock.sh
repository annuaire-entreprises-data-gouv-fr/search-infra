#!/bin/bash

# Check if the required arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <TMP_FOLDER> <FTP_URL>"
    exit 1
fi

# Assign arguments to variables
TMP_FOLDER="$1"
FTP_URL="$2"

# Use lftp to connect to the FTP server, list files, find the one containing "formalité", and download it
lftp <<EOF
open $FTP_URL
set ftp:ssl-allow no
set ssl:verify-certificate no
glob -a get *formalité* -o "$TMP_FOLDER/stock_rne.zip"
quit
EOF

# Check if the FTP operation was successful
if [ $? -eq 0 ]; then
    echo "File containing 'formalité' downloaded successfully to $TMP_FOLDER/stock_rne.zip"
else
    echo "FTP download failed."
    exit 1
fi
