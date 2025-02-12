#!/bin/bash

# 1 for loop, 2 for one-shot mode, 3 for one-shot mode for testing 
# Use first argument if provided
MODE=${1:-2}

# Read env file from the same directory
if [ "$MODE" = "3" ]; then
    if [ -f "$(dirname "$0")/test.env" ]; then
        source "$(dirname "$0")/test.env"
        echo "Loaded environment variables from test.env file"
    else
        echo "Error: test.env file not found in the same directory"
        exit 1
    fi
else
    if [ -f "$(dirname "$0")/prod.env" ]; then
        source "$(dirname "$0")/prod.env"
        echo "Loaded environment variables from prod.env file"
    else
        echo "Error: prod.env file not found in the same directory"
        exit 1
    fi
fi

# 1 for Verizon, 2 for ATT, 3 for Starlink, 4 for Tmobile
OPERATOR=2
OPERATOR_NAME="att"
# Leave blank for default server
SERVER=


DATA_DIR="${ROOT_DIR}/${OPERATOR_NAME}/$(date +"%Y%m%d")"
if [ ! -d $DATA_DIR ]; then
    mkdir -p $DATA_DIR
fi

TS_DATETIME_FORMAT="%Y-%m-%dT%H:%M:%S%z"
bash "$(dirname "$0")/run_network_measurement.sh" $MODE $OPERATOR $SERVER 2>&1 | ts "[${TS_DATETIME_FORMAT}]" | tee -a $DATA_DIR/measurement.log
