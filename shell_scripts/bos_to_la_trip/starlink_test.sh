#!/bin/bash

# Read env file from the same directory
if [ -f "$(dirname "$0")/env" ]; then
    source "$(dirname "$0")/env"
    echo "Loaded environment variables from env file"
else
    echo "Error: env file not found in the same directory"
    exit 1
file
fi

# 1 for loop, 2 for one-shot mode, 3 for one-shot mode for testing 
# Use first argument if provided
MODE=${1:-2}
# 1 for Verizon, 2 for ATT, 3 for Starlink, 4 for Tmobile
OPERATOR=3
OPERATOR_NAME="starlink"
# Leave blank for default server
SERVER=


DATA_DIR="${ROOT_DIR}/${OPERATOR_NAME}/$(date +"%Y%m%d")"
if [ ! -d $DATA_DIR ]; then
    mkdir -p $DATA_DIR
fi

bash run_network_measurement.sh $MODE $OPERATOR $SERVER 2>&1 | tee -a $DATA_DIR/measurement.log

bash ./pull_starlink_history.sh 2>&1 | tee -a $DATA_DIR/starlink_rpc.log
