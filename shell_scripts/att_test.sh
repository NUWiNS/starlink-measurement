#!/bin/bash

# 1 for loop, 2 for one-shot mode
MODE=2
#SERVER=35.245.244.238 # Virginia gcloud server
SERVER=50.112.93.113 # Oregon aws server
# 1 for Verizon, 2 for ATT, 3 for Starlink
OPERATOR=2

OPERATOR_NAME="att"

ROOT_DIR="${HOME}/storage/shared/hawaii_starlink_trip/${OPERATOR_NAME}/$(date +"%Y%m%d")"
if [ ! -d $ROOT_DIR ]; then
    mkdir -p $ROOT_DIR
fi

# tcp test
bash run_network_measurement.sh $MODE $SERVER $OPERATOR 2>&1 | tee -a $ROOT_DIR/measurement.log