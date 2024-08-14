#!/bin/bash

# 1 for loop, 2 for one-shot mode
MODE=2
#SERVER=35.245.244.238 # Virginia gcloud server
SERVER=50.112.93.113 # Oregon aws server
# 1 for Verizon, 2 for ATT, 3 for Starlink, 4 for Tmobile
OPERATOR=4

OPERATOR_NAME="tmobile"

ROOT_DIR="${HOME}/storage/shared/alaska_starlink_trip/${OPERATOR_NAME}/$(date +"%Y%m%d")"
if [ ! -d $ROOT_DIR ]; then
    mkdir -p $ROOT_DIR
fi

bash run_network_measurement.sh $MODE $SERVER $OPERATOR 2>&1 | tee -a $ROOT_DIR/measurement.log