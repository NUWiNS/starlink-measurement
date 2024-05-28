#!/bin/bash

# 1 for loop, 2 for one-shot mode
MODE=2
SERVER=35.245.244.238
# 1 for Verizon, 2 for ATT, 3 for Starlink
OPERATOR=1

# tcp test
bash run_network_measurement.sh $MODE $SERVER $OPERATOR t

echo "-----------------------------------"
echo "Sleep for 10s for next udp test..."
sleep 10
echo "-----------------------------------"

# udp test
bash run_network_measurement.sh $MODE $SERVER $OPERATOR u