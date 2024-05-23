#!/bin/bash

# Start the subscript in the background
bash ./pull_dish_metric.sh status ./outputs  &
SUBSCRIPT_PID=$!
bash ./pull_dish_metric.sh history ./outputs  &
SUBSCRIPT_2_PID=$!

kill_subscript() {
    echo "Master script caught signal, terminating subscript..."
    kill $SUBSCRIPT_PID
    wait $SUBSCRIPT_PID
    echo "Subscript 1 terminated."
    
    kill $SUBSCRIPT_2_PID
    wait $SUBSCRIPT_2_PID
    echo "Subscript 2 terminated."
    exit 0
}

# Function to handle cleanup on exit
cleanup() {
    echo "Master script caught signal, terminating subscript..."
    kill $SUBSCRIPT_PID
    wait $SUBSCRIPT_PID
    echo "Subscript terminated."
    exit 0
}

# Set up trap to catch termination signals
trap cleanup SIGINT SIGTERM SIGHUP

# Simulate some work in the master script
while true; do
    echo "Master script doing some work..."
    sleep 3
    kill_subscript
done
