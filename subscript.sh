#!/bin/bash

# Function to handle cleanup on exit
cleanup() {
    echo "Caught signal in subscript, performing cleanup..."
    # Terminate all child processes
    pkill -P $$
    exit 0
}

# Set up trap to catch termination signals
trap cleanup SIGINT SIGTERM SIGHUP

# Simulate a task and a long sleep
while true; do
    echo "Sleeping for a long duration..."
    sleep 900 &
    SLEEP_PID=$!
    wait $SLEEP_PID
done
