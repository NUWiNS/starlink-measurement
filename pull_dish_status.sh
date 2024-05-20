#!/bin/bash

# Set the polling interval (in seconds)
POLL_INTERVAL=0.1

# Output file path
OUTPUT_FOLDER="./outputs"
OUTPUT_FILE="${OUTPUT_FOLDER}/dish_status.$(date +"%Y%m%d_%H%M%S").log"

get_timestamp_in_micro_sec() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        date +%s%6N
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        gdate +%s%6N
    else
        date +%s%6N
    fi
}


# Function to perform the polling
poll_command() {
    if [ ! -d "$OUTPUT_FOLDER" ]; then
        mkdir -p "$OUTPUT_FOLDER"
    fi

  # Write the header to the output file
  echo "utc_time_micro | response" > "$OUTPUT_FILE"

  while true; do
      # If the output is JSON, use jq to convert it to a single line
      command_output=$(grpcurl -plaintext -emit-defaults -d '{"get_status":{}}' 192.168.100.1:9200 SpaceX.API.Device.Device/Handle \
        | jq -c '.')

      # Save the response time as timestamp
      time_ms=$(get_timestamp_in_micro_sec)

      echo "pulled status data from dish: ${time_ms}"

      # Append the time_ms and command output to the file
      echo "$time_ms | $command_output" >> "$OUTPUT_FILE"

      # Wait for the specified interval before the next poll
      sleep $POLL_INTERVAL
  done
}

# Start the polling service
poll_command