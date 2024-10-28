#!/bin/bash

## This script pulls data from the dish and writes it to a file
# The script takes two arguments:
# 1. The metric to pull from the dish (status, history)
# 2. The output folder to write the data to

get_timestamp_in_millisec() {
    format="%s%3N"

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        date +"$format"
    elif [[ "$OSTYPE" == "linux-android"* ]]; then
        date +"$format"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        gdate +"$format"
    else
        date +"$format"
    fi
}

get_datetime_with_iso_8601_local_timezone() {
    ISO_8601_TIMEZONE_FORMAT="%Y-%m-%dT%H:%M:%S.%6N%:z"

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        date +"$ISO_8601_TIMEZONE_FORMAT"
    elif [[ "$OSTYPE" == "linux-android"* ]]; then
        date +"$ISO_8601_TIMEZONE_FORMAT"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        gdate +"$ISO_8601_TIMEZONE_FORMAT"
    else
        date +"$ISO_8601_TIMEZONE_FORMAT"
    fi
}

bash $(dirname "$0")/command_guard.sh
guard_status=$?

if [ $guard_status -ne 0 ]; then
  exit $guard_status
fi

# accept first argument for rcp call: status, history
request_metric=$1
case $request_metric in
    "status")
        rpc_method="get_status"
        polling_interval=0.1
        ;;
    "history")
        rpc_method="get_history"
        # polling_interval=$((15*60))
        polling_interval=1
        ;;
    *)
        echo "need to specify which metric (status, history) to pull from dish"
        exit 1
        ;;
esac

# Output file path
# get output folder from the first argument
OUTPUT_FOLDER=$2
OUTPUT_FILE="${OUTPUT_FOLDER}/dish_${request_metric}.out"

if [ -z "$OUTPUT_FOLDER" ]; then
    echo "Please provide the output folder as the first argument"
    exit 1
fi

# Start the polling service
if [ ! -d "$OUTPUT_FOLDER" ]; then
    mkdir -p $OUTPUT_FOLDER
fi


handle_exit(){
    echo "Received signal, performing cleanup..."

    end_timestamp=$(get_timestamp_in_millisec)
    echo "End time: ${end_timestamp}">>$OUTPUT_FILE

    pkill -P $$

	exit 0
}

trap handle_exit SIGINT SIGTERM SIGHUP

start_timestamp=$(get_timestamp_in_millisec)
echo "Start time: ${start_timestamp}" > $OUTPUT_FILE
echo "Starting to pull $request_metric data from dish..."


while true; do
    # If the output is JSON, use jq to convert it to a single line
    req_time=$(get_datetime_with_iso_8601_local_timezone)
    echo "request data from dish at ${req_time}"

    command_output=$(grpcurl -plaintext -emit-defaults -d "{\"${rpc_method}\":{}}" 192.168.100.1:9200 SpaceX.API.Device.Device/Handle)

    res_time=$(get_datetime_with_iso_8601_local_timezone)

    command_output=$(echo "$command_output" | jq -c .)

    if echo "$command_output" | jq -e . >/dev/null 2>&1; then
        echo "receive json data from dish at ${res_time}"
        # compress json to oneline and prepend a res_time and command output to the file
        echo "req: ${req_time} | res: ${res_time} | data: ${command_output}" >> $OUTPUT_FILE
    else
        retry_timeout=3
        echo "Failed to get json data from dish, sleeping for $retry_timeout seconds before retrying..."
        sleep $retry_timeout &
        wait $!
        continue
    fi

    # Wait for the specified interval before the next poll
    echo "Sleeping for $polling_interval seconds ..."
    sleep $polling_interval &
    wait $!
done