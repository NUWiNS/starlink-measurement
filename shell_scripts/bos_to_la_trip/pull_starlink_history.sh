#!/bin/bash

# Accept input from command line for test mode, default is production mode
# Use 3 for test mode (consistent with run_network_measurement.sh)
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

# Check if essential variables are set
if [ -z "$ROOT_DIR" ]; then
    echo "Error: ROOT_DIR is not set. Please check your env file along with the script."
    exit 1
fi

get_date() {
   if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v gdate >/dev/null 2>&1; then
            # Use gdate if available
            gdate +%Y%m%d
        else
            # Fallback to BSD date
            date -j "+%Y%m%d"
        fi
    else
        # Linux and others
        date +%Y%m%d
    fi
}

get_time_ms() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v gdate >/dev/null 2>&1; then
            # Use gdate if available
            gdate +%H%M%S%3N
        else
            # Fallback to BSD date
            date -j "+%H%M%S$(printf "%03d" $(($(date +%N) / 1000000)))"
        fi
    else
        # Linux and others
        date +%H%M%S%3N
    fi
}

data_folder=${ROOT_DIR}/dish_history/$(get_date)/$(get_time_ms)
mkdir -p $data_folder

file_name="dish_history.out"
output_file=$data_folder/$file_name

get_timestamp_ms() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v gdate >/dev/null 2>&1; then
            # Use gdate if available
            gdate +%s%3N
        else
            # Fallback to BSD date
            date -j "+%s$(printf "%03d" $(($(date +%N) / 1000000)))"
        fi
    else
        # Linux and others
        date +%s%3N
    fi
}

echo "Pulling dish history data..."

start_timestamp=$(get_timestamp_ms)
echo "Start time: ${start_timestamp}" > $output_file

grpcurl -plaintext -emit-defaults -d "{\"get_history\":{}}" 192.168.100.1:9200 SpaceX.API.Device.Device/Handle >> $output_file 2>&1 

end_timestamp=$(get_timestamp_ms)
echo "End time: ${end_timestamp}">>$output_file

echo "Pulled dish history data to $output_file"