#!/bin/bash

# Read env file from the same directory
if [ -f "$(dirname "$0")/env" ]; then
    source "$(dirname "$0")/env"
    echo "Loaded environment variables from env file"
else
    echo "Warning: env file not found in the same directory"
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

data_folder=${ROOT_DIR}/dish_metrics/$(get_date)/$(get_time_ms)
mkdir -p $data_folder

handle_exit() {
  echo "Caught signal, cleanup subscripts..."
  pkill -P $$
  exit 0
}

trap handle_exit INT SIGINT SIGTERM SIGHUP

# run status polling
bash $(dirname "$0")/../pull_dish_metric.sh status $data_folder >$data_folder/pull_sl_status.log &
FETCH_STATUS_PID=$!
echo "fetching starlink status in background, PID: ${FETCH_STATUS_PID}..."

# run history polling
# bash ./pull_dish_metric.sh history $data_folder >$data_folder/pull_sl_history.log &
# FETCH_HISTORY_PID=$!
# echo "fetching starlink history in background, PID: ${FETCH_HISTORY_PID}..."

while true; do
  read -p "Do you want to quit (y/n)? " answer
  case $answer in
  [Yy]*) 
    pkill -P $$
    break ;;
  [Nn]*) continue ;;
  *) 
    echo "Please answer yes or no."
    continue
    ;;
  esac
done
