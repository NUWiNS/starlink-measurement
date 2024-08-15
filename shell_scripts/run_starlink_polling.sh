#!/bin/bash

data_folder=~/storage/shared/hawaii_starlink_trip/dish_metrics/$(date '+%Y%m%d')/$(date '+%H%M%S%3N')
mkdir -p $data_folder

handle_exit() {
  echo "Caught signal, cleanup subscripts..."
  pkill -P $$
  exit 0
}

trap handle_exit INT SIGINT SIGTERM SIGHUP

# run status polling
bash ./pull_dish_metric.sh status $data_folder >$data_folder/pull_sl_status.log &
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
