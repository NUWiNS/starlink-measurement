#!/bin/bash

ip_address=""
operator=""
port_number=5002
SL_PULL_STATUS_PID=""
SL_PULL_HISTORY_PID=""

handle_exit(){
	echo "Caught signal, performing cleanup..."
    if [ $SL_PULL_STATUS_PID != "" ]; then
        kill $SL_PULL_STATUS_PID
        wait $SL_PULL_STATUS_PID
        echo "Killed SL status task, PID: $SL_PULL_STATUS_PID"
    fi
    if [ $SL_PULL_HISTORY_PID != "" ]; then
        kill $SL_PULL_HISTORY_PID
        wait $SL_PULL_HISTORY_PID
        echo "Killed SL history task, PID: $SL_PULL_HISTORY_PID"
    fi
	exit 0
}

trap handle_exit INT SIGINT SIGTERM SIGHUP

echo "Starting a network measurement, please choose a server"
echo "1) Virginia cloud server: 35.245.244.238"
echo "2) Localhost (for testing): 127.0.0.1"

while true; do
    read -p "Enter your choice (1-2): " choice
    case $choice in
        1)
            ip_address=35.245.244.238
            break
            ;;
        2)
            ip_address=127.0.0.1
            break
            ;;
        *)
            echo "Invalid choice, please enter a number between 1 and 5"
            ;;
    esac
done


echo "Please choose an operator:"
echo "1) Verizon"
echo "2) ATT"
echo "3) Starlink"

while true; do
    read -p "Enter your choice (1-3): " choice
    case $choice in
        1)
            operator="verizon"
            break
            ;;
        2)
            operator="att"
            break
            ;;
        3)
            operator="starlink"
            break
            ;;
        *)
            echo "Invalid choice, please enter a number between 1 and 3"
    esac
done


echo "Testing $operator, server $choice (ip: $ip_address, port: $port_number)"

while true; do
    # save the output to storage/shared folder for adb pull
    data_folder=~/storage/shared/maine_starlink_trip/$operator/$(date '+%Y%m%d')/
    mkdir -p $data_folder

    start_dl_time=$(date '+%H%M%S%3N')
    start_time=$start_dl_time
    mkdir -p $data_folder$start_dl_time

    # if operator is starlink, start starlink test as background
    if [ $operator == "starlink" ]; then
        nohup bash ./pull_dish_metric.sh status $data_folder$start_dl_time > pull_sl_status.log 2>&1 &
        SL_PULL_STATUS_PID=$!
        echo "fetching starlink status in background, PID: $SL_PULL_STATUS_PID"

        nohup bash ./pull_dish_metric.sh history $data_folder$start_dl_time > pull_sl_history.log 2>&1 &
        SL_PULL_HISTORY_PID=$!
        echo "fetching starlink history in background, PID: $SL_PULL_STATUS_PID"
    fi

    echo "TCP downlink test started: $start_time"
    start_time=$(date '+%H%M%S%3N')
    log_file_name="$data_folder$start_dl_time/tcp_downlink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 120s
    timeout 130 nuttcp -v -i0.5 -r -F -l640 -T1 -p $port_number -w 32M $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name 
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved downlink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "DL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| \
	    sed -E 's/\s*KB\/sec//'

    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "TCP uplink test started: $start_time"
    log_file_name="$data_folder$start_dl_time/tcp_uplink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 120s
    timeout 130 nuttcp -v -i0.5 -l640  -T1 -p $port_number -w 32M $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved uplink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "UL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| sed -E 's/\s*KB\/sec//'

    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "Ping test started: $start_time"
    log_file_name="$data_folder$start_dl_time/ping_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 30s
    timeout 35 ping -s 38 -i 0.2 -w 1 $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name >>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved ping test to $log_file_name"
    summary=$(grep -E "rtt" $log_file_name | grep -oP '(?<=rtt).*$')
    echo "Ping summary: $summary"

    echo "------"
    read -p "Do you want to continue test with server $choice (y/n)? " answer
    case $answer in
        [Yy]* ) continue;;
        [Nn]* ) 
            break
            if [ $SL_PULL_STATUS_PID != "" ]; then
                kill $SL_PULL_STATUS_PID
                echo "Killed SL status task, PID: $SL_PULL_STATUS_PID"
            fi
            if [ $SL_PULL_HISTORY_PID != "" ]; then
                kill $SL_PULL_HISTORY_PID
                echo "Killed SL history task, PID: $SL_PULL_HISTORY_PID"
            fi
        ;;
        * ) echo "Please answer yes or no." && break;;
    esac
done
