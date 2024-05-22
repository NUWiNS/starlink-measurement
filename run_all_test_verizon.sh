#!/bin/sh
handle_sigint(){
	echo "Caught SIGINT, exitting status 1"
	exit 1
}

trap handle_sigint INT
operator="Verizon"
echo "This is Verizon test, please choose a server"
echo "1) Virginia cloud server: 35.245.244.238"
echo "2) Localhost (for testing): 127.0.0.1"

while true; do
    read -p "Enter your choice (1-5): " choice
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
port_number=5002
echo "Testing $operator, server $choice (ip: $ip_address, port: $port_number)"
while true; do
    # save the output to storage/shared folder for adb pull
    data_folder=~/storage/shared/maine_starlink_trip/$(date '+%Y%m%d')/
    mkdir -p $data_folder

    start_dl_time=$(date '+%H%M%S%3N')
    start_time=$start_dl_time
    mkdir -p $data_folder$start_dl_time
    echo "TCP downlink test started: $start_time"
    start_time=$(date '+%H%M%S%3N')
    log_file_name="$data_folder$start_dl_time/tcp_downlink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    timeout 130 nuttcp -v -i0.5 -r -F -l640 -T10 -p $port_number -w 32M $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name 
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved downlink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "DL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| \
	    sed -E 's/\s*KB\/sec//'

    sleep 5
    start_time=$(date '+%H%M%S%3N')
    echo "TCP uplink test started: $start_time"
    log_file_name="$data_folder$start_dl_time/tcp_uplink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    timeout 130 nuttcp -v -i0.5 -l640  -T120 -p $port_number -w 32M $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved uplink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "UL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| sed -E 's/\s*KB\/sec//'

    sleep 5
    start_time=$(date '+%H%M%S%3N')
    echo "Ping test started: $start_time"
    log_file_name="$data_folder$start_dl_time/ping_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    timeout 35 ping -s 38 -i 0.2 -w 30 $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name >>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved ping test to $log_file_name"
    summary=$(grep -E "rtt" $log_file_name | grep -oP '(?<=rtt).*$')
    echo "Ping summary: $summary"

    read -p "Do you want to continue test with server $choice (y/n)? " answer
    case $answer in
        [Yy]* ) continue;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no." && break;;
    esac
done
