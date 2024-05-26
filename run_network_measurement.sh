#!/bin/bash

dependencies="nuttcp iperf3 ts"

for _command in $dependencies; do
  if ! command -v $_command &> /dev/null
  then
    echo "Error: $_command could not be found. Please install $_command."
    exit 1
  fi
done

ip_address=""
operator=""
thrpt_protocol=""
nuttcp_port=""
iperf_port=""

SL_PULL_STATUS_PID=""
SL_PULL_HISTORY_PID=""
ISO_8601_TIMEZONE_FORMAT="%Y-%m-%dT%H:%M:%S.%6N%:z"

handle_exit(){
	echo "Caught signal, performing cleanup..."
	exit 0
}

trap handle_exit SIGINT SIGTERM SIGHUP

echo "Starting a network measurement, please choose a server"
echo "1) Virginia cloud server: 35.245.244.238"
echo "2) Localhost (for testing): 127.0.0.1"

while true; do
    read -p "Enter your choice (1-2): " server_choice
    case $server_choice in
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
    read -p "Enter your choice (1-3): " operator_choice
    case $operator_choice in
        1)
            operator="verizon"
            nuttcp_port=5001
            iperf_port=5201
            break
            ;;
        2)
            operator="att"            
            nuttcp_port=5002
            iperf_port=5202
            break
            ;;
        3)
            operator="starlink"
            nuttcp_port=5003
            iperf_port=5203
            break
            ;;
        *)
            echo "Invalid choice, please enter a number between 1 and 3"
    esac
done

echo "Please choose TCP or UDP for throughput test:"
echo "1) TCP"
echo "2) UDP"

while true; do
    read -p "Enter your choice (1-2): " thrpt_choice
    case $thrpt_choice in
        1)
            thrpt_protocol="tcp"
            break
            ;;
        2)
            thrpt_protocol="udp"
            break
            ;;
        *)
            echo "Invalid choice, please enter a number within (1-2)"
    esac
done


echo "Testing $operator, server $server_choice (ip: $ip_address, nuttcp_port: $nuttcp_port, iperf_port: $iperf_port)"

while true; do
    # save the output to storage/shared folder for adb pull
    data_folder=~/storage/shared/maine_starlink_trip/$operator/$(date '+%Y%m%d')/
    mkdir -p $data_folder

    start_dl_time=$(date '+%H%M%S%3N')
    start_time=$start_dl_time
    mkdir -p $data_folder$start_dl_time

    sleep 3
    echo "------"

    echo "${thrpt_protocol} downlink test started: $start_time"
    start_time=$(date '+%H%M%S%3N')
    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_downlink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 120s
    DL_TEST_DURATION=120
    DL_INTERVAL=0.5
    if [ $thrpt_protocol == "udp" ]; then
        # udp downlink test
        DL_UDP_RATE=500M
        PACKET_SIZE=1400
        echo "testing udp downlink with $ip_address:$iperf_port, rate $DL_UDP_RATE, packet size $PACKET_SIZE bytes, interval $DL_INTERVAL, duration $DL_TEST_DURATION ..."
        timeout 140 iperf3 -c $ip_address -p $iperf_port -R -u -b $DL_UDP_RATE -l $PACKET_SIZE -i $DL_INTERVAL -t $DL_TEST_DURATION | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    else
        # tcp downlink test
        echo "testing tcp downlink with $ip_address:$nuttcp_port, interval $DL_INTERVAL, duration $DL_TEST_DURATION ..."
        timeout 140 nuttcp -r -F -v -i $DL_INTERVAL -T $DL_TEST_DURATION -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name 
    fi
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved downlink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "DL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| \
	    sed -E 's/\s*KB\/sec//'

    echo "------"
    echo "Waiting for 5 seconds before starting uplink test..."
    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "${thrpt_protocol} uplink test started: $start_time"
    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_uplink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 120s
    UL_TEST_DURATION=120
    UL_INTERVAL=0.5
    if [ $thrpt_protocol == "udp" ]; then
        # udp uplink test
        UL_UDP_RATE=0M
        PACKET_SIZE=1400
        echo "testing udp uplink with $ip_address:$iperf_port, rate $UL_UDP_RATE, packet size $PACKET_SIZE bytes, interval $UL_INTERVAL, duration $UL_TEST_DURATION ..."
        timeout 140 iperf3 -c $ip_address -p $iperf_port -u -b $UL_UDP_RATE -l $PACKET_SIZE -i $UL_INTERVAL -t $UL_TEST_DURATION | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    else
        # tcp uplink test
        echo "testing tcp uplink with $ip_address:$nuttcp_port, interval $UL_INTERVAL, duration $UL_TEST_DURATION ..."
        timeout 140 nuttcp -v -i $UL_INTERVAL -T $UL_TEST_DURATION -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    fi
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved uplink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "UL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| sed -E 's/\s*KB\/sec//'

    echo "------"
    echo "Waiting for 5 seconds before starting ping test..."
    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "Ping test started: $start_time"
    log_file_name="$data_folder$start_dl_time/ping_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 30s
    PING_TEST_DURATION=30
    timeout 50 ping -s 38 -i 0.2 -w $PING_TEST_DURATION $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved ping test to $log_file_name"
    summary=$(grep -E "rtt" $log_file_name | grep -oP '(?<=rtt).*$')
    echo "Ping summary: $summary"

    echo "------"
    echo "Waiting for 5 seconds before starting traceroute test..."
    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "Traceroute test started: $start_time"
    log_file_name="$data_folder$start_dl_time/traceroute_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # tracerout to the target server
    traceroute $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    echo "End time: $(date '+%s%3N')">>$log_file_name
    echo "Saved traceroute test to $log_file_name"

    echo "------"
    echo "Waiting for 5 seconds before starting nslookup test..."
    sleep 5

    start_time=$(date '+%H%M%S%3N')
    echo "------"
    echo "Nslookup test started: $start_time"
    log_file_name="$data_folder$start_dl_time/nslookup_${start_time}.out"
    # Top 5 websites worldwide: https://www.semrush.com/website/top/
    top5_websites="google.com youtube.com facebook.com wikipedia.org instagram.com"
    for domain in $top5_websites; do
        echo "Start time: $(date '+%s%3N')">>$log_file_name
        nslookup $domain | grep -v '^$' >> $log_file_name
        echo "End time: $(date '+%s%3N')">>$log_file_name
        echo "">>$log_file_name
    done
    echo "Saved nslookup test to $log_file_name"

    echo "------"
    echo "All tests completed, cleaning up..."

    echo "------"
    read -p "Do you want to continue test with server $server_choice (y/n)? " answer
    case $answer in
        [Yy]* ) continue;;
        [Nn]* ) break  ;;
        * ) echo "Please answer yes or no." && break;;
    esac
done
