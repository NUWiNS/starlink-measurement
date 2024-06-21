#!/bin/bash

dependencies="nuttcp iperf3 ts"

for _command in $dependencies; do
  if ! command -v $_command &> /dev/null
  then
    echo "Error: $_command could not be found. Please install $_command."
    exit 1
  fi
done

# Input parameters
# 1) mode: 1 - loop, 2 - one-shot
# 2) ip_address: the target server ip address
# 3) operator_choice: 1 - Verizon, 2 - ATT, 3 - Starlink
# 4) thrpt_choice: t - TCP, u - UDP


ip_address=""
operator=""
thrpt_protocol=""
nuttcp_port=""
iperf_port=""
round=0

SL_PULL_STATUS_PID=""
SL_PULL_HISTORY_PID=""
ISO_8601_TIMEZONE_FORMAT="%Y-%m-%dT%H:%M:%S.%6N%:z"

handle_exit(){
	echo "Caught signal, performing cleanup..."
	exit 0
}

trap handle_exit SIGINT SIGTERM SIGHUP

mode=$1
while true; do
    if [ -z $mode ]; then
        echo "Please choose a mode:"
        echo "1) Loop mode"
        echo "2) One-shot mode"
        read -p "Enter your choice (1-2): " mode
    fi
    case $mode in
        1) break;;
        2) break;;
        *)
            echo "Invalid choice, please enter a number between 1 and 2"
            mode=""
            ;;
    esac
done

ip_address=$2
if [ -z "$ip_address" ]; then
    while true; do
        echo "Starting a network measurement, please choose a server"
        echo "1) Virginia cloud server: 35.245.244.238"
        echo "2) Localhost (for testing): 127.0.0.1"
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
                echo "Invalid choice, please enter a number between 1 and 2"
                ip_address=""
                ;;
        esac
    done
fi


operator_choice=$3
while true; do
    if [ -z "$operator_choice" ]; then
        echo "Please choose an operator:"
        echo "1) Verizon"
        echo "2) ATT"
        echo "3) Starlink"
        echo "4) Tmobile"
        read -p "Enter your choice (1-3): " operator_choice
    fi
    case $operator_choice in
        1)
            operator="verizon"
            nuttcp_port=5101
            iperf_port=5201
            break
            ;;
        2)
            operator="att"            
            nuttcp_port=5102
            iperf_port=5202
            break
            ;;
        3)
            operator="starlink"
            nuttcp_port=5103
            iperf_port=5203
            break
            ;;
        4)
            operator="tmobile"
            nuttcp_port=5104
            iperf_port=5204
            break
            ;;
        *)
            echo "Invalid choice, please enter 1 for Verizon, 2 for ATT, 3 for Starlink, 4 for Tmobile"
            operator_choice=""
            ;;
    esac
done


thrpt_choice=$4
while true; do
    if [ -z "$thrpt_choice" ]; then
        echo "Please choose a protocol for throughput test:"
        echo "t) TCP"
        echo "u) UDP"
        read -p "Enter your choice: " thrpt_choice
    fi
    case $thrpt_choice in
        [tT])
            thrpt_protocol="tcp"
            break
            ;;
        [uU])
            thrpt_protocol="udp"
            break
            ;;
        *)
            echo "Invalid choice, please enter 't' for TCP or 'u' for UDP"
            thrpt_choice=""
            ;;
    esac
done


echo "Testing $operator with $thrpt_protocol, server $server_choice (ip: $ip_address, nuttcp_port: $nuttcp_port, iperf_port: $iperf_port)"

while true; do
    # save the output to storage/shared folder for adb pull
    data_folder=~/storage/shared/alaska_starlink_trip/$operator/$(date '+%Y%m%d')/
    mkdir -p $data_folder

    start_dl_time=$(date '+%H%M%S%3N')
    start_time=$start_dl_time
    mkdir -p $data_folder$start_dl_time

    round=$((round+1))
    sleep 3
    echo "-----------------------------------"

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
        # Avoid TCP MSS limitation for cellular (1376 for verizon e.g.)
        PACKET_SIZE=1300
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

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting uplink test..."
    sleep 2

    start_time=$(date '+%H%M%S%3N')
    echo "-----------------------------------"
    echo "${thrpt_protocol} uplink test started: $start_time"
    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_uplink_${start_time}.out"
    echo "Start time: $(date '+%s%3N')">$log_file_name
    # FIXME: change to 120s
    UL_TEST_DURATION=120
    UL_INTERVAL=0.5
    if [ $thrpt_protocol == "udp" ]; then
        # udp uplink test
        UL_UDP_RATE=300M
        PACKET_SIZE=1300
        echo "testing udp uplink with $ip_address:$iperf_port, rate $UL_UDP_RATE, packet size $PACKET_SIZE bytes, interval $UL_INTERVAL, duration $UL_TEST_DURATION ..."
        timeout 140 nuttcp -u -R $UL_UDP_RATE -v -i $UL_INTERVAL -l $PACKET_SIZE -T $UL_TEST_DURATION -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
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

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting ping test..."
    sleep 2

    start_time=$(date '+%H%M%S%3N')
    echo "-----------------------------------"
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

    # NOTE: Just a hack to make traceroute and nslookup run till the end of tcp + udp measurement
    if [ $thrpt_protocol == "udp" ]; then
      echo "-----------------------------------"
      echo "Waiting for 5 seconds before starting traceroute test..."
      sleep 5

      start_time=$(date '+%H%M%S%3N')
      echo "-----------------------------------"
      echo "Traceroute test started: $start_time"
      log_file_name="$data_folder$start_dl_time/traceroute_${start_time}.out"
      echo "Start time: $(date '+%s%3N')">$log_file_name
      # tracerout to the target server
      timeout 120 traceroute $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
      echo "End time: $(date '+%s%3N')">>$log_file_name
      echo "Saved traceroute test to $log_file_name"

      echo "-----------------------------------"
      echo "Waiting for 5 seconds before starting nslookup test..."
      sleep 5

      start_time=$(date '+%H%M%S%3N')
      echo "-----------------------------------"
      echo "Nslookup test started: $start_time"
      log_file_name="$data_folder$start_dl_time/nslookup_${start_time}.out"
      # Top 5 websites worldwide: https://www.semrush.com/website/top/
      # Only keep facebook.com for now because starlink uses 8.8.8.8 as DNS server all the time
      top5_websites="facebook.com"
      for domain in $top5_websites; do
          echo "Start time: $(date '+%s%3N')">>$log_file_name
          timeout 120 nslookup $domain | grep -v '^$' >> $log_file_name
          echo "End time: $(date '+%s%3N')">>$log_file_name
          echo "">>$log_file_name
      done
      echo "Saved nslookup test to $log_file_name"
    fi

    echo "-----------------------------------"
    echo "All tests (${thrpt_protocol}) completed, cleaning up..."

    if [ $mode -eq 2 ]; then
        break;
    else
        # loop mode
        echo "-----------------------------------"
        read -p "Round ${round} finished, continue to test with server $ip_address (y/n)?" answer
        case $answer in
            [Yy]* ) continue;;
            [Nn]* ) break  ;;
            * ) echo "Please answer yes or no." && break;;
        esac
    fi
done
