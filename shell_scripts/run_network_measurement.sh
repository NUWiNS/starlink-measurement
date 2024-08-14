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


echo "Testing $operator, server $server_choice (ip: $ip_address, nuttcp_port: $nuttcp_port, iperf_port: $iperf_port)"

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

# Function to calculate end time based on start time and duration
calculate_timestamp_ms() {
    local start_ts_ms="$1"
    local duration_seconds="$2"

    # Check if both parameters are provided
    if [ -z "$start_ts_ms" ] || [ -z "$duration_seconds" ]; then
        echo "Usage: calculate_end_time <start_ts_ms> <duration_seconds>" >&2
        return 1
    fi

    # Validate that both inputs are numbers
    if ! [[ "$start_ts_ms" =~ ^[0-9]+$ ]] || ! [[ "$duration_seconds" =~ ^[0-9]+$ ]]; then
        echo "Error: Both start_ts_ms and duration_seconds must be numeric values" >&2
        return 1
    fi

    # Calculate end timestamp
    estimated_end_ts_ms=$((start_ts_ms + duration_seconds * 1000))

    echo "$estimated_end_ts_ms"
}

parse_timestamp() {
    local timestamp=$1
    local seconds
    local milliseconds

    # Check if the timestamp includes a decimal point
    if [[ $timestamp == *.* ]]; then
        # Split the timestamp into seconds and milliseconds
        seconds=${timestamp%.*}
        milliseconds=${timestamp#*.}
    else
        # Assume it's all milliseconds
        seconds=$((timestamp / 1000))
        milliseconds=$((timestamp % 1000))
    fi

    # Pad milliseconds to ensure it's always 3 digits
    milliseconds=$(printf "%03d" $milliseconds)

    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v gdate >/dev/null 2>&1; then
            # Use gdate if available
            gdate -d "@$seconds" "+%Y-%m-%d %H:%M:%S.$milliseconds"
        else
            # Fallback to BSD date
            date -r $seconds "+%Y-%m-%d %H:%M:%S.$milliseconds"
        fi
    else
        # Linux and others
        date -d "@$seconds" "+%Y-%m-%d %H:%M:%S.$milliseconds"
    fi
}

compare_timestamps() {
    local timestamp1="$1"
    local timestamp2="$2"

    # Compare timestamps
    if (( $(echo "$timestamp1 > $timestamp2" | bc -l) )); then
        return 0  # True, datetime1 exceeds datetime2
    else
        return 1  # False, datetime1 does not exceed datetime2
    fi
}

timestamp_diff_ms() {
  local ts1_ms="$1"
  local ts2_ms="$2"
  local diff_ms=$(echo "$ts1_ms - $ts2_ms" | bc)
  echo $diff_ms
}

wait_util_end_time() {
  end_timestamp_ms=$1
  current_timestamp_ms=$(get_timestamp_ms)
  if compare_timestamps $current_timestamp_ms $end_timestamp_ms; then
      echo "Current time exceeds end time, skipping wait"
  else
      diff_ms=$(timestamp_diff_ms $end_timestamp_ms $current_timestamp_ms)
      diff_s=$(convert_ms_to_sec $diff_ms)
      echo "Waiting for ${diff_s} s to reach end time"

      sleep $diff_s
  fi
}

convert_ms_to_sec() {
  local ms="$1"
  echo "scale=3; $ms / 1000" | bc
}

report_start_time() {
  local test_name="$1"
  local start_ts_ms="$2"
  echo "${test_name} started at $(parse_timestamp $start_ts_ms)"
}

report_end_time_and_duration() {
  local test_name="$1"
  local start_ts_ms="$2"
  local estimated_end_ts_ms="$3"

  test_duration_ms=$(timestamp_diff_ms $estimated_end_ts_ms $start_ts_ms)
  echo "${test_name} finished at $(parse_timestamp $estimated_end_ts_ms), duration: $(convert_ms_to_sec $test_duration_ms) s"
}

ROOT_DIR="${HOME}/storage/shared/alaska_starlink_trip"

while true; do
    # save the output to storage/shared folder for adb pull
    data_folder="${ROOT_DIR}/${operator}/$(date '+%Y%m%d')/"
    mkdir -p $data_folder

    start_dl_time=$(date '+%H%M%S%3N')
    start_time=$start_dl_time
    mkdir -p $data_folder$start_dl_time

    round=$((round+1))
    echo "-----------------------------------"

    # TCP DL Test
    # FIXME: change to 120s
    thrpt_protocol="tcp"
    duration_s=120
#    duration_s=3
    timeout_s=140
#    timeout_s=3
    interval_s=0.5

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} downlink test" $start_ts_ms

    start_time=$(date '+%H%M%S%3N')
    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_downlink_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # tcp downlink test
    echo "testing tcp downlink with $ip_address:$nuttcp_port, interval $interval_s, duration $duration_s ..."
    timeout $timeout_s nuttcp -r -F -v -i $interval_s -T $duration_s -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name
    report_end_time_and_duration "${thrpt_protocol} downlink test" $start_ts_ms $actual_end_ts_ms

    echo "Saved downlink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "DL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| \
	    sed -E 's/\s*KB\/sec//'

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting tcp uplink test..."
    sleep 2

    # TCP UL Test
    echo "-----------------------------------"
    # FIXME: change to 120s
    thrpt_protocol="tcp"
    duration_s=120
#    duration_s=3
    timeout_s=140
#    timeout_s=3
    interval_s=0.5

    start_time=$(date '+%H%M%S%3N')
    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} uplink test" $start_ts_ms

    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_uplink_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # tcp uplink test
    echo "testing tcp uplink with $ip_address:$nuttcp_port, interval $interval_s, duration $duration_s ..."
    timeout $timeout_s nuttcp -v -i $interval_s -T $duration_s -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name

    wait_util_end_time $estimated_end_ts_ms

    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name
    report_end_time_and_duration "${thrpt_protocol} uplink test" $start_ts_ms $actual_end_ts_ms

    echo "Saved uplink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "UL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| sed -E 's/\s*KB\/sec//'

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting ping test..."
    sleep 2

    echo "-----------------------------------"
      duration_s=30
#    duration_s=3
      timeout_s=50
#    timeout_s=3

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "Ping test" $start_ts_ms

    start_time=$(date '+%H%M%S%3N')
    log_file_name="$data_folder$start_dl_time/ping_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # FIXME: change to 30s
    timeout $timeout_s ping -s 38 -i 0.2 -w $duration_s $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name
    report_end_time_and_duration "Ping test" $start_ts_ms $actual_end_ts_ms

    echo "Saved ping test to $log_file_name"
    summary=$(grep -E "rtt" $log_file_name | grep -oP '(?<=rtt).*$')
    echo "Ping summary: $summary"

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting udp downlink test..."
    sleep 2

    # UDP DL Test
    echo "-----------------------------------"
    # FIXME: change to 120s
    thrpt_protocol="udp"
    duration_s=120
#    duration_s=3
    timeout_s=140
#    timeout_s=3
    interval_s=0.5

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} downlink test" $start_ts_ms

    start_time=$(date '+%H%M%S%3N')
    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_downlink_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # udp downlink test
    DL_UDP_RATE=500M
    # Avoid TCP MSS limitation for cellular (1376 for verizon e.g.)
    PACKET_SIZE=1300
    echo "testing udp downlink with $ip_address:$iperf_port, rate $DL_UDP_RATE, packet size $PACKET_SIZE bytes, interval $interval_s, duration $duration_s ..."
    timeout $timeout_s iperf3 -c $ip_address -p $iperf_port -R -u -b $DL_UDP_RATE -l $PACKET_SIZE -i $interval_s -t $duration_s | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name
    report_end_time_and_duration "${thrpt_protocol} downlink test" $start_ts_ms $actual_end_ts_ms

    echo "Saved downlink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "DL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| \
	    sed -E 's/\s*KB\/sec//'

    echo "-----------------------------------"

    # 2s to ensure 5G will not switch back to LTE
    echo "Waiting for 2 seconds before starting udp uplink test..."
    sleep 2

    # UDP UL Test
    echo "-----------------------------------"
    # FIXME: change to 120s
    thrpt_protocol="udp"
    duration_s=120
#    duration_s=3
    timeout_s=140
#    timeout_s=3
    interval_s=0.5

    start_time=$(date '+%H%M%S%3N')
    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} uplink test" $start_ts_ms

    log_file_name="${data_folder}${start_dl_time}/${thrpt_protocol}_uplink_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # udp uplink test
    UL_UDP_RATE=300M
    PACKET_SIZE=1300
    echo "testing udp uplink with $ip_address:$iperf_port, rate $UL_UDP_RATE, packet size $PACKET_SIZE bytes, interval $interval_s, duration $duration_s ..."
    timeout $timeout_s nuttcp -u -R $UL_UDP_RATE -v -i $interval_s -l $PACKET_SIZE -T $duration_s -p $nuttcp_port $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name

    wait_util_end_time $estimated_end_ts_ms

    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name
    report_end_time_and_duration "${thrpt_protocol} uplink test" $start_ts_ms $actual_end_ts_ms

    echo "Saved uplink test to $log_file_name"
    rate=$(grep -E 'nuttcp -r' $log_file_name)
    echo "UL average throughput: $rate"
    grep 'nuttcp-r' $log_file_name | grep -o -P '([0-9]+(\.[0-9]+)?)\s*Mbps'| sed -E 's/\s*KB\/sec//'

    echo "-----------------------------------"
    echo "Waiting for 2 seconds before starting traceroute test..."
    sleep 2

    echo "-----------------------------------"
      timeout_s=15
#    timeout_s=3

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "Traceroute test" $start_ts_ms

    start_time=$(date '+%H%M%S%3N')
    log_file_name="$data_folder$start_dl_time/traceroute_${start_time}.out"
    echo "Start time: $start_ts_ms">$log_file_name

    # tracerout to the target server
    timeout $timeout_s traceroute $ip_address | ts '[%Y-%m-%d %H:%M:%.S]'>>$log_file_name
    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms">>$log_file_name
    report_end_time_and_duration "Traceroute test" $start_ts_ms $actual_end_ts_ms

    echo "Saved traceroute test to $log_file_name"

    echo "-----------------------------------"
    echo "Waiting for 2 seconds before starting nslookup test..."
    sleep 2

    echo "-----------------------------------"
      timeout_s=15
#    timeout_s=3

    start_ts_ms=$(get_timestamp_ms)
    report_start_time "Nslookup test" $start_ts_ms

    start_time=$(date '+%H%M%S%3N')
    log_file_name="$data_folder$start_dl_time/nslookup_${start_time}.out"
    # Top 5 websites worldwide: https://www.semrush.com/website/top/
    # Only keep facebook.com for now because starlink uses 8.8.8.8 as DNS server all the time
    top5_websites="facebook.com"
    for domain in $top5_websites; do
        sub_start_ts_ms=$(get_timestamp_ms)
        sub_estimated_end_ts_ms=$(calculate_timestamp_ms ${sub_start_ts_ms} $timeout_s)
        echo "Start time: ${sub_start_ts_ms}" >> $log_file_name

        timeout $timeout_s nslookup $domain | grep -v '^$' >> $log_file_name
        wait_util_end_time ${sub_estimated_end_ts_ms}

        echo "End time: $(get_timestamp_ms)" >> $log_file_name
        echo "" >> $log_file_name
    done
    report_end_time_and_duration "Nslookup test" $start_ts_ms $(get_timestamp_ms)
    echo "Saved nslookup test to $log_file_name"

    echo "-----------------------------------"
    echo "All tests (${thrpt_protocol}) completed, cleaning up..."
    echo "-----------------------------------"

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
