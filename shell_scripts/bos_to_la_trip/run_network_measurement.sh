#!/bin/bash

# Read env file from the same directory
if [ -f "$(dirname "$0")/prod.env" ]; then
    source "$(dirname "$0")/prod.env"
    echo "[measurement] Load environment variables from prod.env file"
else
    echo "[measurement] Warning: prod.env file not found in the same directory"
fi

# # Test the environment variables read from the env file
# echo "Testing environment variables:"
# echo "ROOT_DIR: $ROOT_DIR"

# Check if essential variables are set
if [ -z "$ROOT_DIR" ]; then
    echo "Error: ROOT_DIR is not set. Please check your env file along with the script."
    exit 1
fi

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
ISO_8601_DATETIME_FORMAT="%Y-%m-%dT%H:%M:%S.%6N%:z"
TS_DATETIME_FORMAT="%Y-%m-%dT%H:%M:%.S%z"
TIME_FORMAT="+%H%M%S%3N"

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
        echo "3) One-shot mode for testing"
        read -p "Enter your choice (1-3): " mode
    fi
    case $mode in
        1) break;;
        2) break;;
        3) 
            # Overwrite env variables with test.env
            if [ -f "$(dirname "$0")/test.env" ]; then
                source "$(dirname "$0")/test.env"
                echo "[measurement] Overwrite environment variables from test.env file"
            else
                echo "[measurement] Warning: test.env file not found in the same directory"
            fi
            break;;
        *)
            echo "Invalid choice, please enter a number in range"
            mode=""
            ;;
    esac
done

read_operator_specific_env() {
    local operator=$1
    if [ -f "$(dirname "$0")/${operator}.env" ]; then
        source "$(dirname "$0")/${operator}.env"
        echo "[measurement] Load operator-specific env from ${operator}.env file"
    fi
}

operator_choice=$2
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
            read_operator_specific_env $operator

            nuttcp_port=$VERIZON_NUTTCP_PORT
            iperf_port=$VERIZON_IPERF_PORT
            break
            ;;
        2)
            operator="att"
            read_operator_specific_env $operator

            nuttcp_port=$ATT_NUTTCP_PORT
            iperf_port=$ATT_IPERF_PORT
            break
            ;;
        3)
            operator="starlink"
            read_operator_specific_env $operator

            nuttcp_port=$STARLINK_NUTTCP_PORT
            iperf_port=$STARLINK_IPERF_PORT
            break
            ;;
        4)
            operator="tmobile"
            read_operator_specific_env $operator

            nuttcp_port=$TMOBILE_NUTTCP_PORT
            iperf_port=$TMOBILE_IPERF_PORT
            break
            ;;
        *)
            echo "Invalid choice, please enter 1 for Verizon, 2 for ATT, 3 for Starlink, 4 for Tmobile"
            operator_choice=""
            ;;
    esac
done


ip_address=$3
if [ -z "$ip_address" ]; then
    # Read server options from env file
    IFS=$'\n' read -d '' -r -a SERVER_NAMES <<< "${SERVER_NAMES_LIST}"
    IFS=$'\n' read -d '' -r -a SERVER_IPS <<< "${SERVER_IPS_LIST}"

    if [ ${#SERVER_IPS[@]} -eq 0 ]; then
        echo "Error: SERVER_IPS_LIST is empty"
        exit 1
    fi

    # Validate arrays have same length
    if [ ${#SERVER_NAMES[@]} -ne ${#SERVER_IPS[@]} ]; then
        echo "Error: SERVER_NAMES_LIST and SERVER_IPS_LIST must have the same number of entries"
        exit 1
    fi
    
    while true; do
        echo "Starting a network measurement, please choose a server"
        for i in "${!SERVER_NAMES[@]}"; do
            echo "$i) ${SERVER_NAMES[$i]}: ${SERVER_IPS[$i]}"
        done
        read -p "Enter your choice (0-$((${#SERVER_NAMES[@]}-1))): " server_choice
        
        if [[ "$server_choice" =~ ^[0-9]+$ ]] && [ "$server_choice" -lt "${#SERVER_NAMES[@]}" ]; then
            ip_address=${SERVER_IPS[$server_choice]}
            break
        else
            echo "Invalid choice, please enter a number in range"
            ip_address=""
        fi
    done
fi


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
            gdate -d "@$seconds" "+%Y-%m-%d %H:%M:%S.$milliseconds %Z"
        else
            # Fallback to BSD date
            date -r $seconds "+%Y-%m-%d %H:%M:%S.$milliseconds %Z"
        fi
    else
        # Linux and others
        date -d "@$seconds" "+%Y-%m-%d %H:%M:%S.$milliseconds %Z"
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

grep_file() {
    local file="$1"
    local line_pattern="$2"
    local value_pattern="$3"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        grep "$line_pattern" "$file" | grep -E -o "$value_pattern"
    else
        # Android (Termux) and other Unix-like systems
        grep "$line_pattern" "$file" | grep -P -o "$value_pattern"
    fi
}

check_file_lines_gt() {
  local filename="$1"
  local min_lines="${2:-0}"  # Default to 0 if not provided

  if [ ! -f "$filename" ]; then
    echo "File not found: $filename" >&2
    return 1
  fi

  # Count the number of lines in the file
  local line_count
  line_count=$(wc -l < "$filename")

  # Check if the line count is less than the minimum
  if (( line_count > min_lines )); then
    return 0
  else
    return 1
  fi
}

start_tcp_dl_test() {
    local base_dir="$1"

    thrpt_protocol="tcp"
    duration_s=$TCP_DL_DURATION
    timeout_s=$TCP_DL_TIMEOUT
    interval_s=$TCP_DL_INTERVAL

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} downlink test" $start_ts_ms

    start_time=$(get_time_ms)

    log_file_name="${base_dir}/${thrpt_protocol}_downlink_${start_time}.out"
    echo "Start time: $start_ts_ms" > $log_file_name

    echo "Testing ${thrpt_protocol} downlink with $ip_address:$nuttcp_port, interval $interval_s s, duration $duration_s s, timeout $timeout_s s..."
    
    # Downlink test
    timeout $timeout_s nuttcp -r -F -v -i $interval_s -T $duration_s -p $nuttcp_port $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name

    echo "Saved ${thrpt_protocol} downlink test to $log_file_name"
    if check_file_lines_gt $log_file_name 2; then
      rate=$(grep_file "$log_file_name" "nuttcp-r" '([0-9]+(\.[0-9]+)?)\s*Mbps')
      echo "DL AVG TPUT: $rate"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "${thrpt_protocol} downlink test" $start_ts_ms $actual_end_ts_ms
}

wait_for_gap() {
    echo "Waiting for ${TEST_GAP_DURATION} s..."
    sleep $TEST_GAP_DURATION
}

# Run TCP DL to force cellular switching to 5G if supported
# Save the data points as the same as a TCP DL test
start_5g_booster() {
    local base_dir="$1"

    echo "Starting 5G booster for ${BOOSTER_5G_DURATION} s..."

    thrpt_protocol="tcp"
    duration_s=$BOOSTER_5G_DURATION
    timeout_s=$BOOSTER_5G_TIMEOUT
    interval_s=$BOOSTER_5G_INTERVAL

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)

    report_start_time "5G booster" $start_ts_ms

    log_file_name="${base_dir}/${thrpt_protocol}_downlink_$(get_time_ms).out"
    echo "Start time: $start_ts_ms" > $log_file_name

    timeout $timeout_s nuttcp -r -F -v -i $interval_s -T $duration_s -p $nuttcp_port $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name

    echo "Saved TCP DL results to $log_file_name"
    if check_file_lines_gt $log_file_name 2; then
      rate=$(grep_file "$log_file_name" "nuttcp-r" '([0-9]+(\.[0-9]+)?)\s*Mbps')
      echo "DL AVG TPUT: $rate"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "5G booster" $start_ts_ms $actual_end_ts_ms
}

start_tcp_ul_test() {
    local base_dir="$1"

    thrpt_protocol="tcp"
    duration_s=$TCP_UL_DURATION
    timeout_s=$TCP_UL_TIMEOUT
    interval_s=$TCP_UL_INTERVAL

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "${thrpt_protocol} uplink test" $start_ts_ms

    log_file_name="${base_dir}/${thrpt_protocol}_uplink_$(get_time_ms).out"
    echo "Start time: $start_ts_ms" > $log_file_name

    # tcp uplink test
    echo "testing tcp uplink with $ip_address:$nuttcp_port, interval $interval_s, duration $duration_s, timeout $timeout_s ..."
    timeout $timeout_s nuttcp -v -i $interval_s -T $duration_s -p $nuttcp_port $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name

    wait_util_end_time $estimated_end_ts_ms

    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name

    echo "Saved uplink test to $log_file_name"
    if check_file_lines_gt $log_file_name 2; then
      rate=$(grep_file "$log_file_name" "nuttcp-r" '([0-9]+(\.[0-9]+)?)\s*Mbps')
      echo "UL AVG TPUT: $rate"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "${thrpt_protocol} uplink test" $start_ts_ms $actual_end_ts_ms
}

start_icmp_ping_test() {
    local base_dir="$1"

    duration_s=$ICMP_PING_TEST_DURATION
    timeout_s=$ICMP_PING_TEST_TIMEOUT
    packet_size=$ICMP_PING_TEST_PACKET_SIZE
    interval_s=$ICMP_PING_TEST_INTERVAL

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "ICMP Ping test" $start_ts_ms

    log_file_name="${base_dir}/icmp_ping_$(get_time_ms).out"
    echo "Start time: $start_ts_ms" > $log_file_name

    if [[ "$OSTYPE" == "darwin"* ]]; then
        timeout $timeout_s ping -s $packet_size -i $interval_s -c $(printf "%.0f" $(echo "$duration_s / $interval_s" | bc -l)) $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name
    else
        timeout $timeout_s ping -s $packet_size -i $interval_s -w $duration_s $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name
    fi

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms" >> $log_file_name

    echo "Saved ping test to $log_file_name"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS format
        summary=$(grep_file "$log_file_name" "round-trip" "min/avg/max/stddev = ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+) ms")
    else
        # Android format
        summary=$(grep_file "$log_file_name" "rtt min/avg/max/mdev" "= ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+) ms")
    fi
    if check_file_lines_gt $log_file_name 2; then
      echo "Ping summary (min/avg/max/mdev) $summary"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "Ping test" $start_ts_ms $actual_end_ts_ms
}

start_traceroute_test() {
    local base_dir="$1"

    timeout_s=$TRACEROUTE_TEST_TIMEOUT

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "Traceroute test" $start_ts_ms

    log_file_name="${base_dir}/traceroute_$(get_time_ms).out"
    echo "Start time: $start_ts_ms">$log_file_name

    # tracerout to the target server
    timeout $timeout_s traceroute $ip_address | ts "[$TS_DATETIME_FORMAT]" >> $log_file_name
    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms">>$log_file_name

    echo "Saved traceroute test to $log_file_name"
    if check_file_lines_gt $log_file_name 2; then
      echo "Traceroute summary (min/avg/max/mdev) $summary"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "Traceroute test" $start_ts_ms $actual_end_ts_ms
}

start_tcp_ping_test() {
    local base_dir="$1"

    timeout_s=$TCP_RTT_TEST_TIMEOUT

    start_ts_ms=$(get_timestamp_ms)
    estimated_end_ts_ms=$(calculate_timestamp_ms $start_ts_ms $timeout_s)
    report_start_time "TCP Ping test" $start_ts_ms

    log_file_name="${base_dir}/tcp_ping_$(get_time_ms).out"
    echo "Start time: $start_ts_ms">$log_file_name

    export SERVER_HOST=$ip_address
    export PAYLOAD_SIZE=$TCP_RTT_TEST_PAYLOAD_SIZE
    export PACKET_COUNT=$TCP_RTT_TEST_COUNT
    export INTERVAL=$TCP_RTT_TEST_INTERVAL
    export LOG_FILE_PATH=$log_file_name

    TCP_RTT_SCRIPT_PATH="$(dirname "$0")/../../py_scripts/tcp_rtt_client.py"
    # Detect Python version and use the appropriate command
    if command -v python3 &>/dev/null; then
        timeout $timeout_s python3 $TCP_RTT_SCRIPT_PATH
    elif command -v python &>/dev/null; then
        timeout $timeout_s python $TCP_RTT_SCRIPT_PATH
    else
        echo "Error: Neither python3 nor python is available on this system."
    fi

    wait_util_end_time $estimated_end_ts_ms
    actual_end_ts_ms=$(get_timestamp_ms)
    echo "End time: $actual_end_ts_ms">>$log_file_name

    echo "Saved TCP Ping test to $log_file_name"
    
    if check_file_lines_gt $log_file_name 2; then
      1summary=$(grep_file "$log_file_name" "rtt min/avg/max/mdev" "= ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+) ms")
      echo "TCP Ping summary (min/avg/max/mdev) $summary"
    else
      echo "[CAUTION] EMPTY LOG!"
    fi
    report_end_time_and_duration "TCP Ping test" $start_ts_ms $actual_end_ts_ms
}

__print_divider__() {
    echo "-----------------------------------"
}

while true; do
    date_folder="${ROOT_DIR}/${operator}/$(get_date)"
    mkdir -p $date_folder

    # for this run
    time_folder="${date_folder}/$(get_time_ms)"
    mkdir -p $time_folder

    round=$((round+1))

    __print_divider__

    # TCP DL Test
    start_tcp_dl_test $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    start_5g_booster $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    # TCP UL Test
    start_tcp_ul_test $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    start_5g_booster $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    # ICMP Ping Test
    start_icmp_ping_test $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    start_5g_booster $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    # TCP Ping Test
    start_tcp_ping_test $time_folder

    __print_divider__

    wait_for_gap

    __print_divider__

    # Traceroute Test
    start_traceroute_test $time_folder

    __print_divider__

    echo "All tests completed, cleaning up..."

    __print_divider__

    if [ "$mode" -eq 2 ] || [ "$mode" -eq 3 ]; then
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




