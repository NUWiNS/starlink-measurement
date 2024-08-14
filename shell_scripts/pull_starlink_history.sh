#!/bin/bash

data_folder=~/storage/shared/hawaii_starlink_trip/dish_history/$(date '+%Y%m%d')/$(date '+%H%M%S%3N')
mkdir -p $data_folder

file_name="dish_history.out"
output_file=$data_folder/$file_name

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

echo "Pulling dish history data..."

start_timestamp=$(get_timestamp_in_millisec)
echo "Start time: ${start_timestamp}" > $output_file

grpcurl -plaintext -emit-defaults -d "{\"get_history\":{}}" 192.168.100.1:9200 SpaceX.API.Device.Device/Handle >> $output_file 2>&1 

end_timestamp=$(get_timestamp_in_millisec)
echo "End time: ${end_timestamp}">>$output_file

echo "Pulled dish history data to $output_file"