#!/bin/bash

# Read env file from the same directory
if [ -f "$(dirname "$0")/env" ]; then
    source "$(dirname "$0")/env"
    echo "Loaded environment variables from env file"
else
    echo "Error: env file not found in the same directory"
    exit 1
file
fi

# Check if essential variables are set
if [ -z "$ROOT_DIR" ]; then
    echo "Error: ROOT_DIR is not set. Please check your env file along with the script."
    exit 1
fi

data_folder=${ROOT_DIR}/weather_area/$(date '+%Y%m%d')/
file_name="weather_area_record.out"
file_path=$data_folder$file_name

get_datetime_with_iso_8601_local_timezone() {
    ISO_8601_TIMEZONE_FORMAT="%Y-%m-%dT%H:%M:%S.%6N%:z"

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        date +"$ISO_8601_TIMEZONE_FORMAT"
    elif [[ "$OSTYPE" == "linux-android"* ]]; then
        date +"$ISO_8601_TIMEZONE_FORMAT"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        gdate +"$ISO_8601_TIMEZONE_FORMAT"
    else
        date +"$ISO_8601_TIMEZONE_FORMAT"
    fi
}

# create the data folder
if [ ! -d "$$data_folder" ]; then
  echo "Creating a data folder: $data_folder"
  mkdir -p $data_folder
fi

if [ ! -f "$$file_path" ]; then
  echo "Creating a record file: $file_path"
  echo "----------------------" 
  touch $file_path
fi

echo "The recoreder will log the timestamp with any weather or area changes in the file: $file_path"
echo "----------------------" 

while true; do
  read -p "Do you notice any changes? enter 1 for weather, 2 for area: " choice

  case $choice in
    1)
      read -p "What is the weather like? enter 1 for clear, 2 for cloudy, 3 for rainy, 4 for snowy: " weather_choice
      case $weather_choice in
        1)
          weather="clear"
          ;;
        2)
          weather="cloudy"
          ;;
        3)
          weather="rainy"
          ;;
        4)
          weather="snowy"
          ;;
        *)
          echo "Invalid input, enter 1 for clear, 2 for cloudy, 3 for rainy, 4 for snowy"
          echo "----------------------" 
          continue
          ;;
      esac
      current_time=$(get_datetime_with_iso_8601_local_timezone)
      echo "[${current_time}] weather: $weather is recorded"
      echo "[${current_time}] weather: $weather" >> $file_path
      ;;
    2)
      read -p "What area are you in? enter 1 for urban, 2 for suburban, 3 for rural: " area_choice
      case $area_choice in
        1)
          area="urban"
          ;;
        2)
          area="suburban"
          ;;
        3)
          area="rural"
          ;;
        *)
          echo "Invalid input, enter 1 for urban, 2 for suburban, 3 for rural"
          echo "----------------------" 
          continue
          ;;
      esac
      current_time=$(get_datetime_with_iso_8601_local_timezone)
      echo "[${current_time}] area: $area is recorded"
      echo "[${current_time}] area: $area" >> $file_path
      ;;
    *)
      echo "Invalid input, please enter 1 for weather, 2 for area"
      echo "----------------------" 
      continue
      ;;
  esac
  echo "----------------------" 
done





