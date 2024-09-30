# NUWiNS - Starlink Measurements

## Datasets

Download datasets from Google Drive: https://drive.google.com/drive/u/4/folders/1_ODEYeyA3XR53mqqlgC685umIRwEL4T2

Enter the folder of each trip and download the files following the path `<trip_name>/PhoneData/starlink_<trip_name>_phone_data.zip`.

Once downloaded, extract the files and place them in the `datasets` folder.

The structure of the `datasets` folder should be as follows:

```
datasets
└── alaska_starlink_trip
    └── raw
└── hawaii_starlink_trip
    └── raw
└── maine_starlink_trip
    └── raw
```

## Pre-requisites

Python 3.11 or higher is required to run the scripts.

Create a virtual environment by running the following command:

```bash
python3 -m venv .venv
# for mac and linux
source .venv/bin/activate
```

Install the required packages at the root path by running the following command:

```bash
pip install -r requirements.txt
```

## Data Processing

Enter the `scripts` folder, and enter the folder for the trip you want to process.

Run the `main.py` script to process the data.

```
python3 <trip_name>/main.py
```

The plots will be saved in the `outputs` folder.