#!/bin/bash

# Install dependencies
# wget - download files from the web
# jq - json processor, used for parsing json data
# moreutils - additional unix utilities
# dnsutils - dns utilities, used for dns queries
# traceroute - network diagnostic tool, used for tracing the route to a host
# bc - basic calculator, used for arithmetic operations
dependencies="wget jq moreutils bc coreutils"
android_dependencies="dnsutils traceroute"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    apt install $dependencies
elif [[ "$OSTYPE" == "linux-android"* ]]; then
    pkg update

    # setup external storage in termux
    if ! command -v termux-setup-storage &> /dev/null
    then
        pkg install termux-am 
        termux-setup-storage
    fi

    pkg install $dependencies $android_dependencies
    
    usr_bin_path=/data/data/com.termux/files/usr/bin

    if ! command -v grpcurl &> /dev/null
    then
        # check grpcurl version: https://github.com/fullstorydev/grpcurl/releases
        cp ./deps/grpcurl $usr_bin_path/grpcurl
        chmod +x $usr_bin_path/grpcurl
        echo "grpcurl installed in $usr_bin_path"
    fi

    # nuttcp: https://nuttcp.net/nuttcp/
    if ! command -v nuttcp &> /dev/null 
    then
        cp ./deps/nuttcp-8.1.4 $usr_bin_path/nuttcp
        chmod +x $usr_bin_path/nuttcp
        echo "nuttcp installed in $usr_bin_path"
    fi

    if ! command -v iperf3 &> /dev/null
    then
        cp ./deps/iperf3.9 $usr_bin_path/iperf3
        chmod +x $usr_bin_path/iperf3
        echo "iperf3 installed in $usr_bin_path"
    fi

elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install $dependencies
    
    usr_bin_path=/usr/local/bin

    if ! command -v grpcurl &> /dev/null
    then
       brew install grpcurl
    fi

    # nuttcp: https://nuttcp.net/nuttcp/
    if ! command -v nuttcp &> /dev/null 
    then
        brew install nuttcp
    fi

    if ! command -v iperf3 &> /dev/null
    then
        brew install iperf3
    fi
else
    echo "Failed to install dependencies: unsupported OS $OSTYPE"
    exit 1
fi