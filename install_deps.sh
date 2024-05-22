# Install dependencies
dependencies="wget jq moreutils"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    apt install $dependencies
elif [[ "$OSTYPE" == "linux-android"* ]]; then
    pkg update

    # setup external storage in termux
    pkg install termux-am 
    termux-setup-storage

    pkg install $dependencies 
    
    usr_bin_path=/data/data/com.termux/files/usr/bin

    # check grpcurl version: https://github.com/fullstorydev/grpcurl/releases
    chmod +x grpcurl
    cp ./deps/grpcurl $usr_bin_path/grpcurl
    chmod +x $usr_bin_path/grpcurl

    # nuttcp: https://nuttcp.net/nuttcp/
    cp ./deps/nuttcp-8.1.4 $usr_bin_path/nuttcp
    chmod +x $usr_bin_path/nuttcp

elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install $dependencies
else
    echo "Failed to install dependencies: unsupported OS $OSTYPE"
    exit 1
fi