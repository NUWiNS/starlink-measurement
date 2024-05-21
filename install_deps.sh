# Install dependencies
dependencies="wget jq"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    apt install $dependencies
elif [[ "$OSTYPE" == "linux-android"* ]]; then
    pkg update
    pkg install $dependencies
    # check grpcurl version: https://github.com/fullstorydev/grpcurl/releases
    wget https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_arm64.tar.gz
    tar -xvf grpcurl_1.9.1_linux_arm64.tar.gz
    chmod +x grpcurl
    mv grpcurl /data/data/com.termux/files/usr/bin/
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install $dependencies
else
    echo "Failed to install dependencies: unsupported OS $OSTYPE"
    exit 1
fi