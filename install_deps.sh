# Install dependencies
dependencies="grpcurl jq"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    apt install $dependencies
elif [[ "$OSTYPE" == "linux-android"* ]]; then
    pkg install $dependencies
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install $dependencies
else
    echo "Failed to install dependencies: unsupported OS $OSTYPE"
    exit 1
fi