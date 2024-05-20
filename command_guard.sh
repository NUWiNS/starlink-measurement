# Pre-requisite: Install 
# - grpcurl: a client for fetching data from gRPC APIs, https://github.com/fullstorydev/grpcurl
# - jq: a command-line JSON processor, https://jqlang.github.io/jq/

prerequisits="grpcurl jq"
error_code=0

for _command in $prerequisits; do
  if ! command -v $_command &> /dev/null
  then
    echo "Error: $_command could not be found. Please install $_command."
    ((error_code++))
  fi
done

exit $error_code
