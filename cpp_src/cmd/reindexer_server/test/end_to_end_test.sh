HOST=127.0.0.1
PORT=9088


CHECK_TIMEOUT=5
CHECK_ATTEMPTS=10

echo "Awaiting of server startup..."

attempt=1
while ((1==1)); do
  if [[ attempt -gt CHECK_ATTEMPTS ]]; then
    echo "Unable to establish a connection to the server"
    exit 1
  fi

  echo "Attempt: $attempt"

  result="$(curl -s -G "${HOST}:${PORT}/api/v1/check")"

  if [[ "$result" ]]; then
    echo "Connection is successfully established"
    break
  fi

  sleep ${CHECK_TIMEOUT}

  attempt=`expr $attempt + 1`
done

if [ -z "$1" ]; then
  pytest . --alluredir=/builds/itv-backend/reindexer/allure-results
else
  pytest . -k "$1" --alluredir=/builds/itv-backend/reindexer/allure-results
fi
test_exit_code=$?

pycleanup --cache --pyc --egg > /dev/null

exit $test_exit_code
