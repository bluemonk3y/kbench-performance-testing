#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi

echo "Stats require at least 1 minute to become stable"
export RECORDS=15000000
## about 50k per second, about 3million per minute, needs 5 minutes of testing - total amount about  15m
###expot RECORDS=1000
export BATCH=7000
export LINGER=1000
export INFLIGHT=1
export COUNT=1
echo "Running producers:" $COUNT `date`
#./run-producers.sh

echo "Waiting for 3 minutes to clear stats" `date`
###sleep 180

COUNT=10
echo "Running producers:" $COUNT `date`
./run-producers.sh

echo "Waiting for 3 minutes to clear stats" `date`
###sleep 180

COUNT=25
echo "Running producers:" $COUNT `date`
##./run-producers.sh

echo "Finished running tests" `date`

