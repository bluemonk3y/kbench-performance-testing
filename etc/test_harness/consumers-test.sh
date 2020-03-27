#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi

echo "Stats require at least 1 minute to become stable"
export RECORDS=15000000
###export RECORDS=1000000
export COUNT=5
export GROUP=25-1448
echo "Running consumers:" $COUNT
./run-consumers.sh


