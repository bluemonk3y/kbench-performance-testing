#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi

curl -s http://$BROKER:7771/jolokia/read/java.lang:type=Memory/HeapMemoryUsage/used
