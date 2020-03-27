#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi

curl http://$BROKER:7771/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
