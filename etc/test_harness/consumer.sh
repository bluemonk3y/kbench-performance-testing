#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi

kafka-consumer-perf-test --broker-list $BROKER:9092 --consumer.config config/client.properties --topic  perf-topic-1 --messages 5000000
