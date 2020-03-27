#!/bin/bash


if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi
RECORDS=5000000
BATCH=10000
LINGER=1000
INFLIGHT=1

kafka-producer-perf-test --producer.config config/client.properties  \
  --topic perf-topic-1 --num-records 5000000 --throughput -1 --record-size 2358 \
  --producer-props bootstrap.servers=$BROKER:9092 \
  buffer.memory=67108864 batch.size=$BATCH linger.ms=$LINGER max.in.flight.requests.per.connection=$INFLIGHT \
   acks=all compression.type=lz4
