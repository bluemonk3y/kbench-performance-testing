#!/bin/bash
if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi
echo "RECORD" $RECORDS $COUNT
RECORDS=${RECORDS-500}
BATCH=${BATCH-10000}
LINGER=${LINGER-1000}
INFLIGHT=${INFLIGHT-1}
OUT=$COUNT
FROM=1
THROUGHPUT=-1
SIZE=2358
# run processes and store pids in array
for (( i=$FROM; i<=$COUNT; i++ ))
do

  OUT=p-$COUNT-$BATCH-$LINGER

  mkdir -p out/$OUT 

  TOPIC=perf-topic-$i
  #TOPIC=perf-topic-1

  kafka-producer-perf-test --producer.config config/client.properties  \
  --topic $TOPIC --num-records $RECORDS --throughput $THROUGHPUT --record-size $SIZE \
   --print-metrics \
  --producer-props bootstrap.servers=$BROKER:9092 \
  buffer.memory=67108864 batch.size=$BATCH linger.ms=$LINGER max.in.flight.requests.per.connection=$INFLIGHT \
   acks=all compression.type=lz4 > out/$OUT/$i.out &

 pids[${i}]=$!
done

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done



