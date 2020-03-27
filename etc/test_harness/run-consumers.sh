#!/bin/bash
if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi
echo "RECORD" $RECORDS $COUNT
RECORDS=${RECORDS-500}
GROUP=${GROUP-101}
OUT=$COUNT
FROM=1
# run processes and store pids in array
for (( i=$FROM; i<=$COUNT; i++ ))
do

  OUT=c-$COUNT
  mkdir -p out/$OUT 
  TOPIC=perf-topic-.*
  #TOPIC=perf-topic-1
  #kafka-consumer-perf-test --broker-list $BROKER:9092 --consumer.config config/client.properties \
  # --show-detailed-stats --group perf-test-consumer \
  # --show-detailed-stats --group perf-test-consumer \
  # this version supports wildcards
  java -jar perf-client.jar --broker-list $BROKER:9092 --consumer.config config/client.properties \
    --group perf-test-consumer-$GROUP --client.id perf-consumer-$i \
    --topic $TOPIC --messages $RECORDS > out/$OUT/$i.out &
  pids[${i}]=$!
done

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done



