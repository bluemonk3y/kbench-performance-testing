#!/bin/bash



if [ -z "$BROKER" ]
then
    echo "export BROKER to be the FQDN"
    exit 1
fi


FROM=1
TO=25
PARTITIONS=10
BOOTSTRAP=dc2sp-liks-066.idm.gsd.local:9092

for (( i=$FROM; i<=$TO; i++ ))
do
  TOPIC=perf-topic-$i
  echo "Creating topic:" $TOPIC

  kafka-topics --delete --bootstrap-server $BOOTSTRAP --command-config config/client.properties --topic $TOPIC

  kafka-topics --create --bootstrap-server $BOOTSTRAP --command-config config/client.properties \
       --replication-factor 3 --partitions $PARTITIONS --config min.insync.replicas=2 --topic  $TOPIC

done

