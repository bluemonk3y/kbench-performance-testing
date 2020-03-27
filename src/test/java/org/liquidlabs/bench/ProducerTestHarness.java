package org.liquidlabs.bench;

import org.junit.jupiter.api.Test;

public class ProducerTestHarness {

    @Test
    public void doStuff() {

        // ./kafka-producer-perf-test --topic perf-topic-1 --num-records 300000 --throughput -1 --record-size 2358 --producer-props bootstrap.servers=ubu-server-1:9091 buffer.memory=67108864 batch.size=10000 acks=all compression.type=gzip
        org.apache.kafka.tools.ProducerPerformance producerPerformance = new org.apache.kafka.tools.ProducerPerformance();

//        kafka.tools.ConsumerPerformance

    }
}
