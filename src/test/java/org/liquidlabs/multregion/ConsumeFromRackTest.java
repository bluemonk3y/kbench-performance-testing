package org.liquidlabs.multregion;

import org.junit.jupiter.api.Test;

public class ConsumeFromRackTest {


    @Test
    public void shouldFailoverRacks() throws Exception {

        String[] racks = "eu-east-1a eu-west-1a".split(" ");

        String[] args = "--topicName perf-topic-1 --rack eu-east-1a --bootstrapServer ubu-server-1:9092 --consumerGroup mregion-group1".split(" ");
        ConsumeFromRack.main(args);

    }
}
