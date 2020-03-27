package org.liquidlabs.multregion;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static net.sourceforge.argparse4j.impl.Arguments.store;

//import joptsimple._

/**
 *
 * See: https://docs.confluent.io/current/installation/configuration/consumer-configs.html
 */
public class ConsumeFromRack {

    private final static org.slf4j.Logger log = LoggerFactory.getLogger(ConsumeFromRack.class);


    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);

        String topicName = res.getString(ARGS.topicName.name());
        String rack = res.getString(ARGS.rack.name());
        String consumerGroup = res.getString(ARGS.consumerGroup.name());
        String bootstrapServers = res.getString(ARGS.bootstrapServers.name());
        String configFile = res.getString(ARGS.configFile.name());


        log.info("Starting consumer...");

        Properties config = Utils.loadProps(configFile);
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("group.id", consumerGroup);
        // to read from broker rack
        config.put("client.rack", rack);
        config.put("bootstrap.servers", bootstrapServers);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("auto.offset.reset", "earliest" /** earliest/latest/none **/);

        System.out.println("Consuming from:" + topicName + " Broker:" + bootstrapServers + " Rack:" + rack);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(Arrays.asList(topicName));

        AtomicInteger recordCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        int expectedRecords = 100 * 1000;

        while (recordCount.get() < expectedRecords) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> System.out.println(record.value()));
            System.out.println("Records:" + records.count());
            consumer.commitSync();
            recordCount.getAndAdd(records.count());
            Thread.sleep(1000);
        }
        long end = System.currentTimeMillis();

        Long elapsed = Long.valueOf(end - start);

        log.info("Elapsed:{}", elapsed);
        log.info("Total record: {}", recordCount);
        log.info("Record: throughput {}", Double.valueOf(recordCount.get() / elapsed));

    }

    // same as: https://github.com/bluemonk3y/kgrafa/blob/master/src/main/java/io/confluent/kgrafa/util/TimeSeeker.java
    /** Get the command-line argument parser. */
    enum ARGS  { rack, consumerGroup, topicName, configFile, bootstrapServers}
    private static ArgumentParser argParser() {

        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("consumer-region")
                .defaultHelp(true)
                .description("This tool is used to verify the Consumer failover from different regions");

        parser.addArgument("--" + ARGS.consumerGroup.name())
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("CONSUMER-GROUP").setDefault("MultiRegionRackTestGroup")
                .help("consumer group to use");

        parser.addArgument("--" + ARGS.bootstrapServers.name())
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("Bootstrap servers").setDefault("localhost:9092")
                .help("broker to use");

        parser.addArgument("--" + ARGS.rack.name())
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("RACK")
                .help("consumer rack to read from");

        parser.addArgument("--" + ARGS.topicName.name())
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC").setDefault("MultiRegionRackTestTopic")
                .help("produce messages to this topic");

        parser.addArgument("--" + ARGS.configFile.name())
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .help("producer config properties file.");



        return parser;
    }
}
