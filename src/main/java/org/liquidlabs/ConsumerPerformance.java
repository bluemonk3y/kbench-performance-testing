package org.liquidlabs;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Replacing adding JMX metrics: https://jar-download.com/artifacts/org.apache.kafka/kafka_2.11/2.1.1/source-code/kafka/tools/ConsumerPerformance.scala
 *
 *  args: kafka-consumer-perf-test --broker-list ip-172-31-26-184.eu-west-1.compute.internal:9091 --topic  perf-topic-1 --messages 5000000
 *
 * Important properties affecting read performance:
 *
 * - receive.buffer.bytes - The size of the TCP receive buffer (SO_RCVBUF) to use when reading data
 *
 * - fetch.min.bytes - The minimum amount of data the server should return for a fetch request.
 *       If insufficient data is available the request will wait for that much data to accumulate before
 *       answering the request.
 *
 * - fetch.max.wait.ms - The maximum amount of time the server will block before answering the fetch
 *        request if there isn’t sufficient data to immediately satisfy the requirement given by
 *        fetch.min.bytes.
 *
 * See: https://docs.confluent.io/current/installation/configuration/consumer-configs.html
 */
public class ConsumerPerformance {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);

        String topic = res.getString("topic");
        long expectedRecords = res.getLong("messages");
        String brokerList = res.getString("broker-list");
        String group = res.getString("group");
        String offset = res.getString("auto.offset.reset");

        String consumerPropsOpt = res.getString("consumer.config");

        String clientId = res.getString("client.id");

        final int reportingIntervalOpt = 5000;



        Properties config = Utils.loadProps(consumerPropsOpt);
        config.put("client.id", clientId);
        if (!config.contains("group")) {
            config.put("group.id", group);
        }
        if (!config.contains("bootstrap.servers")) {
            config.put("bootstrap.servers", brokerList);
        }
        if (!offset.equals("not-set")) {
            config.put("auto.offset.reset", offset);
        } else {
            config.put("auto.offset.reset", "earliest");
        }
        log("auto.offset.reset:" + config.get("auto.offset.reset"));
        log("group.id:" + config.get("group.id"));

        if (config.get("auto.offset.reset").equals("earliest")) {
            AdminClient adminClient = AdminClient.create(config);
            adminClient.deleteConsumerGroups(Arrays.asList((String) config.get("group.id")));
        }

        log("Starting consumer..." + group + " - " + clientId);

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());


        //seekToOffset(timeStampString, consumer, topicName);

        consumer.subscribe(Pattern.compile(topic));
        Set<String> subscription = consumer.subscription();


        AtomicInteger recordCount = new AtomicInteger();
        AtomicLong totalBytes = new AtomicLong();
        Map<Integer, AtomicInteger> partitionCountMap = new HashMap();

        long start = System.currentTimeMillis();

        // turn into a kafka streams processor and use the windowing task with KTable:
        // similar to here: https://github.com/bluemonk3y/kgrafa/blob/master/src/main/java/io/confluent/kgrafa/MetricStatsCollector.java

        long lastLog = start;
        while (recordCount.get() < expectedRecords) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            process(partitionCountMap, records, recordCount, totalBytes);
            consumer.commitSync();
            if (lastLog < System.currentTimeMillis() - reportingIntervalOpt) {
                lastLog = System.currentTimeMillis();
                log(recordCount, totalBytes, lastLog - start);
            }
        }
        long end = System.currentTimeMillis();

        Long elapsed = Long.valueOf(end - start);

        // TODO add JMX Metrics (print every 5 seconds)
        // TODO: start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec,
        //  ---> rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec


        log(recordCount, totalBytes, elapsed);
        log("\nFinished\n","");


    }

    private static void log(AtomicInteger recordCount, AtomicLong totalBytes, Long elapsed) {
        log("Elapsed:%s", elapsed);
        log("records: %d", recordCount.get());
        log("records/sec %f", recordCount.get() / ((double) elapsed/1000.0));
        log("throughput: %f MB/s", ((totalBytes.get())/1024.0 * 1024.0) /(double) elapsed/1000.0);
        log("MB: %f", totalBytes.get()/ (1024.0 * 1024.0));
//        log.info("Partition record distribution: {}", partitionCountMap);
        System.out.println();
    }
    private static void log(String msg, Object... o) {
        System.out.printf(msg, o);
        System.out.print(" ");
    }

    // same as: https://github.com/bluemonk3y/kgrafa/blob/master/src/main/java/io/confluent/kgrafa/util/TimeSeeker.java
    private static void seekToOffset(String time, Consumer consumer, String topicName) throws ParseException {
        long timestampMs = SimpleDateFormat.getTimeInstance().parse(time).getTime();

        // Get the list of partitions
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
        List<TopicPartition> topicPartitionList = partitionInfos
                .stream()
                .map(info -> new TopicPartition(topicName, info.partition()))
                .collect(Collectors.toList());

        // Assign the consumer to these partitions
        consumer.assign(topicPartitionList);
        // Build a topic-timestamp map for filtering against
        Map<TopicPartition, Long> partitionTimestampMap = topicPartitionList.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> timestampMs));

        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(partitionTimestampMap);
        // Tell the consumer to seek for those offsets
        partitionOffsetMap.forEach((tp, offsetAndTimestamp) -> consumer.seek(tp, offsetAndTimestamp.offset()));
    }

    private static void process(Map<Integer, AtomicInteger> partitionCountMap, final ConsumerRecords<byte[], byte[]> records, AtomicInteger recordCount, AtomicLong totalBytes) {
        recordCount.addAndGet(records.count());

        Set<TopicPartition> partitions = records.partitions();
        partitions.stream().forEach( partition -> {
            partitionCountMap.putIfAbsent(partition.partition(), new AtomicInteger());
            partitionCountMap.get(partition.partition()).getAndAdd(records.records(partition).size());

        });
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
        while (iterator.hasNext()){
            ConsumerRecord<byte[], byte[]> next = iterator.next();
            totalBytes.getAndAdd(next.value().length);
        }
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("consumer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the consumer performance.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("consume messages from this topic");

        parser.addArgument("--consumer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG")
                .help("config file");

        parser.addArgument("--client.id")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("CONFIG")
                .help("config file");

        parser.addArgument("--consumer.auto.offset.reset")
                .action(store())
                .required(false).setDefault("not-set")
                .type(String.class)
                .dest("auto.offset.reset")
                .metavar("AUTO_OFFSET_REST")
                .help("auto offset reset file");

        parser.addArgument("--messages")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("messages").setDefault(10000000l)
                .help("number of messages to consume");

        parser.addArgument("--broker-list")
                .action(store())
                .required(false).setDefault("localhost:9092")
                .type(String.class)
                .dest("broker-list")
                .metavar("BROKER-LIST")
                .help("number of messages to consume");

        parser.addArgument("--group")
                .action(store())
//                .required(true)
                .type(String.class).setDefault("perf-consumer-group")
                .metavar("CONSUMER_GROUP")
                .help("consumer group name");

        return parser;
    }
    public static class Stats<K,V> {
        private final MessageDigest keyChecksum;
        private final MessageDigest valueChecksum;
        int numRecords;
        long bytes;
        int partitions;
        // per partitions? meh
        public Stats() throws NoSuchAlgorithmException {
            keyChecksum = MessageDigest.getInstance("MD5");
            valueChecksum = MessageDigest.getInstance("MD5");
        }
        public void update(ConsumerRecord<K,V> record) {
            keyChecksum.update(record.key().toString().getBytes());
            keyChecksum.update(record.value().toString().getBytes());
        }


        @Override
        public String toString() {
            return "Stats{" + // Or convert to hex
                    "keyChecksum=" + keyChecksum.digest() +
                    ", valueChecksum=" + valueChecksum.digest() +
                    ", numRecords=" + numRecords +
                    ", bytes=" + bytes +
                    ", partitions=" + partitions +
                    '}';
        }
    }
}
