package io.vectorized.tests;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Arrays;
import java.util.function.Function;
import java.lang.Math;
import java.lang.Thread;
import java.util.Properties;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FetchBench 
{
    String connection;

    String topic;
    TopicPartition tp;
    List<TopicPartition> tps;

    Producer<String, String> producer;
    Consumer<String, String> consumer;
    volatile long lastOffset = -1;

    public FetchBench(String connection, String topic) {
        this.connection = connection;
        this.topic = topic;
        this.tp = new TopicPartition(topic, 0);
        this.tps = Collections.singletonList(new TopicPartition(topic, 0));
    }

    public void initProducer(String txId) throws Exception {
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(pprops);
        this.producer.initTransactions();
    }

    public void initConsumer() {
        var isolation = "read_committed";

        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(cprops);
        this.consumer.assign(this.tps);
    }

    public long commitTx(String key, String value) throws Exception {
        producer.beginTransaction();
        var future = producer.send(new ProducerRecord<String, String>(topic, key, value));
        long offset = future.get().offset();
        producer.commitTransaction();
        return offset;
    }

    long read(long startOffset, long target) throws Exception {
        consumer.seek(tp, startOffset);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            var it = records.iterator();
            while (it.hasNext()) {
                var record = it.next();
                if (record.offset() == target) {
                    return System.nanoTime() - Long.parseLong(record.value());
                }
            }
        }
    }
    
    void write(int iterations) throws Exception {
        long[] measures = new long[iterations];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long started = System.nanoTime();
        
        long last = commitTx("key0", ""+System.nanoTime());
        for (int i=0;i<iterations;i++) {
            long next = commitTx("key"+i, ""+System.nanoTime());
            long tx_elapsed = read(last, next);
            last = next;
            measures[i] = tx_elapsed;
            min = Math.min(min, tx_elapsed);
            max = Math.max(max, tx_elapsed);
        }

        long elapsed = System.nanoTime() - started;
        System.out.println("measured w/r " + iterations + " in " + elapsed + "ns");
        System.out.println("min: " + min + "ns");
        System.out.println("max: " + max + "ns");
        Arrays.sort(measures);
        System.out.println("median: " + measures[measures.length / 2] + "ns");
    }

    public static void main( String[] args ) throws Exception
    {
        var bench = new FetchBench("172.31.20.243:9092", "topic1");
        bench.initProducer("my-tx-1");
        bench.initConsumer();
        bench.write(1000);
    }
}
