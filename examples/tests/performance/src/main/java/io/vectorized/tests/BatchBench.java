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
import java.util.Properties;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BatchBench 
{
    String connection;
    String topic;
    Producer<String, String> producer;

    public BatchBench(String connection, String topic) {
        this.connection = connection;
        this.topic = topic;
    }

    public void initProducer(String txId, int inFlight, int batchSize) throws Exception {
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        pprops.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, inFlight);
        pprops.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        
        //batch.size
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(pprops);
        this.producer.initTransactions();
    }

    void measure(int iterations) throws Exception {
        long started = System.nanoTime();
        producer.beginTransaction();
        for (int i=0;i<iterations;i++) {
            producer.send(new ProducerRecord<String, String>(topic, "key"+i, "value"+i));
        }
        producer.commitTransaction();
        long elapsed = System.nanoTime() - started;
        System.out.println("insert " + iterations + " records in a single tx in " + elapsed + "ns");
    }

    public static void main( String[] args ) throws Exception
    {
        int messages = 200000;
        int inFlight = 1;
        int batchSize = 1384;
        try {
            messages = Integer.parseInt(args[0]);
            inFlight = Integer.parseInt(args[1]);
            batchSize = Integer.parseInt(args[2]);
        } catch (Exception e) {
            System.out.println("BatchBench num of messages (e.g. 200000), max in flight requests (e.g. 1), batch size (e.g. 1384)");
            return;
        } 
        var bench = new BatchBench("127.0.0.1:9092", "topic1");
        bench.initProducer("my-tx-1");
        bench.measure(messages, inFlight, batchSize);
    }
}
