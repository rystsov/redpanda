package io.vectorized.tests;

import java.util.Properties;
import java.lang.Math;
import java.util.Arrays;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TxSendBench 
{
    public Producer<String, String> producer;

    public TxSendBench(String connection, String txId) {
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(pprops);
        this.producer.initTransactions();
    }

    void warmup(String topic1, String topic2, int iterations) throws Exception {
        long started = System.nanoTime();
        for (int i=0;i<iterations;i++) {
            producer.beginTransaction();
            var f1 = producer.send(new ProducerRecord<String, String>(topic1, "key"+i, "value"+i));
            var f2 = producer.send(new ProducerRecord<String, String>(topic2, "key"+i, "value"+i));
            producer.commitTransaction();
            f1.get();
            f2.get();
        }
        long elapsed = System.nanoTime() - started;
        System.out.println("warmed up " + iterations + " txes in " + elapsed + "ns");
    }

    void measure(String topic1, String topic2, int iterations) throws Exception {
        long[] measures = new long[iterations];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long started = System.nanoTime();
        for (int i=0;i<iterations;i++) {
            long tx_started = System.nanoTime();
            producer.beginTransaction();
            var f1 = producer.send(new ProducerRecord<String, String>(topic1, "key"+i, "value"+i));
            var f2 = producer.send(new ProducerRecord<String, String>(topic2, "key"+i, "value"+i));
            producer.commitTransaction();
            f1.get();
            f2.get();
            long tx_elapsed = System.nanoTime() - tx_started;
            min = Math.min(min, tx_elapsed);
            max = Math.max(max, tx_elapsed);
            measures[i] = tx_elapsed;
        }
        long elapsed = System.nanoTime() - started;
        System.out.println("measured " + iterations + " txes in " + elapsed + "ns");
        System.out.println("min: " + min + "ns");
        System.out.println("max: " + max + "ns");
        Arrays.sort(measures);
        System.out.println("median: " + measures[measures.length / 2] + "ns");
    }

    public static void main( String[] args ) throws Exception
    {
        var bench = new TxSendBench("127.0.0.1:9092", "my-tx-1");
        bench.warmup("topic1", "topic2", 100);
        bench.measure("topic1", "topic2", 1000);
    }
}
