package io.vectorized.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.errors.ProducerFencedException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeAll;
import java.lang.Thread;
import java.util.HashMap;
import java.util.Map;

public class TxConsumerReadCommittedReadTest extends Consts 
{
    @BeforeAll
    static void initAll() throws Exception {
        // reusing tx ids to abort any ongoing transactions
        
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.close();

        producer = new TxProducer(getConnection(), txId2);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.close();
    }

    @Test
    public void fetchReadsComittedTest() throws Exception
    {
        Map<String, Long> offsets = new HashMap<>();
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        long first_offset = producer.commitTx(topic1, "key1", "value1");
        offsets.put("key1", first_offset);
        for (int i=2;i<10;i++) {
            long offset = producer.commitTx(topic1, "key"+i, "value"+i);
            offsets.put("key"+i, offset);
        }
        long last_offset = producer.commitTx(topic1, "key10", "value10");
        offsets.put("key10", last_offset);
        producer.close();

        var consumer = new TxConsumer(getConnection(), topic1, true);
        consumer.seekToEnd();
        int retries = 8;
        while (last_offset >= consumer.position() && retries > 0) {
            // partitions lag behind a coordinator
            // we can't avoid sleep :(
            Thread.sleep(500);
            consumer.seekToEnd();
            retries--;
        }
        assertThat(last_offset, lessThan(consumer.position()));

        var records = consumer.read(first_offset, last_offset, 1);
        consumer.close();
        assertEquals(records.size(), offsets.size());
        for (var record : records) {
            assertTrue(offsets.containsKey(record.key));
            assertEquals(offsets.get(record.key), record.offset);
        }
    }

    @Test
    public void fetchDoesntReadAbortedTest() throws Exception
    {
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        long first_offset = producer.commitTx(topic1, "key1", "value1");
        producer.abortTx(topic1, "key2", "value2");
        long last_offset = producer.commitTx(topic1, "key3", "value3");
        producer.close();

        var consumer = new TxConsumer(getConnection(), topic1, true);
        consumer.seekToEnd();
        int retries = 8;
        while (last_offset >= consumer.position() && retries > 0) {
            // partitions lag behind a coordinator
            // we can't avoid sleep :(
            Thread.sleep(500);
            consumer.seekToEnd();
            retries--;
        }
        assertThat(last_offset, lessThan(consumer.position()));

        var records = consumer.read(first_offset, last_offset, 1);
        consumer.close();
        assertEquals(records.size(), 2);
        for (var record : records) {
            if (record.key.equals("key1")) {
                assertEquals(first_offset, record.offset);
            } else if (record.key.equals("key3")) {
                assertEquals(last_offset, record.offset);
            } else {
                fail("Unexpected key: " + record.key);
            }
        }
    }
}
