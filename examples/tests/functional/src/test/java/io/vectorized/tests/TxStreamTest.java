package io.vectorized.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.errors.ProducerFencedException;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.Map;
import java.lang.Math;
import org.junit.jupiter.api.BeforeAll;
import java.lang.Thread;

public class TxStreamTest extends Consts 
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
    public void testSetGroupStartOffset() throws Exception
    {
        TxStream stream = new TxStream(getConnection());
        stream.initProducer(txId1);
        stream.initConsumer(topic1, groupId, true);
        stream.setGroupStartOffset(0);
        stream.close();
    }

    @Test
    public void test() throws Exception
    {
        var producer = new SimpleProducer(getConnection());
        long target_offset = producer.send(topic2, "noop-1", "noop");
        producer.close();
        
        Map<Long, TxRecord> input = new HashMap<>(); 
        TxStream stream = new TxStream(getConnection());
        stream.initProducer(txId1);

        long first_offset = Long.MAX_VALUE;
        long last_offset=0;
        for (int i=0;i<3;i++) {
            TxRecord record = new TxRecord();
            record.key = "key" + i;
            record.value = "value" + i;
            record.offset = stream.commitTx(topic1, record.key, record.value);
            first_offset = Math.min(first_offset, record.offset);
            last_offset = record.offset;
            input.put(record.offset, record);
        }

        stream.initConsumer(topic1, groupId, true);
        stream.setGroupStartOffset(first_offset);
        var mapping = stream.process(last_offset, x -> x.toUpperCase(), 1, topic2);
        stream.close();
        
        var consumer = new TxConsumer(getConnection(), topic2, true);
        var transformed = consumer.readN(target_offset+1, 3, 1);

        for (var target : transformed) {
            assertTrue(mapping.containsKey(target.offset));
            long source_offset = mapping.get(target.offset);
            assertTrue(input.containsKey(source_offset));
            var source = input.get(source_offset);
            assertEquals(source.key, target.key);
            assertEquals(source.value.toUpperCase(), target.value);
        }
    }
}
