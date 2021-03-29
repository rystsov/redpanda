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

/**
 * Unit test for simple App.
 */
public class TxConsumerReadCommittedSeekTest extends Consts 
{
    @BeforeAll
    static void initAll() throws Exception {
        // reusing tx ids to abort any ongoing transactions
        
        var producer = new TxProducer(connection, txId1);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.close();

        producer = new TxProducer(connection, txId2);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.close();
    }

    @Test
    public void txlessSeekTest() throws Exception
    {
        var producer = new SimpleProducer(connection);
        long offset = producer.send(topic1, "key1", "value1");
        producer.close();

        var consumer = new TxConsumer(connection, topic1, true);
        consumer.seekToEnd();
        
        int retries = 8;
        while (offset >= consumer.position() && retries > 0) {
            // partitions lag behind a coordinator
            // we can't avoid sleep :(
            Thread.sleep(500);
            consumer.seekToEnd();
            retries--;
        }
        assertThat(offset, lessThan(consumer.position()));

        consumer.close();
    }

    @Test
    public void txSeekTest() throws Exception
    {
        var producer = new TxProducer(connection, txId1);
        producer.initTransactions();
        long offset = producer.commitTx(topic1, "key1", "value1");
        producer.close();

        var consumer = new TxConsumer(connection, topic1, true);
        consumer.seekToEnd();

        int retries = 8;
        while (offset >= consumer.position() && retries > 0) {
            // partitions lag behind a coordinator
            // we can't avoid sleep :(
            Thread.sleep(500);
            consumer.seekToEnd();
            retries--;
        }
        assertThat(offset, lessThan(consumer.position()));

        consumer.close();
    }

    @Test
    public void seekRespectsOngoingTx() throws Exception
    {
        var producer = new TxProducer(connection, txId1);
        producer.initTransactions();
        producer.beginTransaction();
        long offset = producer.send(topic1, "key1", "value1");
        
        var consumer = new TxConsumer(connection, topic1, true);
        consumer.seekToEnd();
        assertThat(consumer.position(), lessThanOrEqualTo(offset));
        
        producer.commitTransaction();
        producer.close();
        consumer.close();
    }
}
