package io.vectorized.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.errors.ProducerFencedException;
import java.util.concurrent.ExecutionException;
import java.lang.Thread;

/**
 * Unit test for simple App.
 */
public class TxConsumerReadUncommittedSeekTest extends Consts 
{
    @Test
    public void txlessSeekTest() throws Exception
    {
        var producer = new SimpleProducer(connection);
        long offset = producer.send(topic1, "key1", "value1");
        producer.close();

        var consumer = new TxConsumer(connection, topic1, false);
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

        var consumer = new TxConsumer(connection, topic1, false);
        
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
    public void seekDoesntRespectOngoingTx() throws Exception
    {
        var producer = new TxProducer(connection, txId1);
        producer.initTransactions();
        producer.beginTransaction();
        long offset = producer.send(topic1, "key1", "value1");
        
        var consumer = new TxConsumer(connection, topic1, false);
        
        int retries = 8;
        while (offset >= consumer.position() && retries > 0) {
            // partitions lag behind a coordinator
            // we can't avoid sleep :(
            Thread.sleep(500);
            consumer.seekToEnd();
            retries--;
        }
        assertThat(offset, lessThan(consumer.position()));
        
        producer.commitTransaction();
        producer.close();
        consumer.close();
    }
}
