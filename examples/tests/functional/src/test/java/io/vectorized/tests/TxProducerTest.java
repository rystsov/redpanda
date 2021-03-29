package io.vectorized.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.errors.ProducerFencedException;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for simple App.
 */
public class TxProducerTest extends Consts 
{
    @Test
    public void initPasses() throws Exception
    {
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        producer.close();
    }

    @Test
    public void txPasses() throws Exception
    {
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.close();
    }

    @Test
    public void txesPass() throws Exception
    {
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        producer.commitTx(topic1, "key1", "value1");
        producer.commitTx(topic1, "key2", "value2");
        producer.close();
    }

    @Test
    public void abortPasses() throws Exception
    {
        var producer = new TxProducer(getConnection(), txId1);
        producer.initTransactions();
        producer.abortTx(topic1, "key1", "value1");
        producer.close();
    }

    @Test
    public void commutingTxesPass() throws Exception {
        var p1 = new TxProducer(getConnection(), txId1);
        var p2 = new TxProducer(getConnection(), txId2);
        p1.initTransactions();
        p1.beginTransaction();
        p1.send(topic1, "key1", "p1:value1");
        p2.initTransactions();
        p2.beginTransaction();
        p2.send(topic1, "key1", "p2:value1");
        p1.commitTransaction();
        p2.commitTransaction();
    }

    @Test
    public void conflictingTxesFail() throws Exception {
        var p1 = new TxProducer(getConnection(), txId1);
        var p2 = new TxProducer(getConnection(), txId1);
        p1.initTransactions();
        p1.beginTransaction();
        p1.send(topic1, "key1", "p1:value1");
        p2.initTransactions();
        p2.beginTransaction();
        p2.send(topic1, "key1", "p2:value1");
        try {
            p1.commitTransaction();
            fail("commit must throw ProducerFencedException");
        } catch(ProducerFencedException e) {
            // eating ProducerFencedException
        }
        p2.commitTransaction();
        p2.close();
        p1.close();
    }
}
