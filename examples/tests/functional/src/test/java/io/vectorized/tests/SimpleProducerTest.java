package io.vectorized.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Unit test for simple App.
 */
public class SimpleProducerTest extends Consts 
{
    @Test
    public void sendPasses() throws Exception
    {
        var producer = new SimpleProducer(connection);
        producer.send(topic1, "key1", "value1");
        producer.close();
    }
}
