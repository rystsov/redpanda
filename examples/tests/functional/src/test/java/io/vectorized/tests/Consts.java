package io.vectorized.tests;

public class Consts 
{
    public static String KAFKA_CONNECTION_PROPERTY = "KAFKA_CONNECTION";
    
    public static String topic1 = "topic1";
    public static String topic2 = "topic2";
    public static String txId1 = "my-tx-1";
    public static String txId2 = "my-tx-2";
    public static String groupId = "groupId";

    public static String getConnection() {
        var env = System.getenv();
        if (env.containsKey(KAFKA_CONNECTION_PROPERTY)) {
            return env.get(KAFKA_CONNECTION_PROPERTY);
        } else {
            return "127.0.0.1:9092";
        }
    }
}
