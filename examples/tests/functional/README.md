Prerequisites

  JDK (>=15.0.2) - https://www.oracle.com/java/technologies/javase-downloads.html
  Maven (>=3.6.3) - https://maven.apache.org/download.cgi

Build redpanda:

    task rp:build

Run a redpanda instance:

    cd examples/single-node
    ./prepare.sh
    ./start.sh

Before running tests start a Redpanda instance and create two topics: topic1 and topic2:

    export KAFKA=<< PATH to KAFKA >>
    $KAFKA/bin/kafka-topics.sh --create --topic topic1 --partitions 1 --replication-factor 1 --bootstrap-server 127.0.0.1:9092
    $KAFKA/bin/kafka-topics.sh --create --topic topic2 --partitions 1 --replication-factor 1 --bootstrap-server 127.0.0.1:9092

Run all tests:

    cd examples/tests/functional
    mvn clean test

Run individual tests:

    mvn -Dtest=SimpleProducerTest#sendPasses test
    mvn -Dtest=TxProducerTest#initPasses test
