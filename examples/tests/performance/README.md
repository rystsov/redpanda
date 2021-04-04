Prerequisites

  JDK (>=15.0.2) - https://www.oracle.com/java/technologies/javase-downloads.html
  Maven (>=3.6.3) - https://maven.apache.org/download.cgi

Build redpanda:

    task rp:build

Run a redpanda instance:

    cd examples/redpanda-single-node
    ./prepare.sh
    ./start.sh

Before running tests start a Redpanda instance and create two topics: topic1 and topic2:

    export KAFKA=<< PATH to KAFKA >>
    $KAFKA/bin/kafka-topics.sh --create --topic topic1 --partitions 1 --replication-factor 1 --bootstrap-server 127.0.0.1:9092
    $KAFKA/bin/kafka-topics.sh --create --topic topic2 --partitions 1 --replication-factor 1 --bootstrap-server 127.0.0.1:9092

Compile benchmarks:

    mvn clean dependency:copy-dependencies package

### How test Kafka?

See settings in `examples/kafka-single-node`. It's set to max consistency to avoid violations of transactions. See details in the ["Does Apache Kafka do ACID transactions?"](https://medium.com/@andrew_schofield/does-apache-kafka-do-acid-transactions-647b207f3d0e):

> Then you factor in the way that Kafka writes to its log asynchronously and you can see that what Kafka considers to be a committed transaction is not really atomic at all.
>
> Under normal operation, it will all work fine, but it doesn’t take much imagination to think of a failure that can break ACID. For example, if a partition is under-replicated and the leader suffers an unexpected power outage, a little data loss could occur and this breaks the integrity of the transaction. A correlated hard failure such as a power outage that affects all brokers could even result in commit/abort markers becoming lost in all replicas. You deploy Kafka in such a way as to minimise and hopefully eliminate these kinds of problems, but there’s still an element of asynchronous durability in the mix.

# Benchmarks

Testing Redpanda & Kafka on a single node (laptop, ThinkPad P53 Core i9 9th gen) and a 3-nodes cluster (AWS, i3.large).

## TxSendBench

Test executes a transaction inserting two events to two topics X times.

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.TxSendBench

### Single node

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 4110 ms | 2.9 ms | 3.9 ms | 16.5 ms|
| Kafka | 39027 ms | 32 ms | 39 ms | 79 ms |

### 3-nodes cluser

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 3813 ms | 3.1 ms | 3.7 ms | 11.7 ms|
| Kafka | 30073 ms | 5.7 ms | 25.5 ms | 68 ms |

## FetchBench

Test executes a transaction writing a single event to a topic and after commit reading it until it gets a written event X times.

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.FetchBench

### Single node

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 3754 ms | 2.3 ms | 3.3 ms | 18 ms|
| Kafka | 39761 ms | 16 ms | 41 ms | 129 ms |

### 3-nodes cluser

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 4273 ms | 3.1 ms | 3.8 ms | 29.8 ms|
| Kafka | 14828 ms | 6.5 ms | 27.5 ms | 56.4 ms |

## TxSendOffsetsBench

Test executes a transation executing a single `sendOffsetsToTransaction` X times

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.TxSendOffsetsBench

### Single node

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 942 ms | 3.2 ms | 4 ms | 16 ms|
| Kafka | 24292 ms | 22 ms | 121 ms | 132 ms |

### 3-nodes cluser

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 710 ms | 2.5 ms | 3.5 ms | 6.6 ms|
| Kafka | 21579 ms | 7.4 ms | 107 ms | 126 ms |


## StreamBench

Test executes a transation reading an event from an input stream, transforming it, writing to output stream and saving an offset to a consumer group X times.

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.StreamBench

### Single node

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 1238 ms | 4.4 ms | 5.7 ms | 20 ms|
| Kafka | 30574 ms | 23 ms | 150 ms | 163 ms |

### 3-nodes cluser

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 1131 ms | 4.5 ms | 5.4 ms | 10.5 ms|
| Kafka | 26524 ms | 14.7 ms | 132 ms | 154 ms |
