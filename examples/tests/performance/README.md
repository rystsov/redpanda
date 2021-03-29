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

### Single node (p50 x10)

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 4110 ms | 2.9 ms | 3.9 ms | 16.5 ms|
| Kafka | 39027 ms | 32 ms | 39 ms | 79 ms |

#### Kafka

measured 1000 txes in 22569726567ns
min: 2281642ns
p50: 23204628ns
p99: 50520111ns
max: 68093082ns

### 3-nodes cluser (p50 x7)

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 3813 ms | 3.1 ms | 3.7 ms | 11.7 ms|
| Kafka | 30073 ms | 5.7 ms | 25.5 ms | 68 ms |

#### Redpanda

measured 1000 txes in 4781965967ns
min: 3644523ns
p50: 4642989ns
p99: 6425918ns
max: 17329271ns

#### Kafka

measured 1000 txes in 31766233753ns
min: 5345414ns
p50: 26864777ns
p99: 54945233ns
max: 77773181ns

## FetchBench

Test executes a transaction writing a single event to a topic and after commit reading it until it gets a written event X times.

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.FetchBench

### Single node (p50 x12)

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 3754 ms | 2.3 ms | 3.3 ms | 18 ms|
| Kafka | 39761 ms | 16 ms | 41 ms | 129 ms |

#### Kafka

measured w/r 1000 in 6553678034ns
min: 3160438ns
p50: 4753218ns
p99: 26748518ns
max: 76239912ns

### 3-nodes cluser (p50 x7)

| System | 1000 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 4273 ms | 3.1 ms | 3.8 ms | 29.8 ms|
| Kafka | 14828 ms | 6.5 ms | 27.5 ms | 56.4 ms |

#### Redpanda

measured w/r 1000 in 5045237603ns
min: 3979908ns
p50: 4607801ns
p99: 9576830ns
max: 37635047ns

#### Kafka

measured w/r 1000 in 9859160250ns
min: 7012782ns
p50: 8584203ns
p99: 31234438ns
max: 63201004ns

## TxSendOffsetsBench

Test executes a transation executing a single `sendOffsetsToTransaction` X times

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.TxSendOffsetsBench

### Single node (p50 x30)

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 942 ms | 3.2 ms | 4 ms | 16 ms|
| Kafka | 24292 ms | 22 ms | 121 ms | 132 ms |

### 3-nodes cluser (p50 x30)

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 710 ms | 2.5 ms | 3.5 ms | 6.6 ms|
| Kafka | 21579 ms | 7.4 ms | 107 ms | 126 ms |

#### Redpanda

measured 1000 txes in 3082192299ns
min: 2401788ns
p50: 2999899ns
p99: 4782525ns
max: 7426585ns

#### Kafka

measured 1000 txes in 107690453149ns
min: 7059997ns
p50: 107468962ns
p99: 122729559ns
max: 144571720ns

## StreamBench

Test executes a transation reading an event from an input stream, transforming it, writing to output stream and saving an offset to a consumer group X times.

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.StreamBench

### Single node (p50 x26)

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 1238 ms | 4.4 ms | 5.7 ms | 20 ms|
| Kafka | 30574 ms | 23 ms | 150 ms | 163 ms |

### 3-nodes cluser (p50 x24)

| System | 200 txes | min | median | max |
| ------ | --------- | ---- | ------- | --- |
| Redpanda | 1131 ms | 4.5 ms | 5.4 ms | 10.5 ms|
| Kafka | 26524 ms | 14.7 ms | 132 ms | 154 ms |

measured 1000 txes in 6319669858ns
min: 5282590ns
p50: 5964986ns
p99: 13091369ns
max: 26247879ns

#### Kafka

measured 1000 txes in 142969763356ns
min: 12794179ns
p50: 136172718ns
p99: 163652966ns
max: 5257328934ns

measured 1000 txes in 139047703201ns
min: 13671326ns
p50: 133920155ns
p99: 155574756ns
max: 5240893982ns

measured 1000 txes in 133426484871ns
min: 13967893ns
p50: 133334773ns
p99: 153297602ns
max: 168375409ns

measured 1000 txes in 132844908023ns
min: 12213482ns
p50: 132874039ns
p99: 157374905ns
max: 164912294ns
