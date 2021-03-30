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

Compile bench benchmarks:

    mvn clean dependency:copy-dependencies package

Run all tests:

    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.TxSendBench
    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.StreamBench
    java -cp $(pwd)/target/performance-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/* io.vectorized.tests.TxSendOffsetsBench

== Redpanda ========================

warmed up 100 txes in     474 304 960 ns
measured 1000 txes in   4 161 368 711 ns
min:                        2 968 096 ns
max:                       10 342 328 ns
median:                     3 988 949 ns

== Kafka ===========================

warmed up 100 txes in   5 274 534 114 ns
measured 1000 txes in  37 561 500 218 ns
min:                       32 989 316 ns
max:                       71 690 062 ns
median:                    36 090 033 ns

=====================================================

== Redpanda ========================

measured 200 txes in    1 118 065 649 ns
min:                        4 316 003 ns
max:                       19 572 793 ns
median:                     5 026 282 ns

== Kafka ===========================

measured 200 txes in   30 574 019 313 ns
min:                       25 398 822 ns
max:                      170 911 605 ns
median:                   152 368 649 ns

=====================================================

measured 200 txes in      766 967 712 ns
min:                        3 029 808 ns
max:                        6 113 190 ns
median:                     3 570 479 ns

measured 200 txes in   24 292 613 748 ns
min:                       30 096 059 ns
max:                      132 180 312 ns
median:                   121 679 592 ns




insert 200000 records in a single tx in 1945968649ns
insert 200000 records in a single tx in 2726462497ns
insert 200000 records in a single tx in 7273449811ns

insert 200000 records in a single tx in 18670650098ns

insert 200000 records in a single tx in 17353401322ns
insert 200000 records in a single tx in   631768764ns

insert 200000 records in a single tx in  336680326ns

