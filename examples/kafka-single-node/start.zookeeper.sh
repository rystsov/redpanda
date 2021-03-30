#!/bin/bash

export ZOOKEEPER=/home/rystsov/ojava/txtest/apache-zookeeper-3.6.2-bin
$ZOOKEEPER/bin/zkServer.sh --config . start-foreground
