#!/bin/bash

rm -rf data.kafka data.zookeeper
mkdir data.kafka data.zookeeper
echo 1 > data.zookeeper/myid

sed -i'' "s@STORAGE_PATH@$(pwd)@" kafka.properties
sed -i'' "s@STORAGE_PATH@$(pwd)@" zoo.cfg
