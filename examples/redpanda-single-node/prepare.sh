#!/bin/bash

rm -rf data coredump
mkdir data coredump

sed -i'' "s@STORAGE_PATH@$(pwd)@" redpanda.yaml
