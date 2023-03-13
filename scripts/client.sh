#!/bin/bash
set -e

binary=$1

export PROJECT_ROOT=$HOME/thallium-flight-benchmark

for i in {1..5};
do

ssh node1 "sync"
ssh node1 "echo 3 > /proc/sys/vm/drop_caches"
ssh node1 "sync"

uri=$(ssh node1 "cat /tmp/thallium_uri")
$PROJECT_ROOT/bin/$binary $uri

done
