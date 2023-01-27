#!/bin/bash
set -e

uri=$1

export PROJECT_ROOT=$HOME/thallium-flight-benchmark

for i in {1..5};
do

ssh node1 "sync"
ssh node1 "echo 3 > /proc/sys/vm/drop_caches"
ssh node1 "sync"

$PROJECT_ROOT/bin/tc $uri dataset ofi+verbs

done
