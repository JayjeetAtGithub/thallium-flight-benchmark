#!/bin/bash
set -e

uri=$1
protocol=$2

for i in {1..5};
do

ssh node1 "sync"
ssh node1 "echo 3 > /proc/sys/vm/drop_caches"
ssh node1 "sync"

./bin/tc $uri dataset $protocol

done
