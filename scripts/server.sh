#!/bin/bash
set -e

selectivity=$1
binary=$2

sync
echo 3 > /proc/sys/vm/drop_caches
sync

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
$PROJECT_ROOT/bin/$binary $selectivity dataset+mem
