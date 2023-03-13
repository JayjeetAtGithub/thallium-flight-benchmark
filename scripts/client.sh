#!/bin/bash
set -e

binary=$1

export PROJECT_ROOT=$HOME/thallium-flight-benchmark

sync
echo 3 > /proc/sys/vm/drop_caches
sync

uri=$(ssh node1 "cat /tmp/thallium_uri")
$PROJECT_ROOT/bin/$binary $uri
