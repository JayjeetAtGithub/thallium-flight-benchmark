#!/bin/bash
set -e

selectivity=$1
binary=$2

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
$PROJECT_ROOT/bin/$binary $selectivity dataset+mem
