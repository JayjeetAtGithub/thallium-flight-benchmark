#!/bin/bash
set -e

selectivity=$1
protocol=$2

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
$PROJECT_ROOT/bin/ts $selectivity dataset+mem $protocol
