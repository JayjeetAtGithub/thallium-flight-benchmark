#!/bin/bash
set -e

selectivity=$1
protocol=$2

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
ln -s $PROJECT_ROOT/bake_config.json bake_config.json
ln -s $PROJECT_ROOT/yokan_config.json yokan_config.json
$PROJECT_ROOT/bin/ts $selectivity dataset+mem $protocol
