#!/bin/bash
set -e

selectivity=$1

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
# rm -rf *.json
# cp $PROJECT_ROOT/bake_config.json bake_config.json
# cp $PROJECT_ROOT/yokan_config.json yokan_config.json
$PROJECT_ROOT/bin/ts $selectivity dataset+mem ofi+verbs
