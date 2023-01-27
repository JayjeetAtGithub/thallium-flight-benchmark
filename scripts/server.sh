#!/bin/bash
set -e

selectivity=$1

export PROJECT_ROOT=$HOME/thallium-flight-benchmark
$PROJECT_ROOT/bin/ts $selectivity dataset+mem ofi+verbs
