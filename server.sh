#!/bin/sh
set -e

selectivity=$1
protocol=$2

./bin/ts $selectivity dataset+mem $protocol
