#!/bin/bash
set -ex

for i in {1..3000}; do
    ./bin/bake_writer $(pwd)/yellow_tripdata_2022-01.parquet $(pwd)/yellow_tripdata_2022-01.parquet.${i}
done
