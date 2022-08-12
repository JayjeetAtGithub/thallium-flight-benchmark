#!/bin/bash
set -ex

for i in {1..400}; do
    ./bin/bake_writer $(pwd)/16MB.uncompressed.parquet $(pwd)/16MB.uncompressed.parquet.${i}
done
