#!/bin/bash
set -ex

for i in {1..200}; do
    ./bin/bake_writer $(pwd)/16MB.uncompressed.parquet $(pwd)/16MB.uncompressed.parquet.${i}
done
