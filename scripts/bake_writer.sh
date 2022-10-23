#!/bin/bash
set -ex

wget https://skyhook-ucsc.s3.us-west-1.amazonaws.com/16MB.uncompressed.parquet
for i in {1..200}; do
    ./bin/bake_writer $(pwd)/16MB.uncompressed.parquet /mnt/cephfs/dataset/16MB.uncompressed.parquet.${i}
done
