#!/bin/bash
set -ex

apt update
apt install -y python3 \
               python3-pip \
               python3-venv \
               python3-numpy \
               cmake \
               libboost-all-dev \
               llvm

if [ ! -d "arrow" ]; then
    git clone https://github.com/apache/arrow /tmp/arrow
fi

cd /tmp/arrow
git submodule update --init --recursive
git pull

mkdir -p cpp/release
cd cpp/release

cmake -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_DATASET=ON \
  -DARROW_CSV=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_FLIGHT=ON \
  ..

make -j$(nproc) install

cp -r /usr/local/lib/* /usr/lib/
