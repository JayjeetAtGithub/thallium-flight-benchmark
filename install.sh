#!/bin/bash
set -ex

apt update
apt install -y ibverbs-utils

git clone https://github.com/JayjeetAtGithub/thallium-flight-benchmark ~/thallium-flight-benchmark
~/thallium-flight-benchmark/scripts/deploy_arrow.sh

git clone -b releases/v0.18 -c feature.manyFiles=true https://github.com/spack/spack.git ~/spack
. ~/spack/share/spack/setup-env.sh

git clone https://github.com/mochi-hpc/mochi-spack-packages.git ~/mochi-spack-packages
spack repo add ~/mochi-spack-packages

spack install libfabric fabrics=tcp,udp,sockets,verbs,rxm
spack install --reuse mercury +ucx ^ucx+verbs
spack install --reuse mochi-margo
spack install --reuse mochi-thallium
spack install --reuse mochi-bake
spack install --reuse mochi-yokan ^rocksdb
