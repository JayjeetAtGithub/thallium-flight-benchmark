#!/bin/bash
set -ex

git clone -c feature.manyFiles=true https://github.com/spack/spack.git ~/spack
cd ~/spack
git checkout releases/v0.18
. share/spack/setup-env.sh

spack install libfabric fabrics=tcp,udp,sockets,rxm,verbs
spack install --reuse mochi-thallium
spack load mochi-thallium
