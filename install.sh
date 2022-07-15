#!/bin/sh
set -ex

# installing some dependencies
apt update
apt install uuid-dev libjson-c-dev libpmemobj-cpp-dev

# create the working dir
rm -rf $HOME/mochi-tools
mkdir $HOME/mochi-tools
cd $HOME/mochi-tools

# build libfabric
echo "building libfabric"
git clone https://github.com/ofiwg/libfabric
cd libfabric
./autogen.sh
./configure --prefix=$HOME/mochi/install --enable-tcp=yes --enable-udp=yes --enable-sockets=yes --enable-verbs=yes --enable-rxm=yes
make -j 32
make install
cd ../

# build mercury
echo "building mercury"
git clone https://github.com/mercury-hpc/mercury
cd mercury
git submodule update --init
mkdir build
cd build
cmake -DMERCURY_USE_SELF_FORWARD:BOOL=ON \
 -DBUILD_TESTING:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON \
 -DCMAKE_INSTALL_PREFIX=$HOME/mochi/install \
 -DBUILD_SHARED_LIBS:BOOL=ON -DCMAKE_BUILD_TYPE:STRING=Debug ..
make
make install
cd ../..

# build argobots
echo "building argobots"
git clone https://github.com/pmodels/argobots
cd argobots
./autogen.sh
./configure --prefix=$HOME/mochi/install
make 
make install
cd ../

# build margo 
echo "building margo"
git clone https://github.com/mochi-hpc/mochi-margo
cd mochi-margo
./prepare.sh
mkdir -p build
cd build
../configure --prefix=$HOME/mochi/install \
    PKG_CONFIG_PATH=$HOME/mochi/install/lib/pkgconfig
make
make install
cd ../..

# build abt-io 
echo "building abt-io"
git clone https://github.com/mochi-hpc/mochi-abt-io
cd mochi-abt-io
./prepare.sh
mkdir -p build
cd build
../configure --prefix=$HOME/mochi/install \
    PKG_CONFIG_PATH=$HOME/mochi/install/lib/pkgconfig
make
make install
cd ../..

# build bake 
echo "building bake"
git clone https://github.com/mochi-hpc/mochi-bake
cd mochi-bake
./prepare.sh
mkdir -p build
cd build
../configure --prefix=$HOME/mochi/install \
    PKG_CONFIG_PATH=$HOME/mochi/install/lib/pkgconfig
make
make install
cd ../..