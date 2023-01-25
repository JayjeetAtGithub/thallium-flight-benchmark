# Thallium vs Arrow Flight Benchmark

## Configuring CloudLab APT r320/c6220 Nodes

**NOTE:** All the steps need to be repeated on both the client and server nodes.

1. Install dependencies.

```bash
# install ibverbs
apt update
apt install -y ibverbs-utils

# clone the repository
git clone https://github.com/JayjeetAtGithub/thallium-flight-benchmark ~/thallium-flight-benchmark

# install arrow
~/thallium-flight-benchmark/scripts/deploy_arrow.sh

# install spack
git clone -b releases/v0.18 -c feature.manyFiles=true https://github.com/spack/spack.git ~/spack
. ~/spack/share/spack/setup-env.sh

# setup mochi namespace
git clone https://github.com/mochi-hpc/mochi-spack-packages.git ~/mochi-spack-packages
spack repo add ~/mochi-spack-packages

# install mochi-thallium
spack install libfabric fabrics=tcp,udp,sockets,verbs,rxm
spack install --reuse mercury +ucx ^ucx+verbs
spack install --reuse mochi-margo@main
spack install --reuse mochi-thallium
spack install --reuse mochi-bake
spack install --reuse mochi-yokan ^rocksdb
spack load mochi-thallium
spack load mochi-bake
spack load mochi-yokan
```

2. Load RDMA kernel modules.

```bash
modprobe ib_uverbs
modprobe ib_ipoib
```

3. Check if the IB interface is available.

```bash
ifconfig -a
```

You should find a `ib0`/`ibp8s0`/`ibp130s0` interface.

4. Assign an IP address to the IB network interface.
```bash
# on client
ifconfig ibp8s0 10.0.1.50

# on server
ifconfig ibp8s0 10.0.2.50
```

## Compiling

On both client and server nodes, 

```bash
cmake .
make
```

The binaries will be generated in the `bin` directory.

## Deploying dataset

On the server node,
```bash
./scripts/deploy_data.sh
```

## References

* https://docs.oracle.com/cd/E19436-01/820-3522-10/ch4-linux.html

* https://www.rdmamojo.com/2015/01/24/verify-rdma-working/
