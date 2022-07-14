# Thallium vs Arrow Flight Benchmark

## Configuring CloudLab APT r320 Nodes

**NOTE:** All the steps need to be repeated on both the client and server nodes.

1. Install dependencies.

```bash
# install ibverbs
apt update
apt install -y ibverbs-utils

# install arrow
./deploy_arrow.sh

# install mochi-thallium
spack install libfabric fabrics=tcp,udp,sockets,verbs,rxm
spack install --reuse mochi-thallium
spack load mochi-thallium
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

You should find a `ib0` or `ibp8s0` interface.

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

## Running Benchmarks

### Thallium
```bash
# on server
./bin/ts 

# on client
./bin/tc [port]"
```

### Flight
```bash
# on server
./bin/fs [port]

# on client
./bin/fc [port]
```

## References

* https://docs.oracle.com/cd/E19436-01/820-3522-10/ch4-linux.html

* https://www.rdmamojo.com/2015/01/24/verify-rdma-working/
