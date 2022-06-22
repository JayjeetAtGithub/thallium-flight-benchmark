## Setting up RDMA

https://docs.oracle.com/cd/E19436-01/820-3522-10/ch4-linux.html
https://www.rdmamojo.com/2015/01/24/verify-rdma-working/


```
apt update
apt install -y ibverbs-utils libboost-all-dev

modprobe ib_uverbs

modprobe ib_ipoib

ifconfig ib0

ifconfig ib0 10.0.1.50
ifconfig ib0 10.0.2.50
```

```
./bin/ts 
./src/tc "ofi+verbs;ofi_rxm://[host]:[port]"
```


```
./bin/fs 10.10.1.2 4001
./bin/fc 10.10.1.2 4001 /mnt/cephfs/dataset
```