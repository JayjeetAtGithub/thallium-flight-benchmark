#!/bin/bash
set -ex

modprobe ib_uverbs
modprobe ib_ipoib
ifconfig -a
