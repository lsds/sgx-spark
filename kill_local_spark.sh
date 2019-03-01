#!/bin/bash

jps -l | grep spark | awk '{print $1}' | xargs kill -9 $p
pkill -9 sgx-lkl
rm -f /dev/shm/sgx-lkl*
