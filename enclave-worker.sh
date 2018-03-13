#!/bin/bash

export LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib

export SGX_ENABLED=true
export SGXLKL_STRACELKL=1
export SGXLKL_VERBOSE=1
export SGXLKL_TRACE_SYSCALL=0
export SGXLKL_TRACE_MMAP=0
export SGXLKL_TAP=tap3
export SGXLKL_HD=lkl/alpine-rootfs.img
export SGXLKL_KERNEL=0
export SGXLKL_VERSION=1
export SGXLKL_ESLEEP=16
export SGXLKL_SSLEEP=16
export SGXLKL_ESPINS=16
export SGXLKL_SSPINS=16
export SGXLKL_HOSTNAME=localhost
export SGXLKL_STHREADS=6
export SGXLKL_ETHREADS=3
export IS_ENCLAVE=true
export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=/sgx-lkl-shmem
export SGXLKL_SHMEM_SIZE=10240000
export CONNECTIONS=1
export PREFETCH=8

../sgx-lkl-sim/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java \
-XX:InitialCodeCacheSize=8m \
-XX:ReservedCodeCacheSize=8m \
-Xms16m \
-Xmx16m \
-XX:CompressedClassSpaceSize=8m \
-XX:MaxMetaspaceSize=32m \
-XX:+UseCompressedClassPointers \
-XX:+UseMembar \
-XX:+AssumeMP \
-Xint \
-Djava.library.path=/spark/lib/ \
-Dcom.sun.management.jmxremote \
-Djava.rmi.server.hostname=10.0.1.1 \
-Dcom.sun.management.jmxremote.port=5000 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-worker
