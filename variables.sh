#!/bin/bash

export SGX_ENABLED=true

export SCALA_VERSION=2.11

export SPARK_VERSION=2.3.1
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082

export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_SIZE=102400000
export PREFETCH=128
export CONNECTIONS=4

export LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usrib:/usr/local/lib

export SGXLKL_STRACELKL=1
export SGXLKL_VERBOSE=1
export SGXLKL_TRACE_SYSCALL=0
export SGXLKL_TRACE_MMAP=0
export SGXLKL_HD=lkl/alpine-rootfs.img
export SGXLKL_KERNEL=0
export SGXLKL_VERSION=1
export SGXLKL_ESLEEP=512
export SGXLKL_SSLEEP=512
export SGXLKL_ESPINS=512
export SGXLKL_SSPINS=512
export SGXLKL_HOSTNAME=localhost
export SGXLKL_STHREADS=8
export SGXLKL_ETHREADS=4

export JVM_INITIAL_CODE_CACHE_SIZE=16m
export JVM_RESERVED_CODE_CACHE_SIZE=16m
export JVM_XMS=32m
export JVM_XMX=32m
export JVM_COMPRESSED_CLASS_SPACE_SIZE=16m
export JVM_MAX_METASPACE_SIZE=64m

