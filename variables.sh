#!/bin/bash

export SGX_ENABLED=false
export USE_HDFS_ENCRYPTION=false

export SCALA_VERSION=2.11

export SPARK_VERSION=2.3.2
export SPARK_MASTER_HOST=$(hostname -f)
export SPARK_MASTER_PORT=7077
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_DEFAULT_BUFFER_SIZE=$((32*1024*1024))

export SGXLKL_EXECUTABLE=../sgx-lkl/build/sgx-lkl-run
export SGXLKL_IMAGE=lkl/alpine-rootfs.img

export SGXLKL_SHMEM_SIZE=2147483647 
export PREFETCH=2048
export CONNECTIONS=8
export SERIALIZER=default
export BACKOFF_WAIT_MIN=1
export BACKOFF_WAIT_MAX=64

export HADOOP_HOME=/tmp

export LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usrib:/usr/local/lib

export SGXLKL_VERBOSE=1
export SGXLKL_TRACE_SYSCALL=0
export SGXLKL_TRACE_MMAP=0
export SGXLKL_ESLEEP=512
export SGXLKL_SSLEEP=512
export SGXLKL_ESPINS=512
export SGXLKL_SSPINS=512
export SGXLKL_HOSTNAME=localhost
export SGXLKL_STHREADS=16
export SGXLKL_ETHREADS=4
export SGXLKL_HEAP=805306368

export JVM_INITIAL_CODE_CACHE_SIZE=32m
export JVM_RESERVED_CODE_CACHE_SIZE=32m
export JVM_XMS=512m
export JVM_XMX=512m
export JVM_COMPRESSED_CLASS_SPACE_SIZE=32m
export JVM_MAX_METASPACE_SIZE=128m

