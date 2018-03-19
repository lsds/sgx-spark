#!/bin/bash

export SGX_ENABLED=true
export IS_WORKER=true
export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem
export SGXLKL_SHMEM_SIZE=10240000
export PREFETCH=128
export CONNECTIONS=1

java \
-cp conf/:assembly/target/scala-2.11/jars/\* \
-Xmx1g org.apache.spark.deploy.worker.Worker \
--webui-port 8081 \
spark://kiwi01.doc.res.ic.ac.uk:7077 2>&1 | tee outside-worker

