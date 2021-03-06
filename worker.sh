#!/bin/bash

source variables.sh

export IS_ENCLAVE=false
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem

if [ "${SPARK_MASTER_HOST}" == "" ]; then
	echo "Define \${SPARK_MASTER_HOST}"
	exit 1
fi 

# to debug
#-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \

java \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\*:sgx-spark-common/target/\*:sgx-spark-shm/target/\* \
org.apache.spark.deploy.worker.Worker \
spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} 2>&1 | tee outside-worker

