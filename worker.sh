#!/bin/bash

source variables.sh

if [ "${SPARK_MASTER_HOST}" == "" ]; then
	echo "Define \${SPARK_MASTER_HOST}"
	exit 1
fi 

export IS_WORKER=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem

#--webui-port 8081 \
java \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\* \
-Xmx1g org.apache.spark.deploy.worker.Worker \
spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} 2>&1 | tee outside-worker

