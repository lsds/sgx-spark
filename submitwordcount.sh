#!/bin/bash

source variables.sh

export SGX_ENABLED=false

export IS_ENCLAVE=false
export SGX_ENABLED=false
export SPARK_JOBNAME=wordcount
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver

rm -rf $(pwd)/output

INFILE=$(pwd)/README.md
#INFILE=$(pwd)/README.md.16
#INFILE=$(pwd)/README.md.256
#INFILE=$(pwd)/README.md.4096
#INFILE=$(pwd)/README.md.8192
#INFILE=$(pwd)/README.md.16384
#INFILE=$(pwd)/README.md.65536
#INFILE=$(pwd)/README.md.131072
#INFILE=$(pwd)/README.md.262144
#INFILE=$(pwd)/README.md.524288
#INFILE=$(pwd)/README.md.1048576
#INFILE=$(pwd)/README.md.2097152

./bin/spark-submit \
--class org.apache.spark.examples.MyWordCount \
--master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
--deploy-mode client \
--driver-memory 2g \
--executor-memory 2g \
--verbose \
--name $SPARK_JOBNAME \
--conf "spark.app.id=$SPARK_JOBNAME" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$(pwd)/sgx/target/*:$(pwd)/shm/target/*" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/sgx/target/*:$(pwd)/shm/target/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar $INFILE output 2>&1 | tee outside-driver
