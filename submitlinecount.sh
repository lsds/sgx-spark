#!/bin/bash

source variables.sh

export IS_ENCLAVE=false
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SPARK_JOBNAME=linecount

#INFILE=$(pwd)/data/mllib/kmeans_data.txt.single
#INFILE=$(pwd)/data/mllib/kmeans_data.txt.short
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
--class org.apache.spark.examples.LineCount \
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
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar ${INFILE} 2>&1 | tee outside-driver
