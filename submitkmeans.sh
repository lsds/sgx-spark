#!/bin/bash

source variables.sh

export SGX_ENABLED=false

export IS_ENCLAVE=false

export SGX_ENABLED=false
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SPARK_JOBNAME=kmeans

rm -rf $(pwd)/output

INFILE=$(pwd)/data/mllib/kmeans_data.txt
#INFILE=$(pwd)/data/mllib/kmeans_data.txt.short

./bin/spark-submit \
--class org.apache.spark.examples.mllib.KMeansExample \
--master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
--deploy-mode client \
--driver-memory 2g \
--executor-memory 2g \
--verbose \
--name $SPARK_JOBNAME \
--conf "spark.app.id=$SPARK_JOBNAME" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$(pwd)/sgx-spark-common/target/*:$(pwd)/sgx-spark-shm/target/*" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/sgx-spark-common/target/*:$(pwd)/sgx-spark-shm/target/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar ${INFILE} output 2>&1 | tee outside-driver
