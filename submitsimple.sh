#!/bin/bash

export SGX_ENABLED=true
export SPARK_VERSION=2.3.2-SGX
export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SGXLKL_SHMEM_SIZE=10240000
export PREFETCH=8
export CONNECTIONS=1
export SPARK_LOCAL_IP=127.0.0.1 
export SPARK_JOBNAME=kmeans

rm -rf $(pwd)/output

./bin/spark-submit \
--class org.apache.spark.examples.Simple \
--master spark://kiwi01.doc.res.ic.ac.uk:7077 \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name $SPARK_JOBNAME \
--conf "spark.app.id=$SPARK_JOBNAME" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/sgx/target/*:$(pwd)/shm/target/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar $(pwd)/data/mllib/kmeans_data.txt 2>&1 | tee outside-driver
