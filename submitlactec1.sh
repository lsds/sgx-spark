#!/bin/bash

export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SGXLKL_SHMEM_SIZE=10240000
export SGX_ENABLED=true
export PREFETCH=8
export CONNECTIONS=1
export SPARK_LOCAL_IP=127.0.0.1

JARS=$(echo $(pwd)/examples/target/scala-2.11/jars/*jar )
JARS_COMMA=$(echo $JARS | tr ' ' ',')
JARS_COLON=$(echo $JARS | tr ' ' ':')

#--jars "$(echo $(pwd)/examples/target/scala-2.11/jars/*jar | tr ' ' ',')" \
./bin/spark-submit \
--class org.apache.spark.examples.lactec.Example1 \
--master spark://kiwi01.doc.res.ic.ac.uk:7077 \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name kmeans \
--jars "$JARS_COMMA" \
--conf "spark.app.id=kmeans" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$(pwd)/sgx-spark-common/target/*:$(pwd)/sgx-spark-shm/target/*" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/sgx-spark-common/target/*:$(pwd)/sgx-spark-shm/target/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar (pwd)/TestLactec/TestCustomer2.csv 2>&1 | tee outside-driver
