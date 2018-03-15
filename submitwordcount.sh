#!/bin/bash

export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SGXLKL_SHMEM_SIZE=10240000
export SGX_ENABLED=true
export PREFETCH=8
export CONNECTIONS=1
export SPARK_LOCAL_IP=127.0.0.1

rm -rf $(pwd)/output

./bin/spark-submit \
--class org.apache.spark.examples.MyWordCount \
--master spark://kiwi01.doc.res.ic.ac.uk:7077 \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name wordcount \
--conf "spark.app.id=wordcount" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar README.md output
