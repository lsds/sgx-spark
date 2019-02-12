#!/bin/bash

source variables.sh

export SPARK_JOBNAME=lactec

export IS_ENCLAVE=false
export IS_DRIVER=true
export IS_WORKER=false

export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver

#DATADIR=$(pwd)/FaultsLactecAPP3
DATADIR=hdfs://localhost:9000/FaultsLactecAPP


JARS=$(echo $(pwd)/examples/target/scala-2.11/jars/*jar )
JARS_COMMA=$(echo $JARS | tr ' ' ',')
JARS_COLON=$(echo $JARS | tr ' ' ':')

#--jars "$(echo $(pwd)/examples/target/scala-2.11/jars/*jar | tr ' ' ',')" \
#--conf "spark.executor.extraClassPath=$JARS_COLON" \
#--conf "spark.driver.extraClassPath=$JARS_COLON" \
./bin/spark-submit \
--class org.apache.spark.examples.lactec.Example2 \
--master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
--deploy-mode client \
--verbose \
--driver-memory 2g \
--executor-memory 1g \
--name $SPARK_JOBNAME \
--jars "$JARS_COMMA" \
--conf "spark.default.parallelism=1" \
--conf "spark.app.id=$SPARK_JOBNAME" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$(pwd)/sgx/target/*:$(pwd)/shm/target/*" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/sgx/target/*:$(pwd)/shm/target/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}-SGX.jar $DATADIR/TestCustomer.csv $DATADIR/TestDSM.csv $DATADIR/TestFaults.csv 2016-01-01 2016-12-31  2>&1 | tee outside-driver

