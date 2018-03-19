#!/bin/bash

DATADIR=$(pwd)/FaultsLactecAPP

export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SGXLKL_SHMEM_SIZE=10240000
export SGX_ENABLED=true
export IS_DRIVER=true
export PREFETCH=128
export CONNECTIONS=1
export SPARK_LOCAL_IP=127.0.0.1

JARS=$(echo $(pwd)/examples/target/scala-2.11/jars/*jar )
JARS_COMMA=$(echo $JARS | tr ' ' ',')
JARS_COLON=$(echo $JARS | tr ' ' ':')

#--jars "$(echo $(pwd)/examples/target/scala-2.11/jars/*jar | tr ' ' ',')" \
./bin/spark-submit \
--class org.apache.spark.examples.lactec.Example2 \
--master spark://kiwi01.doc.res.ic.ac.uk:7077 \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name kmeans \
--jars "$JARS_COMMA" \
--conf "spark.app.id=kmeans" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.executor.extraClassPath=$JARS_COLON" \
--conf "spark.driver.extraClassPath=$JARS_COLON" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar $DATADIR/TestCustomer.csv $DATADIR/TestDSM.csv $DATADIR/TestFaults.csv 2017-01-01 2017-12-31  2>&1 | tee outside-driver
