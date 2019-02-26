#!/bin/bash

source variables.sh

export SGX_ENABLED=false # no need for enclave driver
export IS_ENCLAVE=false
export IS_DRIVER=false # normal spark, no sgx
export IS_WORKER=false

export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SPARK_JOBNAME=utfpr

if [ $# -ne 1 ]; then
	echo "Usage: ./$(basename $0) <mode>"
	exit 1
fi

MODE=$1
INFILE=
OUTFILE=$(pwd)/output/

if [ $MODE -eq 0 ]; then
	INFILE=$(pwd)/phasor/e100/phasor50k.txt
	rm -rf $OUTFILE
	mkdir $OUTFILE
fi

./bin/spark-submit \
--class org.apache.spark.examples.utfpr.SmartMeteringSparkFileMode \
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
--conf "spark.default.parallelism=1" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar $MODE $OUTFILE $INFILE 2>&1 | tee outside-driver
