#!/bin/bash

sudo cp /spark-image/alpine-rootfs.img ${SPARK_DIR}/lkl
cd ${SPARK_DIR}/lkl
sudo make finalize-image
cd ${SPARK_DIR}
sudo chown user:user ${SPARK_DIR}/lkl/alpine-rootfs.img

export IS_DRIVER=true
export IS_WORKER=false

(
export IS_ENCLAVE=true

${SGXLKL_DIR}/build/sgx-lkl-run lkl/alpine-rootfs.img /opt/j2re-image/bin/java \
-XX:InitialCodeCacheSize=32m \
-XX:ReservedCodeCacheSize=32m \
-Xms512m \
-Xmx512m \
-XX:CompressedClassSpaceSize=32m \
-XX:MaxMetaspaceSize=128m \
-XX:+UseCompressedClassPointers \
-XX:+UseMembar \
-XX:+AssumeMP \
-Djava.library.path=/spark/lib/ \
-cp \
/home/scala-library/:/spark/conf/:/spark/assembly/target/scala-${SCALA_VERSION}/jars/\*:/spark/examples/target/scala-${SCALA_VERSION}/jars/*:/spark/shm/target/\*:/spark/sgx/target/\* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-driver
) &

sleep 2

(
export IS_ENCLAVE=false

JARS=$(echo $(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*jar $(pwd)/sgx/target/*jar $(pwd)/shm/target/*jar)
JARS_COMMA=$(echo $JARS | tr ' ' ',')
JARS_COLON=$(echo $JARS | tr ' ' ':')

./bin/spark-submit \
--class ${SPARK_JOB_CLASS} \
--master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name ${SPARK_JOB_NAME} \
--jars "$JARS_COMMA" \
--conf "spark.app.id=${SPARK_JOB_NAME}" \
--conf "spark.executor.extraLibraryPath=${SPARK_DIR}/lib" \
--conf "spark.driver.extraLibraryPath=${SPARK_DIR}/lib" \
--conf "spark.executor.extraClassPath=$JARS_COLON" \
--conf "spark.driver.extraClassPath=$JARS_COLON" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}-SNAPSHOT.jar \
${SPARK_JOB_ARG0} ${SPARK_JOB_ARG1} ${SPARK_JOB_ARG2} ${SPARK_JOB_ARG3} ${SPARK_JOB_ARG4} ${SPARK_JOB_ARG5} ${SPARK_JOB_ARG6} ${SPARK_JOB_ARG7} ${SPARK_JOB_ARG8} ${SPARK_JOB_ARG9} 2>&1 | tee outside-driver

)

