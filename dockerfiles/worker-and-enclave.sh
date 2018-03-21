#!/bin/bash

sudo cp /spark-image/alpine-rootfs.img ${SPARK_DIR}/lkl
cd ${SPARK_DIR}/lkl
sudo make finalize-image
cd ${SPARK_DIR}
sudo chown user:user ${SPARK_DIR}/lkl/alpine-rootfs.img

sudo openvpn --mktun --dev tap0

export IS_WORKER=true
export IS_DRIVER=false

(
export IS_ENCLAVE=true

${SGXLKL_DIR}/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java \
-XX:InitialCodeCacheSize=32m \
-XX:ReservedCodeCacheSize=32m \
-Xms512m \
-Xmx512m \
-XX:CompressedClassSpaceSize=32m \
-XX:MaxMetaspaceSize=128m \
-XX:+UseCompressedClassPointers \
-XX:+UseMembar \
-XX:+AssumeMP \
-Xint \
-Djava.library.path=/spark/lib/ \
-cp \
/home/scala-library/:/spark/conf/:/spark/assembly/target/scala-${SCALA_VERSION}/jars/\*:/spark/examples/target/scala-${SCALA_VERSION}/jars/* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-worker
) &

sleep 2

(
export IS_ENCLAVE=false

java \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\* \
-Xmx1g org.apache.spark.deploy.worker.Worker \
spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} 2>&1 | tee outside-worker
)

