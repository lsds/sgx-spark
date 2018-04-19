#!/bin/bash

source variables.sh

export SGXLKL_TAP=tap6

export IS_ENCLAVE=true
export IS_DRIVER=true
export IS_WORKER=false

export DEBUG_IS_ENCLAVE_REAL=false

export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver

# -Dcom.sun.management.jmxremote \
# -Djava.rmi.server.hostname=${SGXLKL_IP4} \
# -Dcom.sun.management.jmxremote.port=5000 \
# -Dcom.sun.management.jmxremote.authenticate=false \
# -Dcom.sun.management.jmxremote.ssl=false \
#../sgx-lkl/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java \
java \
-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
-Xms${JVM_XMS} \
-Xmx${JVM_XMX} \
-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
-XX:+UseCompressedClassPointers \
-XX:+UseMembar \
-XX:+AssumeMP \
-Xint \
-Djava.library.path=lib/ \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\*:examples/target/scala-${SCALA_VERSION}/jars/\* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-driver
