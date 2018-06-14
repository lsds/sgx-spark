#!/bin/bash

source variables.sh

export SGXLKL_TAP=tap6

export IS_ENCLAVE=true
export IS_DRIVER=true
export IS_WORKER=false

export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver

# -Dcom.sun.management.jmxremote \
# -Djava.rmi.server.hostname=${SGXLKL_IP4} \
# -Dcom.sun.management.jmxremote.port=5000 \
# -Dcom.sun.management.jmxremote.authenticate=false \
# -Dcom.sun.management.jmxremote.ssl=false \
# -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n \
#echo gdb --args \
${SGXLKL_EXECUTABLE} ${SGXLKL_IMAGE} /opt/j2re-image/bin/java \
-Xms${JVM_XMS} \
-Xmx${JVM_XMX} \
-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
-XX:+UseCompressedClassPointers \
-XX:+PreserveFramePointer \
-XX:+UseMembar \
-XX:+AssumeMP \
-Djava.library.path=/spark/lib/ \
-cp \
/home/scala-library/:/spark/conf/:/spark/assembly/target/scala-${SCALA_VERSION}/jars/\*:/spark/examples/target/scala-${SCALA_VERSION}/jars/*:/spark/sgx/target/\*:/spark/shm/target/\* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-driver
