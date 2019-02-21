#!/bin/bash

source variables.sh

export IS_ENCLAVE=true
export DEBUG_IS_ENCLAVE_REAL=false
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem

#-Xverify:none might make it faster but break things (removes bytecode verification)
#-XX:+PreserveFramePointer is useful for debugging only

# -Dcom.sun.management.jmxremote \
# -Djava.rmi.server.hostname=${SGXLKL_IP4} \
# -Dcom.sun.management.jmxremote.port=5000 \
# -Dcom.sun.management.jmxremote.authenticate=false \
# -Dcom.sun.management.jmxremote.ssl=false \
#-Xint \
#-Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n \
#../sgx-lkl-sim/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java \
#echo gdb --args \
java \
-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
-Xms${JVM_XMS} \
-Xmx${JVM_XMX} \
-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
-XX:+UseCompressedClassPointers \
-XX:+PreserveFramePointer \
-XX:+UseMembar \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
-Djava.library.path=lib/ \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\*:examples/target/scala-${SCALA_VERSION}/jars/\*:sgx-spark-common/target/\*:sgx-spark-shm/target/\* \
org.apache.spark.sgx.SgxMain 2>&1 | tee enclave-worker
