#!/bin/bash

source ./variables.sh

echo assembly/target/scala-${SCALA_VERSION}/jars/

java \
-cp conf/:assembly/target/scala-${SCALA_VERSION}/jars/\* \
-Xmx1g \
org.apache.spark.deploy.master.Master \
--host $(hostname) \
--port ${SPARK_MASTER_PORT} \
--webui-port ${SPARK_MASTER_WEBUI_PORT}
