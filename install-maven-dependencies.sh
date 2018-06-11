#!/bin/bash

mvn install:install-file \
    -Dfile=hadoop-2.6.5-src/hadoop-common-project/hadoop-common/target/hadoop-common-2.6.5.jar \
    -DgroupId=org.apache.hadoop \
    -DartifactId=hadoop-common \
    -Dversion=2.6.5 \
    -Dpackaging=jar

echo "Done."
