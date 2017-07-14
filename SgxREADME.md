# Build and prepare

## Delete existing Maven Hadoop files

`# rm -rf ~/.m2/repository/org/apache/hadoop/`

## Build modified Hadoop

You need to have Google Protocol Buffers 2.5 installed (see https://stackoverflow.com/a/29799354/2273470).
Note that these instructions are for Arch Linux. For Ubuntu 16.04, you'll need to remove the existing
protoc installed by default (version 2.6.1) and when running configure for 2.5.0, don't
specify the prefix flag.

`sgx-spark/hadoop-2.6.5-src# mvn package -DskipTests`

## Build Spark

`sgx-spark# build/mvn -DskipTests package`

## Make Spark use the modified Hadoop

`sgx-spark# cp hadoop-2.6.5-src/hadoop-common-project/hadoop-common/target/hadoop-common-2.6.5.jar assembly/target/scala-2.11/jars/hadoop-common-2.6.5.jar`

# Run 

## Master

`sgx-spark# SPARK_LOCAL_IP=127.0.0.1 /usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7077 --webui-port 8080`

## Worker

`sgx-spark# SPARK_LOCAL_IP=127.0.0.1 /usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://localhost:7077`

## Enclave side

This is the only code that runs inside the enclave.

`sgx-spark# /usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\*:examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain`

## Spark Job

`sgx-spark# SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.MyWordCount --master spark://localhost:7077 --deploy-mode cluster --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar <infile> <outdir>`

## To Run the Example on SGX-LKL 

First, follow all of the instructions above to make and build hadoop and spark. Then cd into the `spark-sgx/lkl`
directory and edit the `Makefile` to point to your `sgx-lkl` directory (this needs to have already been cloned and compiled
seperately).

Run `make` in `spark-sgx/lkl`. This will build the disk image and attempt to execute the worker, master, enclave code and spark
job/driver processes.

Comment the code out in the Makefile if you wish to run the enclave code inside sgx-lkl, or on the host machine (without lkl)
