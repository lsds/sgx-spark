# Build Spark

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

# Run Spark on SGX-LKL

## Master node

`sgx-spark# SPARK_LOCAL_IP=127.0.0.1 java -cp conf/:assembly/target/scala-2.11/jars/\*:examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar -Xmx1g org.apache.spark.deploy.master.Master --host kiwi01.doc.res.ic.ac.uk --port 7077 --webui-port 8080`

## Worker node

`sgx-spark# SGX_ENABLED=true SPARK_SGX_ENCLAVE_IP=10.0.1.1 SPARK_SGX_ENCLAVE_PORT=9999 SPARK_SGX_HOST_IP=146.179.131.194 SPARK_LOCAL_IP=127.0.0.1 java -cp conf/:assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://kiwi01.doc.res.ic.ac.uk:7077`

## Enclave

- cd into the `spark-sgx/lkl` directory and edit the `Makefile` to point to your `sgx-lkl` directory (this needs to have already been cloned and compiled
seperately). 
- Make the Alpine disk image: `make clean && make alpine-rootfs.img`

As root, prepare the tap device for networking:
```
$ openvpn --mktun --dev tap0
$ ip link set dev tap0 up
$ ip addr add 10.0.1.254/24 dev tap0
```

Run the enclave side of Spark-SGX as follows:

`sgx-spark# LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib SGXLKL_STRACELKL=1 SGXLKL_VERBOSE=1 SGXLKL_TRACE_SYSCALL=0 SGXLKL_TRACE_MMAP=0 SGXLKL_TAP=tap0 SGXLKL_HD=lkl/alpine-rootfs.img SGXLKL_KERNEL=0 SGXLKL_VERSION=1 SGXLKL_ESLEEP=1 SGXLKL_SSLEEP=4000 SGXLKL_ESPINS=50000 SGXLKL_SSPINS=500 SGXLKL_STHREADS=8 SGXLKL_ETHREADS=4 SGXLKL_STACK_SIZE=256000 SGXLKL_SHMEM_FILE=/sgx-lkl-shmem SGXLKL_SHMEM_SIZE=1k PREFETCH=8 SPARK_SGX_ENCLAVE_IP=10.0.1.1 SPARK_SGX_ENCLAVE_PORT=9999 ../sgx-lkl-sim/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java -XX:InitialCodeCacheSize=8m -XX:ReservedCodeCacheSize=8m -Xms16m -Xmx16m -XX:CompressedClassSpaceSize=8m -XX:MaxMetaspaceSize=32m -XX:+UseCompressedClassPointers -XX:+AssumeMP -Xint -Djava.library.path=/spark/lib/ -cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain`

## Spark Job

Finally, submit a Spark job:

`sgx-spark# rm -rf $(pwd)/output; SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.MyWordCount --master spark://kiwi01.doc.res.ic.ac.uk:7077 --deploy-mode cluster --verbose --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar $(pwd)/README.md $(pwd)/output`

# Running the same Spark installation natively

Do you see environment variable `SGX_ENABLED=true` when starting the worker node above? Just set it `SGX_ENABLED=false`. Spark will execute natively. In this case you don't need to start the enclave side of the code.

# Current state of the implementation (2017/09/27)

The two JVM instanecs communicate via sockets using a handful of connections. The idea is to use shared memory instead. Some work/code in this direction can be found in the following files/directories:

- When starting the enclave side of Spark-SGX, parameter `SGXLKL_SHMEM_FILE` defines which shared memory file to use (`SGXLKL_SHMEM_SIZE` defines the amount of shared memory). The code in `sgx-musl/tools/starter-exec.c` (around line 630) maps three files into memory: one file for some ring buffer data structures (see below), one file to communicate from the enclave to the outside, and one file to communicate from the outside to the enclave.

- `sgx-musl/src/sm-mq/`: a ring buffer implementation that we were planning to use to manage the shared memory (contact Manuel Nieke (TU Braunschweig)) for any questions

- `sgx-lkl/apps/basic/mq-test` an example app that uses the above ring buffer (inside shared memory) to communicate (contact Manuel Nieke (TU Braunschweig)) for any questions

- `sgx-spark/C`: Java/JNI bindings for the above ring buffer. Compile as follows: `sgx-spark# gcc -I./C/ -shared -fpic -o lib/libringbuff.so C/*.c -lrt`

- `sgx-spark/core/src/main/java/org/apache/spark/sgx/RingBuff.java`: A java class using the C ring buffer on the basis of shared memory. Method `shmem()` in `object SgxMain` in `sgx-spark/src/main/scala/org/apache/spark/sgx/SgxMain.scala` uses the class and shows that it works (uncomment the call to `shmem()` in `main()`). 

- `sgx-spark/core/src/main/java/org/apache/spark/sgx/SgxSettings.scala` defines some settings for shared memory and default values.

- To use the ring buffer via JNI, you need to start `java` with parameter `-Djava.library.path=`, indicating where the library (`libringbuff.so`) can be found



