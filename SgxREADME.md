# Prerequisites

As Sgx-Spark uses [sgx-lkl](https://lsds.doc.ic.ac.uk/gitlab/sereca/sgx-lkl), the
latter must have been downloaded and compiled successfully. In addition, please
ensure that sgx-lkl executes simple Java applications successfully.

# Build Sgx-Spark

- Remove existing Hadoop Maven files to ensure that we will be using our customised Hadoop library:

    `# rm -rf ~/.m2/repository/org/apache/hadoop/`

- Build a modified version of Hadoop. This version has been modified to make simple datatypes serialisable:

    You need to have Google Protocol Buffers 2.5 installed (see https://stackoverflow.com/a/29799354/2273470). Note that these instructions are for Arch Linux. For Ubuntu 16.04, you'll need to remove the existing protoc installed by default (version 2.6.1) and when running configure for 2.5.0, don't specify the prefix flag.

    `sgx-spark/hadoop-2.6.5-src# mvn package -DskipTests`

- Build Spark:

    `sgx-spark# build/mvn -DskipTests package`

- Make Spark use the modified Hadoop by copying the corresponding Hadoop JAR file into the Sgx-Spark jars directory:

    `sgx-spark# cp hadoop-2.6.5-src/hadoop-common-project/hadoop-common/target/hadoop-common-2.6.5.jar assembly/target/scala-2.11/jars/hadoop-common-2.6.5.jar`
    
- Build the native library that implements shared memory primitives:
 
    `sgx-spark/C# make install`

# Prepare the Sgx-Spark Alpine Linux images that will be run on top of SGX-LKL

- Edit file `spark-sgx/lkl/Makefile`: Variable `SGX_LKL` must point to your `sgx-lkl` directory (see [Prerequisistes](#prerequisiutes)). 
- Make the Alpine disk image:

    `#sgx-spark/lkl# make clean && make alpine-rootfs.img`

    As root, prepare two tap device for networking:

    ```
    $ openvpn --mktun --dev tap0
    $ ip link set dev tap0 up
    $ ip addr add 10.0.1.254/24 dev tap0
    $ openvpn --mktun --dev tap1
    $ ip link set dev tap1 up
    $ ip addr add 10.0.1.254/24 dev tap1
    ```

# Run Sgx-Spark using SGX-LKL

1. Run the Master node

    `sgx-spark# SPARK_LOCAL_IP=127.0.0.1 java -cp conf/:assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.master.Master --host kiwi01.doc.res.ic.ac.uk --port 7077 --webui-port 8080`

2. Run the Worker node

    `sgx-spark# SGX_USE_SHMEM=true SGXLKL_SHMEM_FILE=sgx-lkl-shmem SGXLKL_SHMEM_SIZE=10240000 SGX_ENABLED=true PREFETCH=8 CONNECTIONS=1 java -cp conf/:assembly/target/scala-2.11/jars/\* -Djava.library.path=$(pwd)/lib -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://kiwi01.doc.res.ic.ac.uk:7077`

3. Run the enclave for the Worker node

    `sgx-spark# LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib SGXLKL_STRACELKL=1 SGXLKL_VERBOSE=1 SGXLKL_TRACE_SYSCALL=0 SGXLKL_TRACE_MMAP=0 SGXLKL_TAP=tap2 SGXLKL_HD=lkl/alpine-rootfs.img SGXLKL_KERNEL=0 SGXLKL_VERSION=1 SGXLKL_ESLEEP=16 SGXLKL_SSLEEP=16 SGXLKL_ESPINS=16 SGXLKL_SSPINS=16 SGXLKL_HOSTNAME=localhost SGXLKL_STHREADS=6 SGXLKL_ETHREADS=3 IS_ENCLAVE=true SGX_USE_SHMEM=true SGXLKL_SHMEM_FILE=/sgx-lkl-shmem SGX_ENABLED=true SGXLKL_SHMEM_SIZE=10240000 CONNECTIONS=1 PREFETCH=8 ../sgx-lkl/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java -XX:InitialCodeCacheSize=8m -XX:ReservedCodeCacheSize=8m -Xms16m -Xmx16m -XX:CompressedClassSpaceSize=8m -XX:MaxMetaspaceSize=32m -XX:+UseCompressedClassPointers -XX:+AssumeMP -Xint -Djava.library.path=/spark/lib/ -cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar org.apache.spark.sgx.SgxMain`

4. Run the enclave for the driver program:

    `sgx-spark# LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib SGXLKL_STRACELKL=1 SGXLKL_VERBOSE=1 SGXLKL_TRACE_SYSCALL=0 SGXLKL_TRACE_MMAP=0 SGXLKL_TAP=tap1 SGXLKL_HD=lkl/alpine-rootfs.img SGXLKL_KERNEL=0 SGXLKL_VERSION=1 SGXLKL_ESLEEP=16 SGXLKL_SSLEEP=16 SGXLKL_ESPINS=16 SGXLKL_SSPINS=16 SGXLKL_HOSTNAME=localhost SGXLKL_STHREADS=6 SGXLKL_ETHREADS=3 IS_ENCLAVE=true SGX_USE_SHMEM=true SGXLKL_SHMEM_FILE=/sgx-lkl-shmem-driver SGX_ENABLED=true SGXLKL_SHMEM_SIZE=10240000 CONNECTIONS=1 PREFETCH=8 ../sgx-lkl/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java -XX:InitialCodeCacheSize=8m -XX:ReservedCodeCacheSize=8m -Xms16m -Xmx16m -XX:CompressedClassSpaceSize=8m -XX:MaxMetaspaceSize=32m -XX:+UseCompressedClassPointers -XX:+AssumeMP -Xint -Djava.library.path=/spark/lib/ -cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar org.apache.spark.sgx.SgxMain`

5. Finally, submit a Spark job:

    `sgx-spark# rm -rf $(pwd)/output; SGX_USE_SHMEM=true SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver SGXLKL_SHMEM_SIZE=10240000 SGX_ENABLED=true PREFETCH=8 CONNECTIONS=1 SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.mllib.KMeansExample --master spark://kiwi01.doc.res.ic.ac.uk:7077 --deploy-mode client --verbose --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" --conf "spark.executor.extraLibraryPath=$(pwd)/lib" --conf "spark.driver.extraLibraryPath=$(pwd)/lib" --conf "spark.driver.extraJavaOptions=-cp $(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/* -Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar $(pwd)/data/mllib/kmeans_data.txt`

# Native execution of the same Spark installation

- Start the Master node as above (step 1 above)

- Start the Worker node as above (step 2 above), but change variable `SGX_ENABLED=true` to `SGX_ENABLED=false`

- Skip steps 3 and 4 above

- Submit the Spark job as above (step 5 above), but change variable `SGX_ENABLED=true` to `SGX_ENABLED=false`


