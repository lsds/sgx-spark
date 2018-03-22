# Sgx-Spark

This is [Apache Spark](https://github.com/apache/spark) with modifications to run
security sensitive code inside Intel SGX enclaves. The implementation leverages
[sgx-lkl](https://github.com/lsds/sgx-lkl), a library OS that allows to run
Java-based applications inside SGX enclaves.

## Docker quick start

This guide shows how to run Sgx-Spark in a few simple steps using Docker. 
Most parts of the setup and deployment are wrapped within Docker containers.
Compliation and deployment should thus be smooth.

### Preparing the Sgx-Spark Docker environment

- Clone this Sgx-Spark repository

- Build the Sgx-Spark base image. The name of the resulting Docker image is `sgxpsark`. This process might take a while (30-60 mins):
    
    ```
    sgx-spark/dockerfiles$ docker build -t sgxspark .
    ```

- Prepare the disk image that will be required by sgx-lkl. Due to restrictions of Docker, this step can currently not be implemented as part of the above Docker build process. Thus, this step is platform-dependent. The process has been successfully tested on Ubuntu 16.04 and Arch Linux:

    ```
    sgx-spark/lkl$ make prepare-image
    ```

- Create a Docker network device that will be used for communication by the Docker containers:

    ```
    sgx-spark$ docker network create sgxsparknet
    ```

### Running Sgx-Spark jobs using Docker


From within directory `sgx-spark/dockerfiles`, run the Sgx-Spark master node,
the Sgx-Spark worker node, as well as the actual Sgx-Spark job as follows.

- Run the Sgx-Spark master node:

    ```
    sgx-spark/dockerfiles$ docker run \
    --user user \
    --env-file $(pwd)/docker-env \
    --net sgxsparknet \
    --name sgxspark-docker-master \
    -p 7077:7077 \
    -p 8082:8082 \
    -ti sgxspark /sgx-spark/master.sh
    ```

- Run the Sgx-Spark worker node:

    ```
    sgx-spark/dockerfiles$ docker run \
    --user user \
    --env-file $(pwd)/docker-env \
    --net sgxsparknet \
    --privileged \
    -v $(pwd)/../lkl:/spark-image:ro \
    -ti sgxspark /sgx-spark/worker-and-enclave.sh
    ```

- Run the Sgx-Spark job as follows.

    As of writing, the three jobs below are known to be fully supported:

    - WordCount
    
        ```
        sgx-spark/dockerfiles$ docker run \
        --user user \
        --env-file $(pwd)/docker-env \
        --net sgxsparknet \
        --privileged \
        -v $(pwd)/../lkl:/spark-image:ro \
        -e SPARK_JOB_CLASS=org.apache.spark.examples.MyWordCount \
        -e SPARK_JOB_NAME=WordCount \
        -e SPARK_JOB_ARG0=README.md \
        -e SPARK_JOB_ARG1=output \
        -ti sgxspark /sgx-spark/driver-and-enclave.sh
        ```
        
    - KMeans

        ```
        sgx-spark/dockerfiles$ docker run \
        --user user \
        --env-file $(pwd)/docker-env \
        --net sgxsparknet \
        --privileged \
        -v $(pwd)/../lkl:/spark-image:ro \
        -e SPARK_JOB_CLASS=org.apache.spark.examples.mllib.KMeansExample \
        -e SPARK_JOB_NAME=KMeans \
        -e SPARK_JOB_ARG0=data/mllib/kmeans_data.txt \
        -ti sgxspark /sgx-spark/driver-and-enclave.sh
        ```
        
    - Smart grid fault detection
    
        ```
        sgx-spark/dockerfiles$ docker run \
        --user user \
        --env-file $(pwd)/docker-env \
        --net sgxsparknet \
        --privileged \
        -v $(pwd)/../lkl:/spark-image:ro \
        -e SPARK_JOB_CLASS=org.apache.spark.examples.lactec.Example2 \
        -e SPARK_JOB_NAME=lactec2 \
        -e SPARK_JOB_ARG0=FaultsLactecAPP/TestCustomer.csv \
        -e SPARK_JOB_ARG1=FaultsLactecAPP/TestDSM.csv \
        -e SPARK_JOB_ARG2=FaultsLactecAPP/TestFaults.csv \
        -e SPARK_JOB_ARG3=2016-01-01 \
        -e SPARK_JOB_ARG4=2016-01-31 \
        -ti sgxspark /sgx-spark/driver-and-enclave.sh
        ```   

## Conventional compilation, installation and deployment

Of course, Sgx-Spark can be run natively, without Docker. The
compilation, installation and deployment process is detailed in the following.

### Prerequisites

As Sgx-Spark uses [sgx-lkl](https://github.com/lsds/sgx-lkl), the
latter must have been downloaded and compiled successfully. For this, please
follow the documentation of sgx-lkl. In particular, ensure that your
installation of sgx-lkl executes simple Java applications successfully.

### Build Sgx-Spark

- Remove existing Hadoop Maven files to ensure that we will be using the customised Hadoop library that ships with Sgx-Spark:

    ```
    $ rm -rf ~/.m2/repository/org/apache/hadoop/
    ```

- Build the customised Hadoop library that ships with Sgx-Spark. This version of Hadoop has been modified to make simple datatypes serialisable:

    For this step, Google Protocol Buffers (GPB) v2.5 is required.
    
    - Make sure to uninstall any other versions of GPB
    
    - Install GPB v2.5
    
        Instructions for Ubuntu 16.04 are as follows:

            $ cd /tmp
            /tmp$ wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
        	/tmp$ tar xvf protobuf-2.5.0.tar.gz
        	/tmp$ cd protobuf-2.5.0
        	/tmp/protobuf-2.5.0$ ./autogen.sh
        	/tmp/protobuf-2.5.0$ ./configure
        	/tmp/protobuf-2.5.0$ make
        	/tmp/protobuf-2.5.0$ sudo make install

    	An installation guide for Arch Linux is available at https://stackoverflow.com/a/29799354/2273470.

    - Compile the customised Hadoop library:
    
        ```
        sgx-spark/hadoop-2.6.5-src$ mvn package -DskipTests
        ```

- Build Sgx-Spark:

    ```
    sgx-spark$ build/mvn -DskipTests package
    ```

- Ensure that Sgx-Spark uses the customised Hadoop library. For this, copy the Hadoop JAR file into the Sgx-Spark jars directory:

    ```
    sgx-spark$ cp hadoop-2.6.5-src/hadoop-common-project/hadoop-common/target/hadoop-common-2.6.5.jar \
    assembly/target/scala-2.11/jars/hadoop-common-2.6.5.jar
    ```
    
- Sgx-Spark ships with a native C library (`libringbuff.so`) that enables shared-memory-based communication between two JVMs. Compile as follows:
 
    ```
    sgx-spark/C$ make install
    ```

### Prepare the Sgx-Spark disk images that will be run using sgx-lkl

- Adjust file `spark-sgx/lkl/Makefile` for your environment:

    Variable `SGX_LKL` must point to your `sgx-lkl` directory (see [Prerequisites](#prerequisites)). 
 
- Build the Sgx-Spark disk image required for sgx-lkl:

    ```
    sgx-spark/lkl$ make clean all
    ```

- Prepare two tap devices that are required by sgx-lkl:

    ```
    $ openvpn --mktun --dev tap0
    $ openvpn --mktun --dev tap1
    ```

### Run Sgx-Spark using sgx-lkl

Finally, we are ready to run (i) the Sgx-Spark master node,
(ii) the Sgx-Spark worker node, (iii) the worker's enclave, (iv) the Sgx-Spark client, 
and (v) the client's enclave. In the following commands, replace: `<hostname>` with
the master node's actual hostname; `<sgx-lkl>` with the path to your `sgx-lkl` installation.

- Run the Master node

    ```
    sgx-spark$
    SPARK_LOCAL_IP=127.0.0.1 \
    java \
    -cp conf/:assembly/target/scala-2.11/jars/\* \
    -Xmx1g \
    org.apache.spark.deploy.master.Master \
    --host <hostname> \
    --port 7077 \
    --webui-port 8082
    ```

- Run the Worker node

    ```
    sgx-spark$
    SGX_ENABLED=true \
    IS_WORKER=true \
    SGXLKL_SHMEM_FILE=sgx-lkl-shmem \
    java \
    -cp conf/:assembly/target/scala-2.11/jars/\* \
    -Djava.library.path=$(pwd)/lib \
    -Xmx1g \
    org.apache.spark.deploy.worker.Worker \
    --webui-port 8081 \
    spark://<hostname>:7077
    ```

- Run the enclave for the Worker node

    ```sgx-spark$
    LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib \
    SGX_ENABLED=true \
    IS_WORKER=true \
    IS_ENCLAVE=true \    
    SGXLKL_STRACELKL=1 \
    SGXLKL_VERBOSE=1 \
    SGXLKL_TRACE_SYSCALL=0 \
    SGXLKL_TRACE_MMAP=0 \
    SGXLKL_TAP=tap0 \
    SGXLKL_HD=lkl/alpine-rootfs.img \
    SGXLKL_KERNEL=0 \
    SGXLKL_VERSION=1 \
    SGXLKL_HOSTNAME=localhost \
    SGXLKL_STHREADS=6 \
    SGXLKL_ETHREADS=3 \
    SGXLKL_SHMEM_FILE=/sgx-lkl-shmem \
    <sgx-lkl>/sgx-musl-lkl/obj/sgx-lkl-starter \
    /opt/j2re-image/bin/java \
    -Xms16m \
    -Xmx16m \
    -Xint \
    -XX:+UseMembar \
    -XX:+AssumeMP \
    -XX:MaxMetaspaceSize=32m \
    -XX:InitialCodeCacheSize=8m \
    -XX:ReservedCodeCacheSize=8m \
    -XX:CompressedClassSpaceSize=8m \
    -XX:+UseCompressedClassPointers \
    -Djava.library.path=/spark/lib/ \
    -cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar \
    org.apache.spark.sgx.SgxMain
    ```

- Run the enclave for the driver program:

    ```
    sgx-spark$
    LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib \
    SGX_ENABLED=true \
    IS_DRIVER=true \
    IS_ENCLAVE=true \    
    SGXLKL_STRACELKL=1 \
    SGXLKL_VERBOSE=1 \
    SGXLKL_TRACE_SYSCALL=0 \
    SGXLKL_TRACE_MMAP=0 \
    SGXLKL_TAP=tap1 \
    SGXLKL_HD=lkl/alpine-rootfs.img \
    SGXLKL_KERNEL=0 \
    SGXLKL_VERSION=1 \
    SGXLKL_HOSTNAME=localhost \
    SGXLKL_STHREADS=6 \
    SGXLKL_ETHREADS=3 \
    SGXLKL_SHMEM_FILE=/sgx-lkl-shmem-driver \
    <sgx-lkl>/sgx-musl-lkl/obj/sgx-lkl-starter \
    /opt/j2re-image/bin/java \
    -Xms16m \
    -Xmx16m \
    -Xint \
    -XX:+UseMembar \
    -XX:+AssumeMP \
    -XX:MaxMetaspaceSize=32m \
    -XX:InitialCodeCacheSize=8m \
    -XX:ReservedCodeCacheSize=8m \
    -XX:CompressedClassSpaceSize=8m \
    -XX:+UseCompressedClassPointers \
    -Djava.library.path=/spark/lib/ \
    -cp /home/scala-library/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar \
    org.apache.spark.sgx.SgxMain
    ```

- Finally, submit a Spark job.

    For example, WordCount or KMeans:
    
    - WordCount
    
        ```
        sgx-spark$
        rm -rf $(pwd)/output; \
        SPARK_LOCAL_IP=127.0.0.1 \
        SGX_ENABLED=true \
        IS_DRIVER=true \
        SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver \
        ./bin/spark-submit \
        --class org.apache.spark.examples.mllib.KMeansExample \
        --master spark://<hostname>:7077 \
        --deploy-mode client \
        --verbose \
        --executor-memory 1g \
        --name wordcount \
        --conf "spark.app.id=wordcount" \
        --conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
        --conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
        --conf "spark.executor.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
        --conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
        examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar \
        $(pwd)/README.md \
        output
        ```
    
    
    - KMeans

        ```
        sgx-spark$
        SPARK_LOCAL_IP=127.0.0.1 \
        SGX_ENABLED=true \
        IS_DRIVER=true \
        SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver \
        ./bin/spark-submit \
        --class org.apache.spark.examples.mllib.KMeansExample \
        --master spark://<hostname>:7077 \
        --deploy-mode client \
        --verbose \
        --executor-memory 1g \
        --name kmeans \
        --conf "spark.app.id=kmeans" \
        --conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
        --conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
        --conf "spark.executor.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
        --conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-2.11/jars/*:$(pwd)/examples/target/scala-2.11/jars/*" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
        examples/target/scala-2.11/jars/spark-examples_2.11-2.3.1-SNAPSHOT.jar \
        $(pwd)/data/mllib/kmeans_data.txt
        ```

### Native execution of the same Spark installation

In order to run the above installation without SGX, start your environment as follows:

- Start the Master node as above

- Start the Worker node as above, but change variable `SGX_ENABLED=true` to `SGX_ENABLED=false`

- Do not start the enclaves

- Submit the Spark job as above, but change variable `SGX_ENABLED=true` to `SGX_ENABLED=false`


