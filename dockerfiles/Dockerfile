FROM            ubuntu:16.04
MAINTAINER      Florian Kelbert <f.kelbert@imperial.ac.uk>

ENV	SCALA_VERSION=2.11

ENV	PROTOBUF_VERSION=2.5.0

ENV     SPARK_DIR=/sgx-spark
ENV	SPARK_BRANCH=master

ENV	HADOOP_VERSION=2.6.5
ENV	HADOOP_DIR=${SPARK_DIR}/hadoop-${HADOOP_VERSION}-src

ENV     SGXLKL_DIR=/sgx-lkl
ENV	SGXLKL_BRANCH=cleanup-musl

RUN     apt-get update && \
        apt-get install -y --no-install-recommends \
                openvpn \
                scala \
                libtool \
                autoconf \
                curl \
                xutils-dev \
                git \
		build-essential \
		maven \
		openjdk-8-jdk \
		ssh \
		bc \
		python \
		autogen \
		wget \
		autotools-dev \
		sudo \
		automake \
        && apt-get clean \
        && apt-get autoclean \
        && rm -rf /var/lib/apt/lists/*

RUN	useradd -m user && echo "user:user" | chpasswd && adduser user sudo && \
	echo "user ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

ENV	HOME /home/user

USER	user

RUN	mkdir -p $HOME/.ssh
WORKDIR	$HOME/.ssh
COPY	id_rsa $HOME/.ssh/
RUN	sudo chown user:user id_rsa && \
	chmod 400 id_rsa && \
	ssh-keyscan lsds.doc.ic.ac.uk >> known_hosts

RUN     sudo mkdir -p ${SGXLKL_DIR} && \
	sudo chown user:user ${SGXLKL_DIR}
WORKDIR	${SGXLKL_DIR}
RUN     git clone --depth 1 lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-lkl.git ${SGXLKL_DIR} -b ${SGXLKL_BRANCH} && \
	make sim && \
	find . -name .git -exec rm -r {} + && \
	rm -rf ${SGXLKL_DIR}/apps

RUN	sudo mkdir -p ${SPARK_DIR} && \
	sudo chown user:user ${SPARK_DIR}
WORKDIR ${SPARK_DIR}
RUN	git clone --depth 1 lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-spark.git ${SPARK_DIR} -b ${SPARK_BRANCH} && \
	find . -name .git -exec rm -r {} +

WORKDIR	/tmp
RUN	wget https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz && \
	tar xvf protobuf-${PROTOBUF_VERSION}.tar.gz && \
	cd protobuf-${PROTOBUF_VERSION} && \
	./autogen.sh && \
	./configure && \
	make && \
	sudo make install && \
	cd .. && \
	rm -rf protobuf-${PROTOBUF_VERSION}*
RUN	sudo apt-get update && \
	sudo apt-get install -y --no-install-recommends \
		libprotoc-dev \
        && sudo apt-get clean \
        && sudo apt-get autoclean \
        && sudo rm -rf /var/lib/apt/lists/*

WORKDIR	${SPARK_DIR}
RUN    export MAVEN_OPTS="-Xmx1g"
RUN    build/mvn package -pl sgx -am -DskipTests
RUN    build/mvn package -pl shm -am -DskipTests
RUN    build/mvn package -pl hadoop-2.6.5-src -am -DskipTests
RUN    build/mvn package -pl common/sketch -am -DskipTests
RUN    build/mvn package -pl common/kvstore -am -DskipTests
RUN    build/mvn package -pl common/network-common -am -DskipTests
RUN    build/mvn package -pl common/network-shuffle -am -DskipTests
RUN    build/mvn package -pl common/unsafe -am -DskipTests
RUN    build/mvn package -pl common/tags -am -DskipTests
RUN    build/mvn package -pl core -am -DskipTests
RUN    build/mvn package -pl graphx -am -DskipTests
RUN    build/mvn package -pl mllib -am -DskipTests
RUN    build/mvn package -pl mllib-local -am -DskipTests
RUN    build/mvn package -pl tools -am -DskipTests
RUN    build/mvn package -pl streaming -am -DskipTests
RUN    build/mvn package -pl sql/catalyst -am -DskipTests
RUN    build/mvn package -pl sql/core -am -DskipTests
RUN    build/mvn package -pl sql/hive -am -DskipTests
RUN    build/mvn package -pl assembly -am -DskipTests
RUN    build/mvn package -pl examples -am -DskipTests
RUN    build/mvn package -pl repl -am -DskipTests
RUN    build/mvn package -pl launcher -am -DskipTests
RUN    build/mvn package -pl external/kafka-0-10 -am -DskipTests
RUN    build/mvn package -pl external/kafka-0-10-assembly -am -DskipTests
RUN    build/mvn package -pl external/kafka-0-10-sql -am -DskipTests
RUN    build/mvn package -DskipTests
RUN    cp ${HADOOP_DIR}/hadoop-common-project/hadoop-common/target/hadoop-common-${HADOOP_VERSION}.jar ${SPARK_DIR}/assembly/target/scala-${SCALA_VERSION}/jars/hadoop-common-${HADOOP_VERSION}.jar

WORKDIR ${SPARK_DIR}/C
RUN	make install

WORKDIR ${SPARK_DIR}/lkl
RUN	sed -i 's,^SGX_LKL=.*,SGX_LKL='${SGXLKL_DIR}',g' Makefile

WORKDIR ${SPARK_DIR}
COPY	worker-and-enclave.sh ${SPARK_DIR}/
COPY	driver-and-enclave.sh ${SPARK_DIR}/
COPY	log4j.properties ${SPARK_DIR}/conf/
RUN	sudo chown user:user ${SPARK_DIR}/worker-and-enclave.sh && \
	sudo chown user:user ${SPARK_DIR}/driver-and-enclave.sh && \
	sudo chown user:user ${SPARK_DIR}/conf/log4j.properties && \
	chmod +x ${SPARK_DIR}/worker-and-enclave.sh && \
	chmod +x ${SPARK_DIR}/driver-and-enclave.sh

EXPOSE 7077
EXPOSE 8082
