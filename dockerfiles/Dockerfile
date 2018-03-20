FROM            ubuntu:16.04
MAINTAINER      Florian Kelbert <f.kelbert@imperial.ac.uk>

ENV	SCALA_VERSION=2.11

ENV	PROTOBUF_VERSION=2.5.0

ENV     SPARK_DIR=/sgx-spark
ENV	SPARK_BRANCH=dev-fk

ENV	HADOOP_VERSION=2.6.5
ENV	HADOOP_DIR=${SPARK_DIR}/hadoop-${HADOOP_VERSION}-src

ENV     SGXLKL_DIR=/sgx-lkl

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

RUN	mkdir -p /root/.ssh
COPY	id_rsa /root/.ssh/
RUN	chmod 400 /root/.ssh/id_rsa && \
	ssh-keyscan lsds.doc.ic.ac.uk >> /root/.ssh/known_hosts

RUN     mkdir -p ${SGXLKL_DIR}
WORKDIR	${SGXLKL_DIR}
RUN     git clone --depth 1 lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-lkl.git ${SGXLKL_DIR} && \
	make sim && \
	find . -name .git -exec rm -r {} + && \
	rm -rf ${SGXLKL_DIR}/apps

RUN	mkdir -p ${SPARK_DIR}
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
	make install && \
	cd .. && \
	rm -rf protobuf-${PROTOBUF_VERSION}*
RUN	apt-get update && \
	apt-get install -y --no-install-recommends \
		libprotoc-dev \
        && apt-get clean \
        && apt-get autoclean \
        && rm -rf /var/lib/apt/lists/*

WORKDIR	${HADOOP_DIR}
RUN	MAVEN_OPTS="-Xmx1g" mvn package -DskipTests

WORKDIR	${SPARK_DIR}
RUN	MAVEN_OPTS="-Xmx1g" build/mvn -DskipTests package && \
	cp ${HADOOP_DIR}/hadoop-common-project/hadoop-common/target/hadoop-common-${HADOOP_VERSION}.jar ${SPARK_DIR}/assembly/target/scala-${SCALA_VERSION}/jars/hadoop-common-${HADOOP_VERSION}.jar && \
	rm -rf ${HADOOP_DIR}

WORKDIR ${SPARK_DIR}/C
RUN	make install

WORKDIR ${SPARK_DIR}/lkl
RUN	sed -i 's,^SGX_LKL=.*,SGX_LKL='${SGXLKL_DIR}',g' Makefile

WORKDIR ${SPARK_DIR}
RUN	cat ./master.sh

