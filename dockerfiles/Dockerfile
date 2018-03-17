FROM            ubuntu:16.04
MAINTAINER      Florian Kelbert <f.kelbert@imperial.ac.uk>

ENV     SPARK_DIR=/sgx-spark

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
RUN	git clone --depth 1 lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-spark.git ${SPARK_DIR} && \
	find . -name .git -exec rm -r {} +

WORKDIR	/tmp
RUN	wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz && \
	tar xvf protobuf-2.5.0.tar.gz && \
	cd protobuf-2.5.0 && \
	./autogen.sh && \
	./configure && \
	make && \
	make install && \
	cd .. && \
	rm -rf protobuf-2.5.0*
RUN	apt-get update && \
	apt-get install -y --no-install-recommends \
		libprotoc-dev \
        && apt-get clean \
        && apt-get autoclean \
        && rm -rf /var/lib/apt/lists/*

WORKDIR	${SPARK_DIR}/hadoop-2.6.5-src
RUN	export MAVEN_OPTS="-Xmx1g" && \
	mvn package -DskipTests

WORKDIR	${SPARK_DIR}
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/sketch
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/kvstore
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/network-common
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/network-shuffle
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/unsafe
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl common/tags
RUN	MAVEN_OPTS="-Xmx2g" build/mvn package -DskipTests -pl core
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl graphx
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl mllib
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl mllib-local
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl tools
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl streaming
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl sql/catalyst
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl sql/core
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl sql/hive
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl assembly
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl examples
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl repl
RUN	MAVEN_OPTS="-Xmx1024m" build/mvn package -DskipTests -pl launcher
RUN	MAVEN_OPTS="-Xmx1g" build/mvn -DskipTests package && \
	cp hadoop-2.6.5-src/hadoop-common-project/hadoop-common/target/hadoop-common-2.6.5.jar assembly/target/scala-2.11/jars/hadoop-common-2.6.5.jar && \
	rm -rf ${SPARK_DIR}/hadoop-2.6.5-src


WORKDIR ${SPARK_DIR}/C
RUN	make install

WORKDIR ${SPARK_DIR}/lkl
RUN	cat Makefile

RUN	openvpn --mktun --dev tap0 && \
	openvpn --mktun --dev tap1

