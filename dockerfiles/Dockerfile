FROM            ubuntu:16.04
MAINTAINER      Florian Kelbert <f.kelbert@imperial.ac.uk>

ENV     GIT_USER=fkelbert

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
        && apt-get clean \
        && apt-get autoclean \
        && rm -rf /var/lib/apt/lists/*

RUN	mkdir -p /root/.ssh
COPY	id_rsa /root/.ssh/
RUN	chmod 400 /root/.ssh/id_rsa && \
	ssh-keyscan lsds.doc.ic.ac.uk >> /root/.ssh/known_hosts

RUN     mkdir -p ${SGXLKL_DIR}
WORKDIR	${SGXLKL_DIR}
RUN     git clone lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-lkl.git ${SGXLKL_DIR}
RUN	apt-get update && apt-get install -y bc python
RUN	make sim

RUN	mkdir -p ${SPARK_DIR}
WORKDIR ${SPARK_DIR}
RUN	git clone lsds_git@lsds.doc.ic.ac.uk:sereca/sgx-spark.git ${SPARK_DIR}

RUN     apt-get update && apt-get install -y wget autogen autotools-dev automake

RUN	wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz && \
	tar xvf protobuf-2.5.0.tar.gz && \
	cd protobuf-2.5.0 && \
	./autogen.sh && \
	./configure && \
	make && \
	make install && \
	rm -rf protobuf-2.5.0*

RUN     apt-get update && apt-get install -y libprotoc-dev
RUN	protoc --version

WORKDIR	${SPARK_DIR}/hadoop-2.6.5-src
RUN	rm -rf ~/.m2/repository/org/apache/hadoop/
RUN	mvn package -DskipTests


