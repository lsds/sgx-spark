ALPINE_MAJOR=3.6
ALPINE_VERSION=3.6.1
ALPINE_ARCH=x86_64

ALPINE_TAR=alpine-minirootfs.tar.gz
MOUNTPOINT=/media/ext4disk

IMAGE_SIZE_MB=500
JARS=$(shell find ../ -type f -name "*.jar" | tr ' ' ':')

.DELETE_ON_ERROR:

all: alpine-rootfs.img test

$(ALPINE_TAR):
	curl -L -o "$@" "https://nl.alpinelinux.org/alpine/v$(ALPINE_MAJOR)/releases/$(ALPINE_ARCH)/alpine-minirootfs-$(ALPINE_VERSION)-$(ALPINE_ARCH).tar.gz"

alpine-rootfs.img: $(ALPINE_TAR) buildenv.sh
	dd if=/dev/zero of="$@" count=$(IMAGE_SIZE_MB) bs=1M
	mkfs.ext4 "$@"
	sudo mkdir -p $(MOUNTPOINT)
	sudo mount -t ext4 -o loop "$@" $(MOUNTPOINT)

	sudo tar -C $(MOUNTPOINT) -xvf $(ALPINE_TAR)
	sudo cp /etc/resolv.conf $(MOUNTPOINT)/etc/resolv.conf
	sudo install buildenv.sh $(MOUNTPOINT)/usr/sbin

	sudo mkdir -p $(MOUNTPOINT)/opt
	sudo tar -xf jre8-no-segv.tar.gz -C $(MOUNTPOINT)/opt

	sudo chroot $(MOUNTPOINT) /bin/sh /usr/sbin/buildenv.sh

	sudo umount $(MOUNTPOINT)
	sudo chown $(USER) "$@"

test:
# Run Master
	SPARK_LOCAL_IP=127.0.0.1 /usr/bin/java -cp ../assembly/target/scala-2.11/jars/\*:../conf/:../hadoop-2.6.5-src/hadoop-common-project/hadoop-kms/target/kms/WEB-INF/lib/*  -Xmx1g org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7077 --webui-port 8080 > master.txt 2>&1 &
	 sleep 2

# Run Worker
	SPARK_LOCAL_IP=127.0.0.1 /usr/bin/java -cp ../assembly/target/scala-2.11/jars/\*:../conf/:../hadoop-2.6.5-src/hadoop-common-project/hadoop-kms/target/kms/WEB-INF/lib/* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://localhost:7077 > worker.txt 2>&1 &
	 sleep 2

# Run Enclave (this needs to be updated to invoke sgx-lkl and setup the correct network paths between the host and the sgx jvm) 
#	 @-$(shell openvpn --mktun --dev tap0)
#	 $(shell ip link set dev tap0 up)
#	 $(shell ip addr add 10.0.1.254/24 dev tap0)
	/usr/bin/java -cp ../conf/:../assembly/target/scala-2.11/jars/\*:../examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain > enclave.txt 2>&1 &
	 sleep 2

# Spark Job
	SPARK_LOCAL_IP=127.0.0.1 ../bin/spark-submit --class org.apache.spark.examples.MyWordCount --master spark://localhost:7077 --deploy-mode cluster --verbose --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" --driver-library-path $(JARS) --driver-class-path $(JARS) ../examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar > job.txt 2>&1 &

# Kill Process and clean up networking
#   @-$(shell openvpn --rmtun --dev tap0)
#   @-$(shell pkill -9 sgx-lkl-starter)

clean:
	rm -rf alpine*
	rm -rf *.txt
	pkill -9 java
