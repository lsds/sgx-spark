# CHANGE THIS TO POINT TO YOUR SGX-LKL DIRECTORY
SGX_LKL=/data/jdl11/lkl/sgx-lkl

ALPINE_MAJOR=3.6
ALPINE_VERSION=3.6.1
ALPINE_ARCH=x86_64

ALPINE_TAR=alpine-minirootfs.tar.gz
MOUNTPOINT=/media/ext4disk
SPARK_DIR=/spark

IMAGE_SIZE_MB=500
CLASSPATH_JARS=conf/:assembly/target/scala-2.11/jars/\*

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

# Copy enclave jars and files into alpine disk image
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/jars
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)/examples/target/scala-2.11/jars
	sudo cp -r ../conf $(MOUNTPOINT)/$(SPARK_DIR)/
	sudo cp -r ../assembly/target/scala-2.11/jars $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/
	sudo cp ../examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar $(MOUNTPOINT)/$(SPARK_DIR)/examples/target/scala-2.11/jars/

	sudo cp /etc/resolv.conf $(MOUNTPOINT)/etc/resolv.conf
	sudo mkdir -p $(MOUNTPOINT)/opt
	sudo tar -xf jre8-no-segv.tar.gz -C $(MOUNTPOINT)/opt

	sudo chroot $(MOUNTPOINT) /bin/sh /usr/sbin/buildenv.sh

	sudo umount $(MOUNTPOINT)
	sudo chown $(USER) "$@"

test:
# Run Master
	cd ../ && SPARK_LOCAL_IP=127.0.0.1 /usr/bin/java -cp $(CLASSPATH_JARS) -Xmx1g org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7077 --webui-port 8080 > lkl/master.txt 2>&1 &
	 sleep 2

# Run Worker
	cd ../ && SPARK_LOCAL_IP=127.0.0.1 /usr/bin/java -cp $(CLASSPATH_JARS):../conf/ -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://localhost:7077 > lkl/worker.txt 2>&1 &
	 sleep 2

# Run Enclave
	@-$(shell openvpn --mktun --dev tap0)
	$(shell ip link set dev tap0 up)
	$(shell ip addr add 10.0.1.254/24 dev tap0)
	cd ../ && LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib MUSL_STRACELKL=1 MUSL_VERBOSELKL=1 MUSL_TRACE_MMAP=1 MUSL_TAP=tap0 MUSL_HD=${PWD}/alpine-rootfs.img MUSL_KERNEL=0 MUSL_VERSION=1 MUSL_ESLEEP=1 MUSL_SSLEEP=4000 MUSL_ESPINS=50000 MUSL_SSPINS=500 MUSL_STHREADS=32 MUSL_ETHREADS=4 $(SGX_LKL)/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java -XX:InitialCodeCacheSize=2000k -XX:ReservedCodeCacheSize=2000K -Xms2000k -Xmx2000k -XX:CompressedClassSpaceSize=1500K -XX:MaxMetaspaceSize=8000K -XX:+UseCompressedClassPointers -XX:+PrintCompilation -cp /spark/conf/:/spark/assembly/target/scala-2.11/jars\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain > lkl/enclave.txt 2>&1 &
	 sleep 2

# Spark Job
	cd ../ && SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.MyWordCount --master spark://localhost:7077 --deploy-mode cluster --verbose --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar $(shell pwd)/example_input_file $(shell pwd)/output > lkl/job.txt 2>&1 &

# Kill Process and clean up networking
#   @-$(shell openvpn --rmtun --dev tap0)
#   @-$(shell pkill -9 sgx-lkl-starter)

clean:
	rm -rf alpine*
	rm -rf *.txt
	rm -rf ../work
	rm -rf output
	pkill -9 java
	pkill -9 spark

