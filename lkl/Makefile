# CHANGE THIS TO POINT TO YOUR SGX-LKL DIRECTORY
SGX_LKL=/data/fkelbert/sgx-lkl

ALPINE_MAJOR=3.6
ALPINE_VERSION=3.6.2
ALPINE_ARCH=x86_64

ALPINE_TAR=alpine-minirootfs.tar.gz
MOUNTPOINT=/media/ext4disk
SPARK_DIR=/spark

IMAGE_SIZE_MB=2000
CLASSPATH_JARS=conf/:assembly/target/scala-2.11/jars/\*

JVM=jre8-no-segv-nommap.tar.gz
#JVM=jre8-no-segv-nommap-slowdebug.tar.gz

IMG_NAME=alpine-rootfs.img

.DELETE_ON_ERROR:

all: alpine-rootfs.img

$(ALPINE_TAR): 
	curl -L -o "$@" "https://nl.alpinelinux.org/alpine/v$(ALPINE_MAJOR)/releases/$(ALPINE_ARCH)/alpine-minirootfs-$(ALPINE_VERSION)-$(ALPINE_ARCH).tar.gz"

prepare-image: $(ALPINE_TAR) buildenv.sh 
	dd if=/dev/zero of="$(IMG_NAME)" count=$(IMAGE_SIZE_MB) bs=1M
	mkfs.ext4 "$(IMG_NAME)"

	sudo mkdir -p $(MOUNTPOINT)
	sudo mount -t ext4 -o loop "$(IMG_NAME)" $(MOUNTPOINT)

	sudo tar -C $(MOUNTPOINT) -xvf $(ALPINE_TAR)
	sudo install buildenv.sh $(MOUNTPOINT)/usr/sbin
	sudo cp scala-library.jar $(MOUNTPOINT)/home
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)/lib
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/
	sudo mkdir -p $(MOUNTPOINT)/$(SPARK_DIR)/examples/target/scala-2.11/jars
	sudo cp    ../C/* $(MOUNTPOINT)/$(SPARK_DIR)/lib	
	sudo cp /etc/resolv.conf $(MOUNTPOINT)/etc/resolv.conf
	
	sudo mkdir -p $(MOUNTPOINT)/opt
	sudo tar -xf $(JVM) -C $(MOUNTPOINT)/opt

	sudo chroot $(MOUNTPOINT) /bin/sh /usr/sbin/buildenv.sh

	sudo umount $(MOUNTPOINT)
	sudo chown $(USER) "$(IMG_NAME)"

finalize-image:
	sudo mkdir -p $(MOUNTPOINT)
	sudo mount -t ext4 -o loop "$(IMG_NAME)" $(MOUNTPOINT)
	
	sudo cp -r ../conf $(MOUNTPOINT)/$(SPARK_DIR)/
	sudo cp -r ../assembly/target/scala-2.11/jars $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/
	sudo cp    ../examples/target/scala-2.11/jars/spark-examples_2.11-2.3.*-SNAPSHOT.jar $(MOUNTPOINT)/$(SPARK_DIR)/examples/target/scala-2.11/jars/

	sudo umount $(MOUNTPOINT)

test: start-master start-worker start-enclave start-job

test-nosgx: start-master start-worker start-enclave-nosgx start-job

start-master:
	cd ../ && SPARK_LOCAL_IP=127.0.0.1 java -cp $(CLASSPATH_JARS) -Xmx1g org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7077 --webui-port 8080 > lkl/master.txt 2>&1 &

start-worker:
	sleep 2
	cd ../ && SPARK_SGX_ENCLAVE_IP=10.0.1.1 SPARK_SGX_ENCLAVE_PORT=9999 SPARK_SGX_HOST_IP=146.169.2.58 SPARK_LOCAL_IP=127.0.0.1 java -cp $(CLASSPATH_JARS):../conf/ -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://localhost:7077 > lkl/worker.txt 2>&1 &

start-enclave:
	@-$(shell openvpn --mktun --dev tap0)
	$(shell ip link set dev tap0 up)
	$(shell ip addr add 10.0.1.254/24 dev tap0)
	sleep 4
# ENV vars provide the least amount of memory as figured out on 31/8/2017 with the MyWordCount example on file SgxReadme.md
	cd ../ LD_LIBRARY_PATH=/opt/j2re-image/lib/amd64:/opt/j2re-image/lib/amd64/jli:/opt/j2re-image/lib/amd64/server:/lib:/usr/lib:/usr/local/lib SGXLKL_STRACELKL=1 SGXLKL_VERBOSE=1 SGXLKL_TRACE_SYSCALL=0 SGXLKL_TRACE_MMAP=0 SGXLKL_TAP=tap0 SGXLKL_HD=${PWD}/alpine-rootfs.img SGXLKL_KERNEL=0 SGXLKL_VERSION=1 SGXLKL_ESLEEP=1 SGXLKL_SSLEEP=4000 SGXLKL_ESPINS=50000 SGXLKL_SSPINS=500 SGXLKL_STHREADS=8 SGXLKL_ETHREADS=4 SGXLKL_STACK_SIZE=256000 SPARK_SGX_ENCLAVE_IP=10.0.1.1 SPARK_SGX_ENCLAVE_PORT=9999 $(SGX_LKL)/sgx-musl-lkl/obj/sgx-lkl-starter /opt/j2re-image/bin/java -XX:InitialCodeCacheSize=2m -XX:ReservedCodeCacheSize=2m -Xms2m -Xmx2m -XX:CompressedClassSpaceSize=2m -XX:MaxMetaspaceSize=12m -XX:+UseCompressedClassPointers -XX:+AssumeMP -Xint -cp /home/scala-library/:/spark/lib/:/spark/conf/:/spark/assembly/target/scala-2.11/jars/\*:/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain

start-enclave-nosgx:
	sleep 4
	cd ../ &&  java -cp conf:assembly/target/scala-2.11/jars/\*:examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain > lkl/enclave.txt 2>&1 &

start-job:
	sleep 6
	cd ../ && SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.MyWordCount --master spark://localhost:7077 --deploy-mode cluster --verbose --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar $(shell pwd)/example_input_file $(shell pwd)/output > lkl/job.txt 2>&1 &

clean:
	rm -rf alpine*
	rm -rf *.txt
	rm -rf ../work
	rm -rf output
	pkill -9 java ; true
	pkill -9 sgx-lkl-starter  ; true
	pkill -9 spark ; true

