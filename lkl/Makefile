# CHANGE THIS TO POINT TO YOUR SGX-LKL DIRECTORY
SGX_LKL=/data/fkelbert/sgx-lkl

ALPINE_MAJOR=3.7
ALPINE_VERSION=3.7.0
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

all: prepare-image finalize-image

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
	sudo cp    ../sgx/target/*jar $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/jars/
	sudo cp    ../shm/target/*jar $(MOUNTPOINT)/$(SPARK_DIR)/assembly/target/scala-2.11/jars/
	sudo cp    ../examples/target/scala-2.11/jars/spark-examples_2.11-2.3.2-SGX.jar $(MOUNTPOINT)/$(SPARK_DIR)/examples/target/scala-2.11/jars/

	sudo umount $(MOUNTPOINT)

clean:
	rm -rf *.txt
	rm -rf ../work
	rm -rf output
	pkill -9 sgx-lkl-starter  ; true
	pkill -9 spark ; true
