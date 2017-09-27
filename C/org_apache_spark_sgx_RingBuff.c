#include <jni.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "org_apache_spark_sgx_RingBuff.h"
#include "ring_buff.h"

static void *register_shm(char* path, size_t len)
{
	if (path == NULL || strlen(path) == 0)
		exit(2);

	int fd = shm_open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
	if (fd == -1) {
		fprintf(stderr, "Error: unable to access shared memory %s (%s)\n", path, strerror(errno));
		perror("open()");
		exit(3);
	}
	int flags = fcntl(fd, F_GETFL);
	if (flags == -1) {
		perror("fcntl(shmem_fd, F_GETFL)");
		exit(4);
	}
	int res = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	if (res == -1) {
		perror("fcntl(shmem_fd, F_SETFL)");
		exit(5);
	}

	if (len <= 0) {
		fprintf(stderr, "Error: invalid memory size length %zu\n", len);
		exit(6);
	}

	if(ftruncate(fd, len) == -1) {
		fprintf(stderr, "ftruncate: %s\n", strerror(errno));
	    exit(7);
	}

	void *addr;
	if ((addr = mmap(0, len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED) {
		fprintf(stderr, "mmap: %s\n", strerror(errno));
		exit(8);
	}

	memset(addr, 0, sizeof(len));
	close(fd);
	return addr;
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sgx_RingBuff_write_1msg(JNIEnv *env, jobject this, jlong handle, jbyteArray data, jint len) {
	return ring_buff_write_msg((ring_buff_handle_t) handle, (void*) data, (uint32_t) len) == RING_BUFF_ERR_OK;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sgx_RingBuff_read_1msg(JNIEnv *env, jobject this, jlong handle) {
	void *data;
	uint32_t len;
	if (ring_buff_read_msg((ring_buff_handle_t) handle, &data, &len) != RING_BUFF_ERR_OK) {
		return (jbyteArray) {0};
	}

	return (jbyteArray) data;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_sgx_RingBuff_register_1shm(JNIEnv *env, jclass cls, jstring file) {
	char *str = (char *) (*env)->GetStringUTFChars(env, file, 0);
	return (jlong) register_shm(str, strlen(str));
}

