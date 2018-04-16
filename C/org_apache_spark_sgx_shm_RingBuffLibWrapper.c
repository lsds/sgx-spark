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

#include "org_apache_spark_sgx_shm_RingBuffLibWrapper.h"
#include "ring_buff.h"

#define MAX(a,b) ((a) > (b) ? a : b)
#define MIN(a,b) ((a) < (b) ? a : b)

#define MAX_WAIT 32768
#define MIN_WAIT 64

static void *register_shm(char* path, unsigned long long len)
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
		fprintf(stderr, "Error: invalid memory size length %llu\n", len);
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

	close(fd);
	return addr;
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sgx_shm_RingBuffLibWrapper_write_1msg(JNIEnv *env, jclass cls, jlong handle, jbyteArray data) {
	jint len = (*env)->GetArrayLength(env, data);
	char buf[len];
	int ret;
	(*env)->GetByteArrayRegion(env, data, 0, len, buf);

	int wait_write = MIN_WAIT;
	while ((ret = ring_buff_write_msg((ring_buff_handle_t) handle, (void*) buf, (uint32_t) len)) != RING_BUFF_ERR_OK) {
		usleep(wait_write);
		wait_write = MIN(wait_write*2, MAX_WAIT);
	}

	return ret == RING_BUFF_ERR_OK;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sgx_shm_RingBuffLibWrapper_read_1msg(JNIEnv *env, jclass cls, jlong handle) {
	void *data;
	uint32_t len;
	int ret = RING_BUFF_ERR_GENERAL;

	int wait_read = MIN_WAIT;
	while ((ret = ring_buff_read_msg((ring_buff_handle_t) handle, &data, &len)) != RING_BUFF_ERR_OK) {
		usleep(wait_read);
		wait_read = MIN(wait_read*2, MAX_WAIT);
	}

	if ((ret = ring_buff_free((ring_buff_handle_t) handle, data, len)) != RING_BUFF_ERR_OK) {
		printf("Error during ring_buff_free()\n");
		ring_buff_print_err(ret);
		return (jbyteArray) {0};
	}

	jbyteArray result = (*env)->NewByteArray(env, len);
	if (result == NULL) {
		return NULL;
	}

	(*env)->SetByteArrayRegion(env, result, 0, len, data);
	return result;
}

JNIEXPORT jlongArray JNICALL Java_org_apache_spark_sgx_shm_RingBuffLibWrapper_init_1shm(JNIEnv *env, jclass cls, jstring file, jlong size) {
	char* shm_file = (char *) (*env)->GetStringUTFChars(env, file, 0);
	void* shm_addr = register_shm(shm_file, ring_buff_struct_size() * 2);

	// Load message queue control structures to memory
	ring_buff_handle_t enc_to_out_q = (ring_buff_handle_t) shm_addr;
	ring_buff_handle_t out_to_enc_q = ((ring_buff_handle_t) (((char*) shm_addr) + ring_buff_struct_size()));

	// Load shared memory
	size_t strl = strlen(shm_file);
	char enc_to_out_file[strl + 4];
	char out_to_enc_file[strl + 4];
	snprintf(enc_to_out_file, strl + 4, "%s-eo", shm_file);
	snprintf(out_to_enc_file, strl + 4, "%s-oe", shm_file);

	void* enc_to_out_mem = register_shm(enc_to_out_file, size);
	void* out_to_enc_mem = register_shm(out_to_enc_file, size);

	// Set the read buffer for the incoming message queue to match our address space (same for write buffer for outgoing message queue)
	// NOTE: By default, these will both be set to the buffer address of the enclave application, so it only needs to be changed here
	ring_buff_set_read_buff(enc_to_out_q, enc_to_out_mem);
	ring_buff_set_write_buff(out_to_enc_q, out_to_enc_mem);

	long res[2];
	res[0] = (long) enc_to_out_q;
	res[1] = (long) out_to_enc_q;

	jlongArray result = (*env)->NewLongArray(env, 2);
	(*env)->SetLongArrayRegion(env, result, 0, 2, res);
	return result;
}
