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

#define MAX(a,b) ((a) > (b) ? a : b)
#define MIN(a,b) ((a) < (b) ? a : b)

#define MAX_WAIT 32768
#define MIN_WAIT 64

static void *register_shm(char* path, int len)
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
		fprintf(stderr, "Error: invalid memory size length %d\n", len);
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

JNIEXPORT jlongArray JNICALL Java_org_apache_spark_sgx_shm_RingBuffLibWrapper_init_1shm(JNIEnv *env, jclass cls, jstring file, jint size) {
	char* shm_file = (char *) (*env)->GetStringUTFChars(env, file, 0);

	// Load shared memory
	size_t strl = strlen(shm_file);
	char enc_to_out_file[strl + 4];
	char out_to_enc_file[strl + 4];
	snprintf(enc_to_out_file, strl + 4, "%s-eo", shm_file);
	snprintf(out_to_enc_file, strl + 4, "%s-oe", shm_file);

	void* common_mem = register_shm(shm_file, size);
	void* enc_to_out_mem = register_shm(enc_to_out_file, size);
	void* out_to_enc_mem = register_shm(out_to_enc_file, size);

	long res[3];
	res[0] = (long) enc_to_out_mem;
	res[1] = (long) out_to_enc_mem;
	res[2] = (long) common_mem;

	jlongArray result = (*env)->NewLongArray(env, 3);
	(*env)->SetLongArrayRegion(env, result, 0, 3, res);
	return result;
}
