#include <jni.h>
#include <stdio.h>
#include "org_apache_spark_sgx_RingBuff.h"
#include "ring_buff.h"

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

