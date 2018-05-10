package org.apache.spark.sgx.shm;

enum EShmMessageType {
	NEW_CONNECTION,
	CLOSE_CONNECTION,
	ACCEPTED_CONNECTION,
	REGULAR
}
