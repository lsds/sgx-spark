package org.apache.spark.sgx.shm;

public class RingBuffLibWrapper {

	static {
		System.loadLibrary("ringbuff");
	}

	public static native long[] init_shm(String file, int size);
}
