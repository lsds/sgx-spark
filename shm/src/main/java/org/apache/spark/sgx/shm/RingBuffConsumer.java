package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffConsumer extends RingBuffConsumerRaw {
	private ISerialization serializer;

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 2);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	@SuppressWarnings("unchecked")
	public <T> T read() {		
		try {
			T t = (T) serializer.deserialize(readBytes());
			System.err.println("Read: " + t);
			return t;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
