package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

class RingBuffConsumer extends RingBuffConsumerRaw {
	private ISerialization serializer;

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	public Object read() {		
		try {
			return serializer.deserialize(readAsBytes());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
