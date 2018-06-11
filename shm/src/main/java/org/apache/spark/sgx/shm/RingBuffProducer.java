package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffProducer extends RingBuffProducerRaw {
	private ISerialization serializer;
	
	public RingBuffProducer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 2);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	public void write(Object o) {
		try {
			write(serializer.serialize(o));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
