package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffProducer extends RingBuffProducerRaw {
	private ISerialization serializer;
	private Object writelock = new Object();
	
	public RingBuffProducer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 1);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	public void write(Object o) {
		try {
			byte[] b = serializer.serialize(o);
			synchronized(writelock) {
				/*
				try {
					throw new Exception("write object " + o + " of size " + b.length);
				} catch (Exception e) {
					e.printStackTrace();
				}
				*/
				write(b);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
