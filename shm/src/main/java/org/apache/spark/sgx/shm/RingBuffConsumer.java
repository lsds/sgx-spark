package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffConsumer extends RingBuffConsumerRaw {
	private ISerialization serializer;
	private Object readlock = new Object();

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 1);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	@SuppressWarnings("unchecked")
	public <T> T read() {		
		try {
			byte[] b;
			Object o;
			synchronized(readlock) {
				b = readBytes();

				o = serializer.deserialize(b);
				try {
					throw new Exception("read object " + o + " of size " + b.length);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			return (T) o;
			//return (T) serializer.deserialize(b);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
