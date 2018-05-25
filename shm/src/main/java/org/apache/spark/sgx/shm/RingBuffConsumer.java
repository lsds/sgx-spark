package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffConsumer {
	private AlignedMappedDataBuffer buffer;
	private ISerialization serializer;
	
	private int pos = 0;
	
	private byte[] bytes = new byte[1024];

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		this.buffer = new AlignedMappedDataBuffer(buffer, 64);
		this.serializer = serializer;
	}

	public Object read() {
		Object obj = null;

		do {
			try {
				int len = buffer.waitWhile(pos, 0);
				if (len > bytes.length) {
					bytes = new byte[len];
				}
				buffer.getBytes(pos+1, bytes, 0, len);
				buffer.putInt(pos, 0);
				pos += buffer.slotsNeeded(len) + 1;
				obj = serializer.deserialize(bytes);
			} catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				obj = null;
			}
		} while (obj == null);
		return obj;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
