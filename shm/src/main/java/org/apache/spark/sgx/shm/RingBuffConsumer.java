package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

class RingBuffConsumer {
	private AlignedMappedDataBuffer buffer;
	private ISerialization serializer;
	
	private int pos = 0;

	RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		this.buffer = new AlignedMappedDataBuffer(buffer, 64);
		this.serializer = serializer;
	}

	Object read() {
		Object obj = null;
		boolean exception = false;

		do {
			try {
				int len = buffer.waitWhile(pos, 0);
				byte[] bytes = new byte[len];
				buffer.getBytes(pos+1, bytes);
				buffer.putInt(pos, 0);
				pos += buffer.slotsNeeded(len) + 1;
				obj = serializer.deserialize(bytes);
			} catch (Exception e) {
				e.printStackTrace();
				exception = true;
			}
		} while (obj == null && !exception);
		return obj;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
