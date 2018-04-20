package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.data.AlignedMappedDataBuffer;
import org.apache.spark.sgx.data.MappedDataBuffer;

class RingBuffConsumer {
	
	private AlignedMappedDataBuffer buffer;

	RingBuffConsumer(MappedDataBuffer buffer) {
		this.buffer = new AlignedMappedDataBuffer(buffer);
	}

	Object read() {
		Object obj = null;
		boolean exception = false;

		do {
			try {
				int pos = buffer.position();
				int len = buffer.waitWhile(0);
				byte[] bytes = new byte[len];
				buffer.get(bytes);
				buffer.putInt(pos, 0);
				obj = Serialization.deserialize(bytes);
				
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
