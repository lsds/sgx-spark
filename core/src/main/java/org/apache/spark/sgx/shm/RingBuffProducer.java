package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.data.IDataBuffer;

class RingBuffProducer {
	private IDataBuffer buffer;
	
	private int position = 0;

	RingBuffProducer(IDataBuffer buffer) {
		this.buffer = buffer;
	}

	/*
	 * TODO:
	 * - Align with cache line 64b
	 * - Deserialization: do not copy first into local, deserialize directly from shared memory
	 * - Wrapping at end of buffer
	 * - Use System.arracopy
	 * - madvise: do not page out
	 */
	
	int count = 1;
	
	boolean write(Object o) {
		boolean success = false;
		boolean exception = false;
		
		do {
			try {
				byte[] bytes = Serialization.serialize(o);
				buffer.putInt(position, 0);
				int startpos = position;
				position += 4;
				for (byte b : bytes) {
					buffer.put(position++, b);
				}
				buffer.putInt(startpos, bytes.length);
				
				success = true;
			} catch (Exception e) {
				e.printStackTrace();
				exception = true;
			}
		} while (!success && !exception);
		return success;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
