package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

class RingBuffProducer {
	private AlignedMappedDataBuffer buffer;
	private ISerialization serializer;
	
	private int pos = 0;
	
	RingBuffProducer(MappedDataBuffer buffer, ISerialization serializer) {
		this.buffer = new AlignedMappedDataBuffer(buffer, 64);
		this.serializer = serializer;
	}

	/*
	 * Potential TODO:
	 * - Deserialization: do not copy first into local, deserialize directly from shared memory
	 * - Wrapping at end of buffer
	 * - Use System.arracopy
	 * - madvise: do not page out
	 */
	
	void write(Object o) {
		boolean success = false;
		
		do {
			try {				
				byte[] bytes = serializer.serialize(o);
				buffer.waitUntil(pos, 0);
				buffer.putBytes(pos+1, bytes);
				buffer.putInt(pos, bytes.length);
				pos += buffer.slotsNeeded(bytes.length) + 1;
				success = true;
			} catch (Exception e) {
				e.printStackTrace();
				success = false;
			}
		} while (!success);
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
