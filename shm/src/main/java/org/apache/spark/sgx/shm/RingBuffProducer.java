package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

public class RingBuffProducer {
	private AlignedMappedDataBuffer buffer;
	private ISerialization serializer;
	
	private int pos = 0;
	
	public RingBuffProducer(MappedDataBuffer buffer, ISerialization serializer) {
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
	
	public void write(Object o) {
		try {				
			byte[] bytes = serializer.serialize(o);
			if (bytes.length > (buffer.slots() - 1) * buffer.alignment()) {
				throw new RuntimeException("Buffer too small to hold an element of size " + bytes.length);
			}
			
			int slotsNeeded = buffer.slotsNeeded(bytes.length);

			buffer.waitUntil(pos, 0);
			if (pos == buffer.slots() - 1) {
				// We are at the very last slot.
				// Write the size here and the payload at the beginning of the buffer.
				buffer.putBytes(0, bytes);
			} else if (buffer.isValid(pos + slotsNeeded)) {
				// There is enough space before the end of the buffer.
				// Write the size here and the payload right after.
				buffer.putBytes(pos+1, bytes);
			} else {
				// There is not enough space. So we need to divide up the payload data.
				int wrapPoint = (buffer.slots() - pos - 1) * buffer.alignment();
				buffer.putBytes(pos+1, bytes, 0, wrapPoint);
				buffer.putBytes(0, bytes, wrapPoint, bytes.length - wrapPoint);
			}
			buffer.putInt(pos, bytes.length);
			pos += (slotsNeeded + 1) % buffer.slots();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
