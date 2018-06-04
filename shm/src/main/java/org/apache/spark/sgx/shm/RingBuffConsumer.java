package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

class RingBuffConsumer {
	private AlignedMappedDataBuffer buffer;
	private ISerialization serializer;
	
	private int pos = 0;

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		this.buffer = new AlignedMappedDataBuffer(buffer, 64);
		this.serializer = serializer;
	}

	public Object read() {		
		try {
			return serializer.deserialize(readAsBytes());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public byte[] readAsBytes() throws InterruptedException {
			int len = buffer.waitWhile(pos, 0);
			int slotsNeeded = buffer.slotsNeeded(len);
			
			byte[] bytes = new byte[len];
			
			if (pos == buffer.slots() - 1) {
				// We are at the very last slot.
				// Read the payload at the beginning of the buffer.
				buffer.getBytes(0, bytes, 0, len);
			} else if (buffer.isValid(pos + slotsNeeded)) {
				// There was enough space before the end of the buffer.
				// We can read the payload in one go.
				buffer.getBytes(pos + 1, bytes, 0, len);
			} else {
				// There was not enough space. So we had to divide up the payload data.
				int wrapPoint = (buffer.slots() - pos - 1) * buffer.alignment();
				buffer.getBytes(pos + 1, bytes, 0, wrapPoint);
				buffer.getBytes(0, bytes, wrapPoint, len - wrapPoint);
			}
			
			buffer.putInt(pos, 0);
			pos += (slotsNeeded + 1) % buffer.slots();
			return bytes;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
