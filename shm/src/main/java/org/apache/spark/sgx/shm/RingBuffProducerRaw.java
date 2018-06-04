package org.apache.spark.sgx.shm;

public class RingBuffProducerRaw {
	private AlignedMappedDataBuffer buffer;
	private int pos = 0;
	
	public RingBuffProducerRaw(MappedDataBuffer buffer) {
		this.buffer = new AlignedMappedDataBuffer(buffer, 64);
	}	
	
	public void write(byte[] bytes) throws InterruptedException {
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
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}