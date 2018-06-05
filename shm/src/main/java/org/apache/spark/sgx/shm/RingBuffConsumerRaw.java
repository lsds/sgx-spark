package org.apache.spark.sgx.shm;

public class RingBuffConsumerRaw {
	private final AlignedMappedDataBuffer buffer;
	private final int FIRST_POS;
	private int pos;
	private final int ALIGNMENT = 64;

	public RingBuffConsumerRaw(MappedDataBuffer buffer, int reserved_slots) {
		this.buffer = new AlignedMappedDataBuffer(buffer, ALIGNMENT);
		FIRST_POS = reserved_slots;
		pos = FIRST_POS;
		shareReadPos();
	}
	
	public long readLong() throws InterruptedException {
		byte[] bytes = readBytes();
		
		return bytes[0] << 56
			+  bytes[1] << 48
			+  bytes[2] << 40
			+  bytes[3] << 32
			+  bytes[4] << 24
			+  bytes[5] << 16
			+  bytes[6] << 8
			+  bytes[7];
	}

	public byte[] readBytes() throws InterruptedException {
			int len = buffer.waitWhile(pos, 0);
			int slotsNeeded = buffer.slotsNeeded(len);
			
			byte[] bytes = new byte[len];
			
			if (pos == buffer.slots() - 1) {
				// We are at the very last slot.
				// Read the payload at the beginning of the buffer.
				buffer.getBytes(FIRST_POS, bytes, 0, len);
			} else if (buffer.isValid(pos + slotsNeeded)) {
				// There was enough space before the end of the buffer.
				// We can read the payload in one go.
				buffer.getBytes(pos + 1, bytes, 0, len);
			} else {
				// There was not enough space. So we had to divide up the payload data.
				int wrapPoint = (buffer.slots() - pos - 1) * buffer.alignment();
				buffer.getBytes(pos + 1, bytes, 0, wrapPoint);
				buffer.getBytes(FIRST_POS, bytes, wrapPoint, len - wrapPoint);
			}
			
			buffer.putInt(pos, 0);
			pos += (slotsNeeded + 1);
			if (pos > buffer.slots()) {
				pos -= buffer.slots();
				pos += FIRST_POS;
			}
			shareReadPos();
			return bytes;
	}
	
	private void shareReadPos() {
		System.err.println("Writing readPos: " + pos);
		buffer.putInt(0, pos);
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}