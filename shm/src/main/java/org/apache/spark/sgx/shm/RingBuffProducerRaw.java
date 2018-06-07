package org.apache.spark.sgx.shm;

public class RingBuffProducerRaw {
	private final AlignedMappedDataBuffer buffer;
	private final int FIRST_SLOT;
	private int pos;
	private int readPos;
	private final int ALIGNMENT = 64;
	
	public RingBuffProducerRaw(MappedDataBuffer buffer, int reserved_slots) {
		this.buffer = new AlignedMappedDataBuffer(buffer, ALIGNMENT);
		FIRST_SLOT = reserved_slots;
		pos = FIRST_SLOT;
		readPos = pos;
	}	
	
	public void write(long value) throws InterruptedException {
	    write(new byte[] {
	            (byte) (value >> 56),
	            (byte) (value >> 48),
	            (byte) (value >> 40),
	            (byte) (value >> 32),
	            (byte) (value >> 24),
	            (byte) (value >> 16),
	            (byte) (value >> 8),
	            (byte) value
	        });
	}
	
	private int getReadPos() {
		readPos = buffer.getInt(0);
		return readPos;
	}
	
	private int slotsFree() {
		getReadPos();
		if (pos < readPos) {
			return readPos - pos - 2;
		}
		else if (pos > readPos) {
			return buffer.slots() - pos + readPos - FIRST_SLOT - 1;
		}
		return buffer.slots() - FIRST_SLOT - 1; 		
	}

	public int bytesFree() {
		return slotsFree() * ALIGNMENT;
	}

	public boolean hasEnoughSpace(int slotsNeeded) {
		return slotsNeeded <= slotsFree();
	}
	
	private void waitForEnoughSpace(int slotsNeeded) {
		while (!hasEnoughSpace(slotsNeeded)) {
			try {
				Thread.sleep(16);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void write(byte[] bytes) throws InterruptedException {
		if (bytes.length > (buffer.slots() - 1 - FIRST_SLOT) * buffer.alignment()) {
			throw new RuntimeException(buffer + " Buffer too small to hold an element of size " + bytes.length);
		}
		int slotsNeeded = buffer.slotsNeeded(bytes.length);
		buffer.waitUntil(pos, 0);
		waitForEnoughSpace(slotsNeeded);
		if (pos == buffer.slots() - 1) {
			// We are at the very last slot.
			// Write the size here and the payload at the beginning of the buffer.
			buffer.putBytes(FIRST_SLOT, bytes);
			System.err.println(buffer + " Wrote " + bytes.length + " bytes to pos " + FIRST_SLOT + " (slots=" + slotsNeeded + ")");
		} else if (buffer.isValid(pos + slotsNeeded)) {
			// There is enough space before the end of the buffer.
			// Write the size here and the payload right after.
			buffer.putBytes(pos+1, bytes);
			System.err.println(buffer + " Wrote " + bytes.length + " bytes to pos " + (pos+1) + " (slots=" + slotsNeeded + ")");
		} else {
			// There is not enough space. So we need to divide up the payload data.
			int wrapPoint = (buffer.slots() - pos - 1) * buffer.alignment();
			buffer.putBytes(pos+1, bytes, 0, wrapPoint);
			System.err.println(buffer + " Wrote " + wrapPoint + " bytes to pos " + (pos+1) + " and " + (bytes.length - wrapPoint) + " bytes to pos " + FIRST_SLOT + " (slots=" + slotsNeeded + ")");
			buffer.putBytes(FIRST_SLOT, bytes, wrapPoint, bytes.length - wrapPoint);
		}
		buffer.putInt(pos, bytes.length);
		
		pos += (slotsNeeded + 1);
		if (pos >= buffer.slots()) {
			pos -= buffer.slots();
			pos += FIRST_SLOT;
		}
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}