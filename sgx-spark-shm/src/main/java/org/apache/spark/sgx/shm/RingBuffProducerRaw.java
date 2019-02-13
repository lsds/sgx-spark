package org.apache.spark.sgx.shm;

public class RingBuffProducerRaw {
	private final AlignedMappedDataBuffer buffer;
	private final int FIRST_SLOT;
	private int pos;
	private int readPos;
	
	public RingBuffProducerRaw(MappedDataBuffer buffer, int reserved_slots) {
		this.buffer = new AlignedMappedDataBuffer(buffer);
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

	public boolean hasEnoughSpace(int slotsNeeded) {
		return slotsNeeded <= slotsFree();
	}
	
	private void waitForEnoughSpace(int slotsNeeded) {
		while (!hasEnoughSpace(slotsNeeded)) {
			try {
				Thread.sleep(16);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void write(byte[] bytes) throws InterruptedException {
		if (bytes.length > (buffer.slots() - 1 - FIRST_SLOT) * buffer.slotSize()) {
			throw new RuntimeException(buffer + " Buffer too small to hold an element of size " + bytes.length);
		}
		int slotsNeeded = buffer.slotsNeeded(bytes.length);
		buffer.waitUntil(pos, 0);
		waitForEnoughSpace(slotsNeeded);
		if (pos == buffer.slots() - 1) {
			// We are at the very last slot.
			// Write the size here and the payload at the beginning of the buffer.
			buffer.putBytes(FIRST_SLOT, bytes);
		} else if (buffer.isValid(pos + slotsNeeded)) {
			// There is enough space before the end of the buffer.
			// Write the size here and the payload right after.
			buffer.putBytes(pos+1, bytes);
		} else {
			// There is not enough space. So we need to divide up the payload data.
			int wrapPoint = (buffer.slots() - pos - 1) * buffer.slotSize();
			buffer.putBytes(pos+1, bytes, 0, wrapPoint);
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