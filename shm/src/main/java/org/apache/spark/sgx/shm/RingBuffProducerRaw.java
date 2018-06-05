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
	
	private void getReadPos() {
		readPos = buffer.getInt(0);
	}
	
	private int slotsFree() {
		getReadPos();
		System.err.println("readPos: " + readPos);
		System.err.println("pos: " + pos);
		if (pos < readPos) {
			return readPos - pos - 2;
		}
		else if (pos > readPos) {
			return buffer.slots() - pos + readPos - FIRST_SLOT - 1;
		}
		return buffer.slots() - FIRST_SLOT - 1; 		
	}

	public int bytesFree() {
		System.err.println("bytesFree: " + slotsFree());
		return slotsFree() * ALIGNMENT;
	}
	
//	private boolean enoughSpace(int needed) {
//		return(pos < readPos && needed < readPos - pos)
//				|| (pos > readPos && needed < buffer.slots() - pos + readPos - FIRST_SLOT)
//				|| pos == readPos;
//	}

	public boolean hasEnoughSpace(int slotsNeeded) {
//		if (!enoughSpace(needed));
//		readPos = getReadPos();
//		return enoughSpace(needed);
		System.err.println("slotsNeeded: " + slotsNeeded);
		System.err.println("slotsFree: " + slotsFree() + " buffer.slots="+buffer.slots());
		return slotsNeeded <= slotsFree();
	}
	
	private void waitForEnoughSpace(int slotsNeeded) {
		while (!hasEnoughSpace(slotsNeeded)) {
			System.out.println("Waiting for enough space");
			try {
				Thread.sleep(16);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void write(byte[] bytes) throws InterruptedException {
		System.err.println("xxx 1");
		if (bytes.length > (buffer.slots() - 1 - FIRST_SLOT) * buffer.alignment()) {
			throw new RuntimeException("Buffer too small to hold an element of size " + bytes.length);
		}
		System.err.println("xxx 2");
		int slotsNeeded = buffer.slotsNeeded(bytes.length);
		System.err.println("xxx 3: " + slotsNeeded);
		buffer.waitUntil(pos, 0);
		System.err.println("xxx 4");
		waitForEnoughSpace(slotsNeeded);
		System.err.println("xxx 5");
		if (pos == buffer.slots() - 1) {
			System.err.println("xxx 6");
			// We are at the very last slot.
			// Write the size here and the payload at the beginning of the buffer.
			buffer.putBytes(FIRST_SLOT, bytes);
		} else if (buffer.isValid(pos + slotsNeeded)) {
			System.err.println("xxx 7");
			// There is enough space before the end of the buffer.
			// Write the size here and the payload right after.
			buffer.putBytes(pos+1, bytes);
		} else {
			System.err.println("xxx 8");
			// There is not enough space. So we need to divide up the payload data.
			int wrapPoint = (buffer.slots() - pos - 1) * buffer.alignment();
			buffer.putBytes(pos+1, bytes, 0, wrapPoint);
			buffer.putBytes(FIRST_SLOT, bytes, wrapPoint, bytes.length - wrapPoint);
		}
		System.err.println("xxx 9");
		buffer.putInt(pos, bytes.length);
		System.err.println("xxx 10");
		pos += (slotsNeeded + 1);
		if (pos > buffer.slots()) {
			pos -= buffer.slots();
			pos += FIRST_SLOT;
		}
		System.err.println("xxx 11");
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}