package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.SgxSettings;

class AlignedMappedDataBuffer {
	
	private final MappedDataBuffer buffer;

	// TODO: I think this should be refactored. Alignment isn't the best word here.
	//       What is being called "alignment" is actually the slot size. This makes
	//       the code unnecessarily confusing.	
	private final int slotSize;
	private final int slots;
	private final int power;
	
	// Exponential backoff wait times (milliseconds)
	private final int MIN_WAIT = SgxSettings.BACKOFF_WAIT_MIN();
	private final int MAX_WAIT = SgxSettings.BACKOFF_WAIT_MAX();
	
	private final int DEFAULT_SLOTSIZE = 64;
	
	/**
	 * An {@link AlignedMappedDataBuffer} aligns the specified buffer to
	 * the specified slotSize (in bytes). If the slotSize is not a power of two,
	 * the the next large slotSize that is a power of two will be used.
	 * The minimum slotSize is 8.
	 * 
	 * @param buffer
	 * @param slotSize
	 */
	public AlignedMappedDataBuffer(MappedDataBuffer buffer) {
		if (buffer == null) {
			throw new RuntimeException("Invalid arguments.");
		}

		this.buffer = buffer;
		this.power = nextPowerTwo(DEFAULT_SLOTSIZE);
		this.slotSize = 1 << this.power;
		this.slots = buffer.capacity() >> power;
	}
	
	private int nextPowerTwo(int a) {
		int power = 3;
		while ((1 << power) < a) power++;
		return power;
	}
	
	private void check(int slot) {
		if (!isValid(slot)) throw new RuntimeException("Invalid slot: " + slot);
	}
	
	public boolean isValid(int slot) {
		return slot >= 0 && slot < this.slots;
	}
	
	private void check(int slot, int length) {
		check(slot);
		int needed = slotsNeeded(length); 
		if (needed > 1) {
			check(slot + needed -1);
		}
	}
	
	private int toIndex(int slot) {
		return slot << power;
	}
	
	int slotsNeeded(int length) {
		if (length <= slotSize) {
			return 1;
		} else {		
			int r = length % slotSize;
			return ((r == 0) ? length : length + (slotSize - r)) >> power;
		}
	}
	
	long address() {
		return buffer.address();
	}
	
	void putInt(int slot, int value) {
		check(slot);
		buffer.putInt(toIndex(slot), value);
	}
	
	int getInt(int slot) {
		check(slot);
		int v = buffer.getInt(toIndex(slot));
		return v;
	}
	
	void putBytes(int slot, byte[] value) {
		putBytes(slot, value, 0, value.length);
	}
	
	void putBytes(int slot, byte[] value, int from, int length) {
		check(slot, length);
		buffer.put(toIndex(slot), value, from, length);
	}
	
	byte[] getBytes(int slot, byte[] value) {
		return getBytes(slot, value, 0, value.length);
	}
	
	byte[] getBytes(int slot, byte[] value, int from, int length) {
		check(slot, length);
		return buffer.get(toIndex(slot), value, from, length);
	}
	
	int waitWhile(int slot, int value) throws InterruptedException {
		int wait = MIN_WAIT;
		int res;
		int index = toIndex(slot);
		while ((res = buffer.getInt(index)) == value) {	
			Thread.sleep(wait);
			wait = Math.min(wait << 1, MAX_WAIT);
		}
		
		return res;
	}

	int waitUntil(int slot, int value) throws InterruptedException {
		int wait = MIN_WAIT;
		int res;
		int index = toIndex(slot);
		while ((res = buffer.getInt(index)) != value) {
			Thread.sleep(wait);
			wait = Math.min(wait << 1, MAX_WAIT);
		}
		return res;
	}
	
	public int slotSize() {
		return this.slotSize;
	}
	
	int slots() {
		return this.slots;
	}

	void zero(int pos, int slots) {
		int length = slotSize * slots;
		buffer.zero(toIndex(pos), length);
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address() + ")";
	}
}
