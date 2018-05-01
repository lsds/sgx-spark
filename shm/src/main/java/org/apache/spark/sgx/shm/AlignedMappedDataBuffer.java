package org.apache.spark.sgx.shm;

class AlignedMappedDataBuffer {
	
	private MappedDataBuffer buffer;
	
	private final int alignment;
	private final int slots;
	private final int power;
	
	// Exponential backoff wait times (milliseconds)
	private static int MAX_WAIT = 16;
	private static int MIN_WAIT = 1;
	
	/**
	 * An {@link AlignedMappedDataBuffer} aligns the specified buffer to
	 * the specified alignment (in bytes). If the alignment is not a power of two,
	 * the the next large alignment that is a power of two will be used. 
	 * The minimum alignment is 8.
	 * 
	 * @param buffer
	 * @param alignment
	 */
	public AlignedMappedDataBuffer(MappedDataBuffer buffer, int alignment) {
		if (buffer == null || alignment < 0) {
			throw new RuntimeException("Invalid arguments.");
		}
		
		this.buffer = buffer;
		this.power = nextPowerTwo(alignment);
		this.alignment = 1 << this.power;
		this.slots = buffer.capacity() >> power;
	}
	
	private int nextPowerTwo(int a) {
		int power = 3;
		while ((1 << power) < a) power++;
		return power;
	}
	
	private void check(int slot) {
		if (slot < 0 || slot >= this.slots) throw new RuntimeException("Invalid slot: " + slot);
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
		if (length <= alignment) {
			return 1;
		} else {		
			int r = length % alignment;
			return ((r == 0) ? length : length + (alignment - r)) >> power;
		}
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
	
	int alignment() {
		return this.alignment;
	}
	
	int slotSize() {
		return this.alignment;
	}
	
	int slots() {
		return this.slots;
	}
}
