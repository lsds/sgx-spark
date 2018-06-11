package org.apache.spark.sgx.shm;

public class MallocedMappedDataBuffer extends MappedDataBuffer {

	private final long offset;
	
	public MallocedMappedDataBuffer(long address, int capacity) {
		super(address, capacity);
		this.offset = address - MappedDataBufferManager.get().startAddress();
		
		// TODO: Make faster
		int i = 0;
		byte[] b = new byte[1];
		while (i < capacity) {
			put(i++, b);
		}
	}
	
	public long offset() {
		return offset;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address() + ", capacity=" + capacity() + ", offset=" + offset + ")";
	}
}