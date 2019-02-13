package org.apache.spark.sgx.shm;

public class MallocedMappedDataBuffer extends MappedDataBuffer {

	private final long offset;
	
	public MallocedMappedDataBuffer(long address, int capacity) {
		super(address, capacity);
		this.offset = address - MappedDataBufferManager.get().startAddress();
		memset(address, capacity, (byte)0);
	}
	
	public long offset() {
		return offset;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address() + ", capacity=" + capacity() + ", offset=" + offset + ")";
	}
}