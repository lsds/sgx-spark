package org.apache.spark.sgx.shm;

public class MallocedMappedDataBuffer extends MappedDataBuffer {

	private final long offset;
	
	public MallocedMappedDataBuffer(long address, int capacity) {
		super(address, capacity);
		System.out.println("MallocedMappedDataBuffer at address " + address);
		this.offset = address - MappedDataBufferManager.get().startAddress();
		System.out.println("MallocedMappedDataBuffer offset is " + offset);
	}
	
	public long offset() {
		return offset;
	}
}