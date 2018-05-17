package org.apache.spark.sgx.shm;

public class MallocedMappedDataBuffer extends MappedDataBuffer {

	private long offset;
	
	public MallocedMappedDataBuffer(long address, int capacity) {
		super(address, capacity);
		System.out.println("MallocedMappedDataBuffer at address " + address);
		this.offset = MappedDataBufferManager.get().startAddress() - address;
		System.out.println("MallocedMappedDataBuffer offset is " + offset);
	}
	
	public long offset() {
		return offset;
	}
}