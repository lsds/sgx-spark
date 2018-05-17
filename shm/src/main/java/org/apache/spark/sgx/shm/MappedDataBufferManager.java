package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.SgxSettings;

public class MappedDataBufferManager {
	/*
	 * The underlying memory buffer. The memory
	 * is divided into blocks of size blockSize
	 * that will be handed out in consecutive chunks.
	 */
	private final MappedDataBuffer buffer;
	
	private final int noBlocks;
	private final int blockSize = 4096;
	
	/*
	 * Indicates which blocks of the buffer are in use.
	 * x < 0: block is not in use, starting from this block, x consecutive blocks are free
	 * x > 0: block is in use; starting from this block, x consecutive blocks are in use 
	 */
	private final int[] inUse;
	
	private int freeBlocks;
	
	private static MappedDataBufferManager _instance = null;
	
	public static void init(MappedDataBuffer buffer) {		
		if (_instance != null) {
			throw new RuntimeException("Already initialized");
		}
		
		_instance = new MappedDataBufferManager(buffer);
	}
	
	public static MappedDataBufferManager get() {
		if (_instance == null) {
			throw new RuntimeException("Not initialized");
		}
		
		return _instance;
	}
	
	private MappedDataBufferManager(MappedDataBuffer buffer) {
		this.buffer = buffer;
		this.noBlocks = buffer.capacity() / blockSize;
		this.inUse = new int[noBlocks];
		markFree(0, noBlocks);
		
		freeBlocks = noBlocks;
	}
	
	private int blocksNeeded(int bytes) {
		int r = bytes % blockSize;
		return ((r == 0) ? bytes : bytes + (blockSize - r)) / blockSize;
	}
	
	private void markUsed(int startBlock, int blocks) {
		for (int i = 0; i < blocks; i++) {
			inUse[startBlock + i] = blocks - i;
		}
	}
	
	private void markFree(int startBlock, int blocks) {
		for (int i = 0; i < blocks; i++) {
			inUse[startBlock + i] = (-1 * blocks) + i;
		}
	}	
	
	public MallocedMappedDataBuffer malloc(int bytes) {
		if (SgxSettings.IS_ENCLAVE()) {
			throw new RuntimeException("Only available outside of the enclave to avoid the need to synchronize the memory management.");
		}
		
		System.out.println("malloc bytes: " + bytes);
		int blocksNeeded = blocksNeeded(bytes);
		System.out.println("blocks needed: " + blocksNeeded);
		
		if (freeBlocks < blocksNeeded) {
			throw new OutOfMemoryError("The requested amount of memory is not available (" + bytes + "  Bytes)");
		}
		
		int startBlock = findConsecutiveFreeBlocks(blocksNeeded);
		if (startBlock == -1) {
			throw new OutOfMemoryError("The requested amount of memory is not available (" + bytes + " Bytes)");
		}
		
		markUsed(startBlock, blocksNeeded);
		freeBlocks -= blocksNeeded;
		
		return new MallocedMappedDataBuffer(buffer.address() + (startBlock * blockSize), blocksNeeded * blockSize);
	}
	
	public void free(MallocedMappedDataBuffer b) {
		if (SgxSettings.IS_ENCLAVE()) {
			throw new RuntimeException("Only available outside of the enclave to avoid the need to synchronize the memory management.");
		}
		
		int startBlock = (int) ((b.address() - buffer.address()) / blockSize);
		int blocksNeeded = blocksNeeded(b.capacity());
		
		freeBlocks += blocksNeeded;
		
		inUse[startBlock] = -blocksNeeded;
	
		int firstFree = findFirstConsecutiveFree(startBlock);
		int lastFree = findLastConsecutiveFree(startBlock);		
		int totalFree = lastFree - firstFree + 1;
		
		markFree(firstFree, totalFree);
	}
	
	public void register(MallocedMappedDataBuffer b) {
		
	}
	
	private int findFirstConsecutiveFree(int startBlock) {
		// The runtime can be improved here. This can be more clever
		while (startBlock >= 0 && inUse[startBlock] < 0) {
			startBlock--;
		}
		
		// We end up before the array or at the first block that is in use
		return startBlock + 1;
	}
	
	private int findLastConsecutiveFree(int startBlock) {
		while (startBlock < noBlocks && inUse[startBlock] < 0) {
			startBlock -= inUse[startBlock];
		}
		
		// We end up after the array or at the first block that is in use
		return startBlock - 1;
	}
	
	private int findConsecutiveFreeBlocks(int blocks) {
		int i = 0;
		int startBlock = -1;
		while (startBlock == -1 && i < noBlocks) {
			int val = inUse[i];
			if (val > 0) {
				// Found a block in use, skip all
				i += val;
			} else if (val < 0) {
				int free = -val;
				if (blocks <= free) {
					// Found free block that is large enough
					startBlock = i;
				} else {
					// Found a free block that is not large enough, skip all
					i += free;
				}
			}
		}
		
		return startBlock;
	}
	
	long startAddress() {
		return buffer.address();
	}
}
