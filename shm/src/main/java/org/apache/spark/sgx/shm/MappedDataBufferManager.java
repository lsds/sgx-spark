package org.apache.spark.sgx.shm;

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
	
	private MappedDataBufferManager(MappedDataBuffer buffer) {
		this.buffer = buffer;
		this.noBlocks = buffer.capacity() / blockSize;
		this.inUse = new int[noBlocks];
		for (int i = 0; i < noBlocks; i++) {
			inUse[i] = (-1 * noBlocks) + i;
		}
		
		freeBlocks = noBlocks;
	}
	
	private int blocksNeeded(int bytes) {
		int r = bytes % blockSize;
		return ((r == 0) ? bytes : bytes + (blockSize - r)) / blockSize;
	}
	
	public MappedDataBuffer malloc(int bytes) {
		int blocksNeeded = blocksNeeded(bytes);
		
		if (freeBlocks < blocksNeeded) {
			throw new OutOfMemoryError("The requested amount of memory is not available (" + bytes + "  Bytes)");
		}
		
		int startBlock = findConsecutiveFreeBlocks(blocksNeeded);
		if (startBlock == -1) {
			throw new OutOfMemoryError("The requested amount of memory is not available (" + bytes + " Bytes)");
		}
		
		for (int i = 0; i < blocksNeeded; i++) {
			inUse[startBlock + i] = blocksNeeded - i;
		}
		freeBlocks -= blocksNeeded;
		
		return new MappedDataBuffer(buffer.address() + (startBlock * blockSize), blocksNeeded * blockSize);
	}
	
	public void free(MappedDataBuffer b) {
		int startBlock = (int) ((b.address() - buffer.address()) / blockSize);
		int blocksNeeded = blocksNeeded(b.capacity());
		
		freeBlocks += blocksNeeded;
		
		inUse[startBlock] = -blocksNeeded;
	
		int firstFree = findFirstConsecutiveFree(startBlock);
		int lastFree = findLastConsecutiveFree(startBlock);		
		int totalFree = lastFree - firstFree + 1;
		
		for (int i = 0; i < totalFree; i++) {
			inUse[firstFree + i] = (-1 * totalFree) + i;
		}
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
}
