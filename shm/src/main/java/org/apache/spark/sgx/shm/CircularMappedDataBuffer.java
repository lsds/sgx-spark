package org.apache.spark.sgx.shm;

public class CircularMappedDataBuffer {
	
	private AlignedMappedDataBuffer dataBuffer;
	private AlignedMappedDataBuffer metaBuffer;
	
	private final int META_READ_SLOT = 0;
	private final int META_WRITE_SLOT = 1;
	
	// Exponential backoff wait times (milliseconds)
	private static int MAX_WAIT = 16;
	private static int MIN_WAIT = 1;
	
	/**
	 * A circular mapped data buffer backed by an {@link AlignedMappedDataBuffer}.
	 * In addition, an additional meta buffer is required to store/share the current
	 * read/write pointers to the actual data buffer.
	 * 
	 * @param dataBuffer
	 * @param metaBuffer
	 */
	public CircularMappedDataBuffer(AlignedMappedDataBuffer dataBuffer, AlignedMappedDataBuffer metaBuffer) {
		if (metaBuffer.slots() < 2) {
			throw new RuntimeException("Meta buffer too small. Must be able to accomodate at least two values.");
		}
		this.dataBuffer = dataBuffer;
		this.metaBuffer = metaBuffer;
	}
	
	private int wrap(int slot) {
		return slot % dataBuffer.slots();
	}
	
	private void setReadSlot(int slot) {
		metaBuffer.putInt(META_READ_SLOT, wrap(slot));
	}
	
	private void setWriteSlot(int slot) {
		metaBuffer.putInt(META_WRITE_SLOT, wrap(slot));
	}
	
	private int getReadSlot() {
		return metaBuffer.getInt(META_READ_SLOT);
	}
	
	private int getWriteSlot() {
		return metaBuffer.getInt(META_WRITE_SLOT);
	}
	
	private byte[] tryGet() {
		int writeSlot = getWriteSlot();
		int readSlot = getReadSlot();
		
		int length = dataBuffer.getInt(readSlot);
		
		System.out.println("Trying to get at readSlot=" + readSlot + " (writeSlot=" + writeSlot + "). Length field = " + dataBuffer.getInt(readSlot));
		
		if (length == 0) {
			// There is nothing to read
			return null;
		}
		
		// We now know that there is something to read of length length
		byte[] bytes = new byte[length];
		
		int dataSlots = dataBuffer.slotsNeeded(length);
		
		if (readSlot >= writeSlot) {
			// Reader is ahead of writer or at the same slot (and there _is_ something to read)
			// Indices: 0.......W.......R.......S
			if (readSlot + dataSlots <= dataBuffer.slots()) {
				// We can read in one go
				readDataFully(readSlot + 1, bytes);
			} else {
				// We need to read in parts
				int slotsBeforeWrap = dataBuffer.slots() - readSlot;
				readDataInParts(readSlot + 1, bytes, slotsBeforeWrap);
			}
		} else {
			// Writer is ahead of reader. Since we know that there is
			// something to read, we can read all in one go.
			// Indices: 0.......R.......W.......S
			readDataFully(readSlot + 1, bytes);
		}
		
		writeSize(readSlot, 0);
		setReadSlot(readSlot + dataSlots + 1);
		return bytes;
	}
	
	public byte[] get() throws InterruptedException {
		byte[] result = null;
		int wait = MIN_WAIT;
		
		while ((result = tryGet()) == null) {
			Thread.sleep(wait);			
			wait = Math.min(wait << 1, MAX_WAIT);
		}
		
		return result;
	}

	private boolean tryPut(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			throw new NullPointerException("Null or zero-length values are not supported by " + this.getClass().getName());
		}
		
		// We always need one more slot to write the header, which, most importantly,
		// includes the size
		int needSlots = dataBuffer.slotsNeeded(bytes.length) + 1;
		
		// Check if the data can actually fit into the provided memory
		if (needSlots > dataBuffer.slots()) {
			throw new RuntimeException("The provided byte array does not fit into the underlying memory buffer (" + bytes.length + " Bytes is too large).");
		}
		
		int writeSlot = getWriteSlot();
		int readSlot = getReadSlot();
		
		System.out.println("Trying to put " + bytes.length + " at slot writeSlot=" + writeSlot + " (readSlot=" + readSlot + ")");
		
		if (writeSlot > readSlot || (writeSlot == readSlot && dataBuffer.getInt(readSlot) == 0)) {
			// (i) Writer is ahead of reader; or
			// (ii) writer and reader are at the same slot, but there is nothing to read.
			// Indices: 0.......R.......W.......S // with S = slots()
			if (writeSlot + needSlots < dataBuffer.slots()) {
				// Enough space, write everything at once
				//    Data: ................LDDDD...; L = length
				// Indices: 0.......R.......W.......S
				writeDataFully(writeSlot + 1, bytes);
			} else {
				// Not enough space until end of buffer
				int slotsBeforeWrap = dataBuffer.slots() - writeSlot;
				int slotsAfterWrap = readSlot;
				if (slotsBeforeWrap + slotsAfterWrap >= needSlots) {
					// But, there is enough space overall.
					// We can write in two parts:
					//    Data: DDDD............LDDDDDDD; L = length
					// Indices: 0.......R.......W.......S
					writeDataInParts(writeSlot + 1, bytes, slotsBeforeWrap);
				}  else {
					// There is currently not enough space to write
					return false;
				}
			}
			
			writeSize(writeSlot, bytes.length);
			setWriteSlot(writeSlot + needSlots);
			return true;
			
		} else if (readSlot > writeSlot) {
			// Reader is ahead of writer.
			// This means that we can only write if the reader is far enough from the writer.
			// Indices: 0.......W.......R.......S
			if (readSlot - writeSlot >= needSlots) {
				// Enough space to write fully
				writeDataFully(writeSlot + 1, bytes);
				writeSize(writeSlot, bytes.length);
				setWriteSlot(writeSlot + needSlots);
			} else {
				// Not enough space to write
				return false;
			}
			
		} else if (readSlot == writeSlot && dataBuffer.getInt(readSlot) != 0) {
			// Reader and writer are at the same slot, and there is something to be read.
			// Hence, we are unable to write.
			return false;
		} else {
			throw new RuntimeException("Unconsidered case.");
		}
		
		return false;
	}
	
	public void put(byte[] bytes) throws InterruptedException {
		int wait = MIN_WAIT;
		while (!tryPut(bytes)) {
			Thread.sleep(wait);			
			wait = Math.min(wait << 1, MAX_WAIT);
		}
	}
	
	private void writeSize(int slot, int size) {
		dataBuffer.putInt(slot, size);
	}
	
	private byte[] readDataFully(int slot, byte[] bytes) {
		return dataBuffer.getBytes(slot, bytes);
	}
	
	private byte[] readDataInParts(int slot, byte[] bytes, int slotsBeforeWrap) {
		int wrapAfter = dataBuffer.slotSize() * slotsBeforeWrap;
		dataBuffer.getBytes(slot, bytes, 0, wrapAfter);
		dataBuffer.getBytes(0, bytes, wrapAfter, bytes.length - wrapAfter);
		return bytes;
	}
	
	private void writeDataFully(int slot, byte[] bytes) {
		dataBuffer.putBytes(slot, bytes);
	}
	
	private void writeDataInParts(int slot, byte[] bytes, int slotsBeforeWrap) {
		int wrapAfter = dataBuffer.slotSize() * slotsBeforeWrap;
		dataBuffer.putBytes(slot, bytes, 0, wrapAfter);
		dataBuffer.putBytes(0, bytes, wrapAfter, bytes.length - wrapAfter);
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(dataBuffer=" + dataBuffer + ", metaBuffer=" + metaBuffer + ")";
	}
}
