package org.apache.spark.sgx.shm;

import java.nio.ByteOrder;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class MappedDataBuffer {
	
	protected static final Unsafe unsafe = Bits.unsafe();
	
	protected static final boolean unaligned = Bits.unaligned();
	
	private boolean bigEndian;
	private boolean nativeByteOrder = (Bits.byteOrder() == ByteOrder.BIG_ENDIAN);
	
	private final long address;
	private final int capacity;

	private static boolean checkIndexes = false;  // Set to true to enable runtime index checks
	
	public MappedDataBuffer (long address, int capacity) {
		this.address = address;
		this.capacity = capacity;

/*
		try {
			throw new Exception("Create a new MappedDataBuffer of size " + capacity + "@" + address);
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
	}
	
	final ByteOrder order () {
		return (bigEndian) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
	}
	
	final void order (ByteOrder bo) {
		bigEndian = (bo == ByteOrder.BIG_ENDIAN);
		nativeByteOrder = (bigEndian == (Bits.byteOrder() == ByteOrder.BIG_ENDIAN));
	}
	
	long address () {
		return address;
	}

	private long ix (int offset) {
		return (address + offset);
	}
	
	private int checkIndex (int index, int bound) {
		if ((index < 0) || (bound > (capacity - index)))
			throw new IndexOutOfBoundsException ();
		return index;
	}

	private int getInt (long a) {
		if (unaligned) {
			int x = unsafe.getInt (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getInt(a, bigEndian);
	}
	
	int getInt (int index) {
		if (checkIndexes) {
			checkIndex(index, 4 /* size of int */);
		}
		return getInt(ix(index));
	}
	
	private long getLong (long a) {
		if (unaligned) {
			long x = unsafe.getLong (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getLong (a, bigEndian);
	}
	
	long getLong(int index) {
		if (checkIndexes) {
			checkIndex(index, 8 /* size of long */);
		}
		return getLong(ix(index));
	}

	public void put(int index, byte[] value) {
		put(index, value, 0, value.length);
	}

	public void put(int index, byte[] value, int from, int length) {
		if (checkIndexes) {
			if (from < 0 || from >= value.length || from + length > value.length ) {
				throw new RuntimeException("Invalid index: from=" + from + ", length=" + length + " for array of size " + value.length + ".");
			}
			checkIndex(index, from + length);
		}
		unsafe.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET + from * Unsafe.ARRAY_BYTE_INDEX_SCALE, null, ix(index), length);
	}

	public byte[] get(int index, byte[] value) {
		return get(index, value, 0, value.length);
	}

	public byte[] get(int index, byte[] value, int from, int length) {
		if (checkIndexes) {
			checkIndex(index, length);
		}
		unsafe.copyMemory(null, ix(index), value, Unsafe.ARRAY_BYTE_BASE_OFFSET + from * Unsafe.ARRAY_BYTE_INDEX_SCALE, length);
		return value;
	}

	private void putInt(long a, int x) {
		if (unaligned) {
			int y = (x);
			unsafe.putInt(a, (nativeByteOrder ? y : Bits.swap(y)));
		} else {
			Bits.putInt(a, x, bigEndian);
		}
	}
	
	void putInt (int index, int value) {
		if (checkIndexes) {
			checkIndex(index, 4 /* size of int */);
		}
		putInt(ix(index), value);
	}

	private void putLong(long a, long x) {
		if (unaligned) {
			long y = (x);
			unsafe.putLong(a, (nativeByteOrder ? y : Bits.swap(y)));
		} else {
			Bits.putLong(a, x, bigEndian);
		}
	}
	
	void putLong(int index, long value) {
		if (checkIndexes) {
			checkIndex(index, 8 /* size of long */);
		}
		putLong(ix(index), value);
	}

	public void memset(long a, long b, byte v) {
		unsafe.setMemory(a, b, v);
	}

	public void zero(int index, int numBytes) {
		if (checkIndexes) {
			checkIndex(index, numBytes);
		}
		memset(ix(index), numBytes, (byte)0);
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address + ", capacity=" + capacity + ")";
	}

	public int capacity() {
		return capacity;
	}
}