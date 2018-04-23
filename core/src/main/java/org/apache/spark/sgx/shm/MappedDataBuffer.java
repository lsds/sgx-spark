package org.apache.spark.sgx.shm;

import java.nio.ByteOrder;

import org.apache.spark.sgx.utils.Bits;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
class MappedDataBuffer {
	
	protected static final Unsafe unsafe = Bits.unsafe();
	
	protected static final boolean unaligned = Bits.unaligned();
	
	private boolean bigEndian;
	private boolean nativeByteOrder = (Bits.byteOrder() == ByteOrder.BIG_ENDIAN);
	
	private final long address;
	private final int capacity;
	
	public MappedDataBuffer (long address, int capacity) {
		this.address = address;
		this.capacity = capacity;
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
		return (address + (offset << 0));
	}
	
	private int checkIndex (int index) {
		if ((index < 0) || (index > (capacity - 1)))
			throw new IndexOutOfBoundsException ();
		return index;
	}
	
	private int checkIndex (int index, int bound) {
		if ((index < 0) || (bound > (capacity - index)))
			throw new IndexOutOfBoundsException ();
		return index;
	}
	
	byte get (int index) {
		return ((unsafe.getByte(ix(checkIndex(index)))));
	}
	
	private int getInt (long a) {
		if (unaligned) {
			int x = unsafe.getInt (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getInt(a, bigEndian);
	}
	
	int getInt (int index) {
		return getInt(ix(checkIndex(index, (1 << 2))));
	}
	
	private long getLong (long a) {
		if (unaligned) {
			long x = unsafe.getLong (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getLong (a, bigEndian);
	}
	
	long getLong (int index) {
		return getLong(ix(checkIndex(index, (1 << 3))));
	}
	
	void put (int index, byte value) {
		unsafe.putByte(ix(checkIndex(index)), ((value)));
	}
	
	void put (int index, byte[] value) {
		put(index, value, 0, value.length);
	}
	
	void put (int index, byte[] value, int from, int length) {
		if (from < 0 || from >= value.length || from + length >= value.length ) {
			throw new RuntimeException("Invalid index: from=" + from + ", length=" + length + " for array of size " + value.length + ".");
		}
		unsafe.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET + from * Unsafe.ARRAY_BYTE_INDEX_SCALE, null, ix(checkIndex(index, from + length)), length);
	}
	
	byte[] get(int index, byte[] value) {
		return get(index, value, 0, value.length);
	}
	
	byte[] get(int index, byte[] value, int from, int length) {
		unsafe.copyMemory(null, ix(checkIndex(index, length)), value, Unsafe.ARRAY_BYTE_BASE_OFFSET + from * Unsafe.ARRAY_BYTE_INDEX_SCALE, length);
		return value;
	}

    static void checkBounds(int off, int len, int size) {
        if ((off | len | (off + len) | (size - (off + len))) < 0)
            throw new IndexOutOfBoundsException();
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
		putInt(ix(checkIndex(index, (1 << 2))), value);
	}

    private void putLong(long a, long x) {
        if (unaligned) {
            long y = (x);
            unsafe.putLong(a, (nativeByteOrder ? y : Bits.swap(y)));
        } else {
            Bits.putLong(a, x, bigEndian);
        }
    }
	
	void putLong (int index, long value) {
		putLong(ix(checkIndex(index, (1 << 3))), value);
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address + ", capacity=" + capacity + ")";
	}

	int capacity() {
		return capacity;
	}
}
