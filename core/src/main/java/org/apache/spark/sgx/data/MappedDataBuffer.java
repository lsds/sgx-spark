package org.apache.spark.sgx.data;

import java.nio.ByteOrder;

import org.apache.spark.sgx.utils.Bits;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class MappedDataBuffer implements IDataBuffer {
	
	protected static final Unsafe unsafe = Bits.unsafe();
	
	protected static final boolean unaligned = Bits.unaligned();
	
	private boolean bigEndian;
	private boolean nativeByteOrder = (Bits.byteOrder() == ByteOrder.BIG_ENDIAN);
	
	private long address;
	private int capacity;
	
	public MappedDataBuffer (long address, int capacity) {
		this.address = address;
		this.capacity = capacity;
	}
	
	public final ByteOrder order () {
		return (bigEndian) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
	}
	
	public final void order (ByteOrder bo) {
		bigEndian = (bo == ByteOrder.BIG_ENDIAN);
		nativeByteOrder = (bigEndian == (Bits.byteOrder() == ByteOrder.BIG_ENDIAN));
	}
	
	public long address () {
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
	
	public byte get (int index) {
		return ((unsafe.getByte(ix(checkIndex(index)))));
	}
	
	private int getInt (long a) {
		if (unaligned) {
			int x = unsafe.getInt (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getInt(a, bigEndian);
	}
	
	public int getInt (int index) {
		return getInt(ix(checkIndex(index, (1 << 2))));
	}
	
	private long getLong (long a) {
		if (unaligned) {
			long x = unsafe.getLong (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getLong (a, bigEndian);
	}
	
	public long getLong (int index) {
		return getLong(ix(checkIndex(index, (1 << 3))));
	}
	
	public void put (int index, byte value) {
		unsafe.putByte(ix(checkIndex(index)), ((value)));
	}
	
	public void put (int index, byte[] value) {
		int length = value.length;
		unsafe.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET + 0 * Unsafe.ARRAY_BYTE_INDEX_SCALE, null, ix(checkIndex(index, length)), length);
	}
	
	public byte[] get(int index, byte[] value) {
		int length = value.length;
		unsafe.copyMemory(null, ix(checkIndex(index, length)), value, Unsafe.ARRAY_BYTE_BASE_OFFSET + 0 * Unsafe.ARRAY_BYTE_INDEX_SCALE, length);
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
	
	public void putInt (int index, int value) {
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
    
//    public void put(byte[] src, int offset, int length) {
//        checkBounds(offset, length, src.length);
//        if (length > remaining())
//            throw new BufferOverflowException();
//        int end = offset + length;
//        for (int i = offset; i < end; i++)
//            this.put(src[i]);
//    }
	
	public void putLong (int index, long value) {
		putLong(ix(checkIndex(index, (1 << 3))), value);
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address + ", capacity=" + capacity + ")";
	}

	@Override
	public int capacity() {
		return capacity;
	}
}
