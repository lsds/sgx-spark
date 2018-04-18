package org.apache.spark.sgx.data;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

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
		Logger.getLogger("debug").info("Creating " + this);
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
	
	public boolean isMapped () {
		return true;
	}
	
	public ByteBuffer getByteBuffer () {
		throw new IllegalStateException ("error: buffer is mapped");
	}
	
	public long getSize ()  {
		return capacity;
	}
	
	@Override
	public byte get (int offset) {
		return ((unsafe.getByte(ix(checkIndex(offset)))));
	}
	
	private int getInt (long a) {
		if (unaligned) {
			int x = unsafe.getInt (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getInt(a, bigEndian);
	}
	
	@Override
	public int getInt (int offset) {
		return getInt(ix(checkIndex(offset, (1 << 2))));
	}
	
	private float getFloat (long a) {
		if (unaligned) {
			int x = unsafe.getInt (a);
			return Float.intBitsToFloat(nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getFloat(a, bigEndian);
	}
	
	@Override
	public float getFloat (int offset) {
		return getFloat(ix(checkIndex(offset, (1 << 2))));
	}
	
	private long getLong (long a) {
		if (unaligned) {
			long x = unsafe.getLong (a);
			return (nativeByteOrder ? x : Bits.swap(x));
		}
		return Bits.getLong (a, bigEndian);
	}
	
	@Override
	public long getLong (int offset) {
		return getLong(ix(checkIndex(offset, (1 << 3))));
	}
	
	@Override
	public int limit () {
		return capacity;
	}
	
	@Override
	public int position () {
		return 0;
	}

	@Override
	public int capacity () {
		return capacity;
	}
	
	@Override
	public boolean isDirect () {
		return true;
	}
	
	@Override
	public boolean isFinalised () {
		return true;
	}

	@Override
	public void reset () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}

	@Override
	public void clear () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}
	
	@Override
	public byte [] array () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}

	@Override
	public void finalise(int index) {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}
	
	@Override
	public float computeChecksum () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}
	
	@Override
	public void free () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}
	
	@Override
	public int referenceCountGet () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}

	@Override
	public int referenceCountGetAndIncrement () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}

	@Override
	public int referenceCountDecrementAndGet () {
		throw new UnsupportedOperationException ("error: unsupported operation on mapped data buffers");
	}
	
	@Override
	public void put (int index, byte value) {
		unsafe.putByte(ix(checkIndex(index)), ((value)));
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
	
	@Override
	public void putInt (int index, int value) {
		putInt(ix(checkIndex(index, (1 << 2))), value);
	}

    private void putFloat(long a, float x) {
        if (unaligned) {
            int y = Float.floatToRawIntBits(x);
            unsafe.putInt(a, (nativeByteOrder ? y : Bits.swap(y)));
        } else {
            Bits.putFloat(a, x, bigEndian);
        }
    }	

	@Override
	public void putFloat(int index, float value) {
        putFloat(ix(checkIndex(index, (1 << 2))), value);
	}

    private void putLong(long a, long x) {
        if (unaligned) {
            long y = (x);
            unsafe.putLong(a, (nativeByteOrder ? y : Bits.swap(y)));
        } else {
            Bits.putLong(a, x, bigEndian);
        }
    }
	
	@Override
	public void putLong (int index, long value) {
		putLong(ix(checkIndex(index, (1 << 3))), value);
	}
	
	@Override
	public void put (IDataBuffer buffer) {
		throw new UnsupportedOperationException ("error: unsupported operation");
	}

	@Override
	public void put (IDataBuffer buffer, int offset, int length, boolean resetPosition) {
		throw new UnsupportedOperationException ("error: unsupported operation");
	}

	@Override
	public void bzero () {
		throw new UnsupportedOperationException ("error: unsupported operation");
	}
	
	@Override
	public void bzero (int offset, int length) {
		throw new UnsupportedOperationException ("error: unsupported operation");
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(address=" + address + ", capacity=" + capacity + ")";
	}
}
