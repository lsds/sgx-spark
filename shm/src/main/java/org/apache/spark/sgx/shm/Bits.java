package org.apache.spark.sgx.shm;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;

import sun.misc.Unsafe;

/*
 * Exposing java.nio.Bits functions to Crossbow 
 * to manage MappedDataBuffer objects
 */
@SuppressWarnings("restriction")
public class Bits {

	private static final Unsafe unsafe;

	private static final ByteOrder byteOrder;
	
	private static boolean unaligned;
	private static boolean unalignedKnown = false;

	private Bits () {

	}
	
	static {
		
		try {
			
			Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			unsafe = (Unsafe) theUnsafe.get (null);
		} 
		catch (Exception e) {
			throw new AssertionError(e);
		}
	}
	
	public static Unsafe unsafe() {
		return unsafe;
	}
	
	public static boolean unaligned () {

		if (unalignedKnown)
			return unaligned;

		String arch = AccessController.doPrivileged 
				(new sun.security.action.GetPropertyAction("os.arch"));

		unaligned = arch.equals("i386") || arch.equals("x86") || arch.equals("amd64");
		unalignedKnown = true;

		return unaligned;
	}

	public static ByteOrder byteOrder () {
		
		if (byteOrder == null)
			throw new IllegalStateException ();
		return byteOrder;
	}
	
	static {
		long a = unsafe.allocateMemory(8);
		try {
			unsafe.putLong(a, 0x0102030405060708L);
			byte b = unsafe.getByte(a);
			switch (b) {
			case 0x01: byteOrder = ByteOrder.BIG_ENDIAN;     break;
			case 0x08: byteOrder = ByteOrder.LITTLE_ENDIAN;  break;
			default:
				byteOrder = null;
			}
		} finally {
			unsafe.freeMemory(a);
		}
	}

	public static int swap (int x) {
		
		return ((x << 24) | ((x & 0x0000ff00) <<  8) | ((x & 0x00ff0000) >>> 8) | (x >>> 24));
	}
	
	public static long swap (long x) {
		
		return (((long) swap((int) x) << 32) | ((long) swap((int) (x >>> 32)) & 0xffffffffL));
	}

	private static byte _get(long address) {
		
		return unsafe.getByte(address);
	}

	private static int makeInt (byte b3, byte b2, byte b1, byte b0) {
		
		return (((b3 & 0xff) << 24) |
				((b2 & 0xff) << 16) |
				((b1 & 0xff) <<  8) |
				((b0 & 0xff) <<  0));
	}

	static int getIntL(long address) {
		
		return makeInt(
				_get(address + 3),
				_get(address + 2),
				_get(address + 1),
				_get(address + 0));
	}
	
	static int getIntB(long address) {
		
		return makeInt(
				_get(address + 0),
				_get(address + 1),
				_get(address + 2),
				_get(address + 3));
	}
	
	public static int getInt (long address, boolean bigEndian) {

		return (bigEndian ? getIntB (address) : getIntL (address));
	}
	
    private static byte long7(long x) { return (byte)(x >> 56); }
    private static byte long6(long x) { return (byte)(x >> 48); }
    private static byte long5(long x) { return (byte)(x >> 40); }
    private static byte long4(long x) { return (byte)(x >> 32); }
    private static byte long3(long x) { return (byte)(x >> 24); }
    private static byte long2(long x) { return (byte)(x >> 16); }
    private static byte long1(long x) { return (byte)(x >>  8); }
    private static byte long0(long x) { return (byte)(x      ); }	

	static private long makeLong (
			byte b7, byte b6, byte b5, byte b4,
			byte b3, byte b2, byte b1, byte b0
			) {
		
		return ((((long) b7 & 0xff) << 56) |
				(((long) b6 & 0xff) << 48) |
				(((long) b5 & 0xff) << 40) |
				(((long) b4 & 0xff) << 32) |
				(((long) b3 & 0xff) << 24) |
				(((long) b2 & 0xff) << 16) |
				(((long) b1 & 0xff) <<  8) |
				(((long) b0 & 0xff) <<  0));
	}

	static long getLongL(long address) {
		
		return makeLong(
				_get(address + 7),
				_get(address + 6),
				_get(address + 5),
				_get(address + 4),
				_get(address + 3),
				_get(address + 2),
				_get(address + 1),
				_get(address + 0));
	}

	static long getLongB(long address) {
		
		return makeLong(
				_get(address + 0),
				_get(address + 1),
				_get(address + 2),
				_get(address + 3),
				_get(address + 4),
				_get(address + 5),
				_get(address + 6),
				_get(address + 7));
	}
	
	public static long getLong (long address, boolean bigEndian) {

		return (bigEndian ? getLongB (address) : getLongL (address));
	}

	static float getFloatL (long address) {
		
		return Float.intBitsToFloat(getIntL(address));
	}

	static float getFloatB (long address) {
		
		return Float.intBitsToFloat(getIntB(address));
	}


	public static float getFloat (long address, boolean bigEndian) {
		
		return (bigEndian ? getFloatB(address) : getFloatL(address));
	}
	
    static void putLongL(long a, long x) {
        _put(a + 7, long7(x));
        _put(a + 6, long6(x));
        _put(a + 5, long5(x));
        _put(a + 4, long4(x));
        _put(a + 3, long3(x));
        _put(a + 2, long2(x));
        _put(a + 1, long1(x));
        _put(a    , long0(x));
    }
	
    static void putLongB(long a, long x) {
        _put(a    , long7(x));
        _put(a + 1, long6(x));
        _put(a + 2, long5(x));
        _put(a + 3, long4(x));
        _put(a + 4, long3(x));
        _put(a + 5, long2(x));
        _put(a + 6, long1(x));
        _put(a + 7, long0(x));
    }	
	
    public static void putLong(long a, long x, boolean bigEndian) {
        if (bigEndian)
            putLongB(a, x);
        else
            putLongL(a, x);
    }	

    private static void _put(long a, byte b) {
        unsafe.putByte(a, b);
    }
    
    private static byte int3(int x) { return (byte)(x >> 24); }
    private static byte int2(int x) { return (byte)(x >> 16); }
    private static byte int1(int x) { return (byte)(x >>  8); }
    private static byte int0(int x) { return (byte)(x      ); }    
    
    static void putIntB(long a, int x) {
        _put(a    , int3(x));
        _put(a + 1, int2(x));
        _put(a + 2, int1(x));
        _put(a + 3, int0(x));
    }
    
    static void putIntL(long a, int x) {
        _put(a + 3, int3(x));
        _put(a + 2, int2(x));
        _put(a + 1, int1(x));
        _put(a    , int0(x));
    }
    
    public static void putInt(long a, int x, boolean bigEndian) {
        if (bigEndian)
            putIntB(a, x);
        else
            putIntL(a, x);
    }

    static void putFloatL(long a, float x) {
        putIntL(a, Float.floatToRawIntBits(x));
    }

    static void putFloatB(long a, float x) {
        putIntB(a, Float.floatToRawIntBits(x));
    }  
    
    public static void putFloat(long a, float x, boolean bigEndian) {
        if (bigEndian)
            putFloatB(a, x);
        else
            putFloatL(a, x);
    }
}
