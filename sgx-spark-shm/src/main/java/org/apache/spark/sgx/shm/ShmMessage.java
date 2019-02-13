package org.apache.spark.sgx.shm;

import java.io.*;

public class ShmMessage implements Serializable {
	private static final long serialVersionUID = 7329847091123L;

	private EShmMessageType type;
	private Object msg;
	private long port;
	public byte msgtype;  // 0: object, 1: Boolean, 2: Integer, 3: Long, 4: Double
	public static final boolean SMALL_MESSAGE_INLINE_OPTIMIZATION = false;

	ShmMessage(byte[] h, Object o) {
		int pos = 0;
		msgtype = ShmMessage.readByte(h, pos); pos += Byte.BYTES;
		type = ShmMessage.intToType(ShmMessage.readInt(h, pos)); pos += Integer.BYTES;
		port = ShmMessage.readLong(h, pos); pos += Long.BYTES;

		switch (msgtype) {
			case 0:
				msg = o;
				break;
			case 1:
				msg = (ShmMessage.readByte(h, pos) == (byte)1);
				break;
			case 2:
				msg = ShmMessage.readInt(h, pos);
				break;
			case 3:
				msg = ShmMessage.readLong(h, pos);
				break;
			case 4:
				msg = ShmMessage.readDouble(h, pos);
				break;
			default:
				break;
		}
	}

	ShmMessage(EShmMessageType type, Object msg, long port) {
		this.type = type;
		this.msg = msg;
		this.port = port;

		if (msg.getClass() == Boolean.class) {
			msgtype = 1;
		} else if (msg.getClass() == Integer.class) {
			msgtype = 2;
		} else if (msg.getClass() == Long.class) {
			msgtype = 3;
		} else if (msg.getClass() == Double.class) {
			msgtype = 4;
		} else {
			msgtype = 0;
		}

		//if (type == EShmMessageType.REGULAR) System.err.println("Creating new ShmMessageType with object " + msg.getClass().getSimpleName());

		/* all the types I've observed with number of occurrences in kmeans
			  2 Creating new ShmMessageType with object Boolean
			180 Creating new ShmMessageType with object Integer
			126 Creating new ShmMessageType with object Long
			  6 Creating new ShmMessageType with object Double

			 33 Creating new ShmMessageType with object BoxedUnit
			  2 Creating new ShmMessageType with object ArrayBuffer
			  4 Creating new ShmMessageType with object ArraySeq
			 11 Creating new ShmMessageType with object Collect
			  8 Creating new ShmMessageType with object $colon$colon
			  8 Creating new ShmMessageType with object CombineByKeyWithClassTag
			328 Creating new ShmMessageType with object ComputeMapPartitionsRDD
			  2 Creating new ShmMessageType with object ComputePartitionwiseSampledRDD
			 60 Creating new ShmMessageType with object ComputeZippedPartitionsRDD2
			  1 Creating new ShmMessageType with object Count
			 16 Creating new ShmMessageType with object ExternalAppendOnlyMapInsertAll
			 16 Creating new ShmMessageType with object ExternalAppendOnlyMapIterator
			 16 Creating new ShmMessageType with object ExternalSorterInsertAllCombine
			  3 Creating new ShmMessageType with object Fold
			  9 Creating new ShmMessageType with object Map
			  7 Creating new ShmMessageType with object MapPartitions
			  2 Creating new ShmMessageType with object MapPartitionsWithIndex
			  8 Creating new ShmMessageType with object MapValues
			 11 Creating new ShmMessageType with object MsgBroadcastDestroy
			  2 Creating new ShmMessageType with object MsgBroadcastUnpersist
			 30 Creating new ShmMessageType with object MsgBroadcastValue
			 16 Creating new ShmMessageType with object MsgIteratorReqClose$
			 25 Creating new ShmMessageType with object MsgIteratorReqNextN
			 25 Creating new ShmMessageType with object NoEncryption
			 16 Creating new ShmMessageType with object PartitionedAppendOnlyMapCreate
			 16 Creating new ShmMessageType with object PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator
			  4 Creating new ShmMessageType with object Persist
			 30 Creating new ShmMessageType with object ResultTaskRunTask
			  1 Creating new ShmMessageType with object Sample
			  1 Creating new ShmMessageType with object SgxBroadcastProviderIdentifier
			406 Creating new ShmMessageType with object SgxFakeIterator
			  1 Creating new ShmMessageType with object SgxFct0
			 90 Creating new ShmMessageType with object SgxShmIteratorConsumerClose
			180 Creating new ShmMessageType with object SgxShmIteratorConsumerFillBufferMsg
			 11 Creating new ShmMessageType with object SgxSparkContextBroadcast
			  1 Creating new ShmMessageType with object SgxSparkContextConf
			  1 Creating new ShmMessageType with object SgxSparkContextDefaultParallelism
			  1 Creating new ShmMessageType with object SgxSparkContextStop
			  1 Creating new ShmMessageType with object SgxSparkContextTextFile
			  7 Creating new ShmMessageType with object SgxTaskAccumulatorRegister
			  1 Creating new ShmMessageType with object SgxTaskSparkContextCreate
			 16 Creating new ShmMessageType with object SgxWritablePartitionedIteratorProviderIdentifier
			 16 Creating new ShmMessageType with object SizeTrackingAppendOnlyMapCreate
			 32 Creating new ShmMessageType with object SizeTrackingAppendOnlyMapIdentifier
			 16 Creating new ShmMessageType with object Tuple2[]
			  4 Creating new ShmMessageType with object Unpersist
			 22 Creating new ShmMessageType with object VectorWithNorm[]
			  5 Creating new ShmMessageType with object Zip
		 */
	}

	EShmMessageType getType() {
		return type;
	}

	public Object getMsg() {
		return msg;
	}

	long getPort() {
		return port;
	}

	public String toString() {
		return getClass().getSimpleName() + "(type=" + type + ", msg=" + msg + ", port=" + port + ")";
	}

	private static int typeToInt(EShmMessageType t) {
		switch (t) {
			case NEW_CONNECTION: return 0;
			case CLOSE_CONNECTION: return 1;
			case ACCEPTED_CONNECTION: return 2;
			case REGULAR: return 3;
			default: return -1;
		}
	}

	private static EShmMessageType intToType(int i) {
		switch (i) {
			case 0: return EShmMessageType.NEW_CONNECTION;
			case 1: return EShmMessageType.CLOSE_CONNECTION;
			case 2: return EShmMessageType.ACCEPTED_CONNECTION;
			case 3: return EShmMessageType.REGULAR;
			default: System.out.println("Invalid int to EShmMessageType: " + i); System.exit(-1);
		}
		return null;
	}

	private static int writeByte(byte c, byte[] b, int pos) {
		b[pos] = c;
		return pos+1;
	}

	private static int writeInt(int i, byte[] b, int pos) {
		b[pos] = (byte)(i >> 24);
		b[pos+1] = (byte)(i >> 16);
		b[pos+2] = (byte)(i >> 8);
		b[pos+3] = (byte)i;
		return pos+4;
	}

	private static int writeLong(long l, byte[] b, int pos) {
		b[pos] = (byte)(l >> 56);
		b[pos+1] = (byte)(l >> 48);
		b[pos+2] = (byte)(l >> 40);
		b[pos+3] = (byte)(l >> 32);
		b[pos+4] = (byte)(l >> 24);
		b[pos+5] = (byte)(l >> 16);
		b[pos+6] = (byte)(l >> 8);
		b[pos+7] = (byte)l;
		return pos+8;
	}

	private static int writeDouble(double d, byte[] b, int pos) {
		return writeLong(Double.doubleToRawLongBits(d), b, pos);
	}

	private static byte readByte(byte[] b, int pos) {
		return b[pos];
	}

	private static int readInt(byte[] b, int pos) {
		return (((b[pos] & 0xff) << 24) |
				((b[pos+1] & 0xff) << 16) |
				((b[pos+2] & 0xff) <<  8) |
				((b[pos+3] & 0xff) <<  0));
	}

	private static long readLong(byte[] b, int pos) {
		return ((((long) b[pos] & 0xff) << 56) |
				(((long) b[pos+1] & 0xff) << 48) |
				(((long) b[pos+2] & 0xff) << 40) |
				(((long) b[pos+3] & 0xff) << 32) |
				(((long) b[pos+4] & 0xff) << 24) |
				(((long) b[pos+5] & 0xff) << 16) |
				(((long) b[pos+6] & 0xff) <<  8) |
				(((long) b[pos+7] & 0xff) <<  0));
	}

	private static double readDouble(byte[] b, int pos) {
		return Double.longBitsToDouble(ShmMessage.readLong(b, pos));
	}

	public byte[] constructAndGetHeader() {
		int size = Byte.BYTES + Integer.BYTES + Long.BYTES; // type of msg + type + port
		switch (msgtype) {
			case 1:
				size += Byte.BYTES;
				break;
			case 2:
				size += Integer.BYTES;
				break;
			case 3:
				size += Long.BYTES;
				break;
			case 4:
				size += Double.BYTES;
				break;
			default:
				break;
		}

		byte[] buf = new byte[size];
		int pos = ShmMessage.writeByte(msgtype, buf, 0);
		pos = ShmMessage.writeInt(ShmMessage.typeToInt(this.type), buf, pos);
		pos = ShmMessage.writeLong(port, buf, pos);
		switch (msgtype) {
			case 1:
				Boolean b = (Boolean) msg;
				if (b.booleanValue()) ShmMessage.writeByte((byte)1, buf, pos); else ShmMessage.writeByte((byte)0, buf, pos);
				break;
			case 2:
				Integer i = (Integer)msg;
				ShmMessage.writeInt(i, buf, pos);
				break;
			case 3:
				Long l = (Long)msg;
				ShmMessage.writeLong(l, buf, pos);
				break;
			case 4:
				Double d = (Double)msg;
				ShmMessage.writeDouble(d, buf, pos);
				break;
			default:
				break;
		}

		return buf;
	}

	public static boolean objectInHeader(byte[] h) {
		byte msgtype = ShmMessage.readByte(h, 0);
		return msgtype != 0;
	}
}
