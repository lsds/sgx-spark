package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;

import java.util.Arrays;

public class RingBuffConsumer extends RingBuffConsumerRaw {
	private ISerialization serializer;
	private Object readlock = new Object();

	public RingBuffConsumer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 1);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	@SuppressWarnings("unchecked")
	// used by SgxIteratorConsumer.scala
	public <T> T readAny() {
		try {
			byte[] b;
			Object o;
			synchronized(readlock) {
				b = readBytes();

				System.err.println("RingBuffConsumer: reading a new message from an array of size " + b.length);
				//System.err.println("The read message of size " + b.length + " is "+ Arrays.toString(b));

				o = serializer.deserialize(b);
				System.err.println("RingBuffConsumer: read object " + o + " of size " + b.length);
			}

/*
			try {
				throw new Exception("read object " + o + ", " + " of size " + b.length);
			} catch (Exception e) {
				e.printStackTrace();
			}
			*/

			return (T) o;
			//return (T) serializer.deserialize(b);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public ShmMessage readShmMessage() {
		try {
			byte[] b = null;
			Object o = null;

			if (ShmMessage.SMALL_MESSAGE_INLINE_OPTIMIZATION) {
				byte[] h;
				synchronized(readlock) {
					h = readBytes();
					if (!ShmMessage.objectInHeader(h)) {
						b = readBytes();
					}
				}

				if (b != null) {
					o = serializer.deserialize(b);
				}
				ShmMessage m = new ShmMessage(h, o);
				return m;
			} else {
				synchronized(readlock) {
					b = readBytes();
				}

				o = serializer.deserialize(b);

/*
				try {
					throw new Exception("read object " + o + ", " + " of size " + b.length);
				} catch (Exception e) {
					e.printStackTrace();
				}
				*/

				return (ShmMessage) o;
			}

>>>>>>> master
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
