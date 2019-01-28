package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.ISerialization;
import java.util.Arrays;

public class RingBuffProducer extends RingBuffProducerRaw {
	private ISerialization serializer;
	private Object writelock = new Object();
	
	public RingBuffProducer(MappedDataBuffer buffer, ISerialization serializer) {
		super(buffer, 1);
		if (serializer == null) {
			throw new RuntimeException("Must specify a serializer in order to write objects.");
		}
		this.serializer = serializer;
	}

	// SgxIteratorProvider calls this generic write
	public void writeAny(Object o) {
		try {
			byte[] b = serializer.serialize(o);
			synchronized(writelock) {

/*
				try {
					throw new Exception("write object " + o + ", " + o.getClass().getSimpleName() + " of size " + b.length);
				} catch (Exception e) {
					e.printStackTrace();
				}
				*/

				//System.err.println("The written message " + o + " of size " + b.length + " is "+ Arrays.toString(b));

				write(b);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// This is our custom write for ShmMessage only
	public void writeShmMessage(ShmMessage m) {
		byte[] b = null;
		try {
			if (ShmMessage.SMALL_MESSAGE_INLINE_OPTIMIZATION) {
				byte[] h = m.constructAndGetHeader();
				if (m.msgtype == 0) {
					b = serializer.serialize(m.getMsg());
				}

				synchronized(writelock) {
					write(h);
					if (b != null) {
						write(b);
					}
				}
			} else {
				b = serializer.serialize(m);
				synchronized(writelock) {
				/*
					try {
						throw new Exception("write object " + m + ", " + m.getClass().getSimpleName() + " of size " + b.length);
					} catch (Exception e) {
						e.printStackTrace();
					}
					*/

					write(b);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
