package org.apache.spark.sgx.shm;

import java.util.logging.Logger;

import org.apache.spark.sgx.Serialization;

class RingBuff {
	private long handle;
	private boolean blocking;

	RingBuff(long handle, boolean blocking) {
		this.handle = handle;
		this.blocking = blocking;
	}

	boolean write(Object o) {
		boolean success = false;
		boolean exception = false;
		
		do {
			try {
				Logger.getLogger("debug").info("RingBuff.write length: " + Serialization.serialize(o).length);
				success = RingBuffLibWrapper.write_msg(handle, Serialization.serialize(o));
				Logger.getLogger("debug").info("RingBuff.write done: " + success);
			} catch (Exception e) {
				e.printStackTrace();
				exception = true;
			}
		} while (!success && !exception && blocking);

		return success;
	}

	Object read() {
		Object obj = null;
		boolean exception = false;

		do {
			try {
				obj = Serialization.deserialize(RingBuffLibWrapper.read_msg(handle));
			} catch (Exception e) {
				e.printStackTrace();
				exception = true;
			}
		} while (obj == null && !exception && blocking);

		return obj;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
