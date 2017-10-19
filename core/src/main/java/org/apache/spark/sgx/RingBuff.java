package org.apache.spark.sgx;

import java.io.IOException;

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
				success = RingBuffLibWrapper.write_msg(handle, Serialization.serialize(o));
			} catch (IOException e) {
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
				System.out.println(this + " waiting 1");
				obj = Serialization.deserialize(RingBuffLibWrapper.read_msg(handle));
				System.out.println(this + " waiting 2");
			} catch (ClassNotFoundException | IOException e) {
				System.out.println(this + " waiting 3");
				e.printStackTrace();
				exception = true;
				System.out.println(this + " waiting 4");
			}
			System.out.println(this + " waiting 5");
		} while (obj == null && !exception && blocking);
		System.out.println(this + " waiting 6");
		return obj;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
