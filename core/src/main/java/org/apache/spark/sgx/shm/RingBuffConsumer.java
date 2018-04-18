package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.data.IDataBuffer;

class RingBuffConsumer {
	
	private IDataBuffer buffer;
	
	private static int MAX_WAIT = 16;
	private static int MIN_WAIT = 1;
	
	private int position = 0;

	RingBuffConsumer(IDataBuffer buffer) {
		this.buffer = buffer;
	}

	Object read() {
		Object obj = null;
		boolean exception = false;

		do {
			try {
				int len;
				int wait = MIN_WAIT;
				while ((len = buffer.getInt(position)) == 0) {
					Thread.sleep(wait);
					wait = Math.min(wait << 1, MAX_WAIT);
				}
				position += 4;
				byte[] bytes = new byte[len];
				for (int i = 0; i < len; i++) {
					bytes[i] = buffer.get(position++);					
				}
				obj = Serialization.deserialize(bytes);
				
			} catch (Exception e) {
				e.printStackTrace();
				exception = true;
			}
		} while (obj == null && !exception);
		return obj;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(buffer=" + buffer + ")";
	}
}
