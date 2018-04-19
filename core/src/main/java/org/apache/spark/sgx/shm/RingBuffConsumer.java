package org.apache.spark.sgx.shm;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.data.AlignedMappedDataBuffer;
import org.apache.spark.sgx.data.MappedDataBuffer;

class RingBuffConsumer {
	
	private AlignedMappedDataBuffer buffer;

	RingBuffConsumer(MappedDataBuffer buffer) {
		this.buffer = new AlignedMappedDataBuffer(buffer);
		System.out.println("Creating " + this);
	}

	Object read() {
		Object obj = null;
		boolean exception = false;

		do {
			try {
				System.out.println("Reading object");
				int pos = buffer.position();
				System.out.println("position=" + pos);
				int len = buffer.waitWhile(0);
				System.out.println("Reading object done waiting");
				byte[] bytes = new byte[len];
				buffer.get(bytes);
				buffer.putInt(pos, 0);
				obj = Serialization.deserialize(bytes);
				System.out.println("Reading object done: " + obj);
				
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
