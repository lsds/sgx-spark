package org.apache.spark.sgx.data;

public class AlignedMappedDataBuffer {
	
	private MappedDataBuffer buffer;
	
	private int position = 0;
	
	public AlignedMappedDataBuffer(MappedDataBuffer buffer) {
		this.buffer = buffer;
	}
	
	private void movePosition(int plus) {
		int r = plus % 64;
		position += (r == 0) ? plus : plus + (64 - r);
	}
	
	public void putInt(int position, int value) {
		if (position % 64 != 0) {
			throw new RuntimeException("Index is not aligned");
		}
		buffer.putInt(position, value);
	}
	
	public void putInt(int value) {
		putInt(position, value);
		movePosition(64);
	}
	
	public int getInt() {
		int i = buffer.getInt(position);
		movePosition(64);
		return i;
	}
	
	private static int MAX_WAIT = 256;
	private static int MIN_WAIT = 1;
	
	public int waitWhile(int value) throws InterruptedException {
		int wait = MIN_WAIT;
		int res;
		System.out.println("waiting ");
		while ((res = buffer.getInt(position)) == value) {
			System.out.print(".");			
			Thread.sleep(wait);			
			wait = Math.min(wait << 1, MAX_WAIT);
		}
		System.out.println(".");
		movePosition(64);
		return res;
	}
	
	public int waitUntil(int value) throws InterruptedException {
		int wait = MIN_WAIT;
		int res;
		while ((res = buffer.getInt(position)) != value) {
			Thread.sleep(wait);
			wait = Math.min(wait << 1, MAX_WAIT);
		}
		movePosition(64);
		return res;
	}	
	
	public void put (byte[] value) {
		buffer.put(position, value);
		movePosition(value.length);
	}
	
	public byte[] get (byte[] value) {
		byte[] r = buffer.get(position, value);
		movePosition(value.length);
		return r;
	}	
	
	public int position() {
		return position;
	}
}
