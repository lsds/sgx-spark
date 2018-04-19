package org.apache.spark.sgx.data;

public interface IDataBuffer {

	public int capacity ();
	
	public byte  get(int index);
	public int   getInt(int index);
	public long  getLong(int index);
	
	public void put(int index, byte value);
	public void put(int index, byte[] value);
	public void putInt(int index, int value);
	public void putLong(int index, long value);
}
