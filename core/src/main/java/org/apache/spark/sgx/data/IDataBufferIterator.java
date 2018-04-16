package org.apache.spark.sgx.data;

public interface IDataBufferIterator {
	
	public int next ();
	
	public boolean hasNext ();
	
	public IDataBufferIterator reset ();
}
