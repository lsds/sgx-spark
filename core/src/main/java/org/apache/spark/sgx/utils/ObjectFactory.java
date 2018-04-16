package org.apache.spark.sgx.utils;

public interface ObjectFactory<T> {
	
	public T newInstance();
	
	public T newInstance(int ndx);
}
