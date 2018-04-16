package org.apache.spark.sgx.data;

import org.apache.spark.sgx.types.DataType;
import org.apache.spark.sgx.utils.IObjectFactory;

public class CustomDataBufferFactory implements IObjectFactory<DataBuffer> {
	
	private int capacity;
	private DataType type;
	
	public CustomDataBufferFactory (int capacity, DataType type) {
		
		this.capacity = capacity;
		this.type = type;
	}
	
	@Override
	public DataBuffer newInstance () {
		
		return new DataBuffer (0, capacity, type);
	}
	
	@Override
	public DataBuffer newInstance (int ndx) {
		
		return new DataBuffer (ndx, capacity, type);
	}
}
