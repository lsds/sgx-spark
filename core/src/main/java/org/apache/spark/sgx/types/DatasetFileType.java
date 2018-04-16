package org.apache.spark.sgx.types;

public enum DatasetFileType {
	
	EXAMPLES (0), LABELS (1);
	
	private int id;
	
	DatasetFileType (int id) {
		
		this.id = id;
	}
	
	public int getId () {
		return id;
	}
}
