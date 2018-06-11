package org.apache.spark.sgx;

import java.io.IOException;

public interface ISerialization {
	byte[] serialize(Object o) throws IOException;
	Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException;
}