package org.apache.spark.sgx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.nustaq.serialization.FSTConfiguration;

public class Serialization {
	
	public static byte[] serialize(Object o) throws Exception {
		return DefaultSerialization.serialize(o);
	}
	
	public static Object deserialize(byte[] bytes) throws Exception {
		return DefaultSerialization.deserialize(bytes);
	}
}

class DefaultSerialization {
	public static byte[] serialize(Object o) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(stream);
		oos.writeObject(o);
		oos.close();
		return stream.toByteArray();
	}

	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
		Object value = ois.readObject();
		ois.close();
		return value;
	}
}

class FSTSerialization {
	private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
	
	public static byte[] serialize(Object o) {
		return conf.asByteArray(o);
	}

	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		return conf.asObject(bytes);
	}
}