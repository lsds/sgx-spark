package org.apache.spark.sgx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.nustaq.serialization.FSTConfiguration;

public class Serialization {
	
	private static ISerialization getSerializer() {
		switch (SgxSettings.SERIALIZER().toLowerCase()) {
			case "java":
			case "default":
				return new JavaSerialization();
			case "apache":
			case "commons":
				return new CommonsSerialization();
			case "fst":
			default:
				return new FSTSerialization();
		}
	}
	
	private static ISerialization serializer = getSerializer();
	
	public static byte[] serialize(Object o) throws Exception {
		return serializer.serialize(o);
	}
	
	public static Object deserialize(byte[] bytes) throws Exception {
		return serializer.deserialize(bytes);
	}
}

interface ISerialization {
	byte[] serialize(Object o) throws IOException;
	Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException;
}

class JavaSerialization implements ISerialization {
	public byte[] serialize(Object o) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(stream);
		oos.writeObject(o);
		oos.flush();
		byte[] result = stream.toByteArray();
		oos.close();
		stream.close();
		
		return result;
	}

	public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream s = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(s);
		Object value = ois.readObject();
		ois.close();
		s.close();
		return value;
	}
}

class CommonsSerialization implements ISerialization {
	public byte[] serialize(Object o) throws IOException {
		return SerializationUtils.serialize((Serializable) o);
	}

	public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		return SerializationUtils.deserialize(bytes);
	}
}

//class KryoSerialization implements ISerialization {
//	
//	private Kryo kryo = new Kryo();
//	
//	public byte[] serialize(Object o) throws IOException {
//		ByteArrayOutputStream stream = new ByteArrayOutputStream();
//		Output output = new Output(stream);
//		kryo.writeObject(output, o);
//		output.close();
//		return stream.toByteArray();
//	}
//
//	public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//		Input input = new Input(new ByteArrayInputStream(bytes));
//		kryo.readObject(input, 
//	}
//}

class FSTSerialization implements ISerialization {
	private FSTConfiguration conf = FSTConfiguration.createUnsafeBinaryConfiguration();
	
	public byte[] serialize(Object o) {
		return conf.asByteArray(o);
	}

	public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		return conf.asObject(bytes);
	}
}