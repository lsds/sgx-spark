package org.apache.spark.sgx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.nustaq.serialization.FSTConfiguration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

import java.util.logging.Logger;

public class Serialization {
	
	private static ISerialization serializer = new FSTSerialization();
	
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
 		Logger.getLogger("debug").info("JavaSerialization.deserialize0 hash: " + java.util.Arrays.hashCode(bytes));
		Logger.getLogger("debug").info("JavaSerialization.deserialize1 byte length: " + bytes.length);
		ByteArrayInputStream s = new ByteArrayInputStream(bytes);
		Logger.getLogger("debug").info("JavaSerialization.deserialize2: stream1 available: " + s.available());
		ObjectInputStream ois = new ObjectInputStream(s);
		Logger.getLogger("debug").info("JavaSerialization.deserialize3: stream2 available: " + ois.available());
		try {
		Object value = ois.readObject();
		Logger.getLogger("debug").info("JavaSerialization.deserialize4: " + value);
		ois.close();
		s.close();
		Logger.getLogger("debug").info("JavaSerialization.deserialize5: closed");
		return value;
		} catch (Exception e) {
			Logger.getLogger("debug").info("Exception: " + e.getMessage());
		}
		return null;
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