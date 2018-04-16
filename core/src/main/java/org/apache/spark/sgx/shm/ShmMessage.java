package org.apache.spark.sgx.shm;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

class ShmMessage implements Externalizable {
	private static final long serialVersionUID = 7329847091123L;
	
	private EShmMessageType type;
	private Object msg;
	private long port;
	
	public ShmMessage() {
	}

	ShmMessage(EShmMessageType type, Object msg, long port) {
		this.type = type;
		this.msg = msg;
		this.port = port;
	}
	
	EShmMessageType getType() {
		return type;
	}

	Object getMsg() {
		return msg;
	}

	long getPort() {
		return port;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(type);
		out.writeObject(msg);
		out.writeLong(port);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		type = (EShmMessageType) in.readObject();
		msg = in.readObject();
		port = in.readLong();
	}
	
	public String toString() {
		return getClass().getSimpleName() + "(type=" + type + ", msg=" + msg + ", port=" + port + ")";
	}
}
