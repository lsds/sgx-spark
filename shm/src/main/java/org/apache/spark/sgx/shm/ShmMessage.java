package org.apache.spark.sgx.shm;

import java.io.Serializable;

public class ShmMessage implements Serializable {
	private static final long serialVersionUID = 7329847091123L;
	
	private EShmMessageType type;
	private Object msg;
	private long port;

	ShmMessage(EShmMessageType type, Object msg, long port) {
		this.type = type;
		this.msg = msg;
		this.port = port;
	}
	
	EShmMessageType getType() {
		return type;
	}

	public Object getMsg() {
		return msg;
	}

	long getPort() {
		return port;
	}
	
	public String toString() {
		return getClass().getSimpleName() + "(type=" + type + ", msg=" + msg + ", port=" + port + ")";
	}
}
