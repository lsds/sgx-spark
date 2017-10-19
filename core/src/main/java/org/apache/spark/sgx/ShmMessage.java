package org.apache.spark.sgx;

import java.io.Serializable;

class ShmMessage implements Serializable {
	private static final long serialVersionUID = 7329847091123L;
	
	private final EShmMessageType type;
	private final Object msg;
	private final long port;
	
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
	
	public String toString() {
		return getClass().getSimpleName() + "(type=" + type + ", msg=" + msg + ", port=" + port + ")";
	}
}