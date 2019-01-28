package org.apache.spark.sgx;

public abstract class SgxCommunicator {
	
	/**
	 * Send one object.
	 * @param o the object to send
	 */
	public final void sendOne(Object o) {
		write(o);
	}
	
	/**
	 * Receive one object.
	 * @return the received object
	 */
	public final Object recvOne() {
	try {
		throw new Exception("recvOne");
	}catch (Exception e) {
		e.printStackTrace();
	}

		System.err.println("SgxCommunicator.recvOne1");
		Object o = read();
		System.err.println("SgxCommunicator.recvOne2, o = " + o);
		return o;
	}
	
	/**
	 * Send one object and wait for the result of type T.
	 * @param o the object to send
	 * @return the result object of type T
	 */
	@SuppressWarnings("unchecked")
	public final <T> T sendRecv(Object o) {
System.err.println("SgxCommunicator.sendRecv1 o = " + o);
		sendOne(o);
		System.err.println("SgxCommunicator.sendRecv2");
		Object ret = recvOne();
		System.err.println("SgxCommunicator.sendRecv3, ret = " + ret);
		return (T) ret;
	}
	
	public abstract void close();
	
	protected abstract Object read();
	
	protected abstract void write(Object o);
}
