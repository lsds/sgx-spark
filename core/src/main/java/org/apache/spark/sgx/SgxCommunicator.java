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
		return read();
	}
	
	/**
	 * Send one object and wait for the result of type T.
	 * @param o the object to send
	 * @return the result object of type T
	 */
	@SuppressWarnings("unchecked")
	public final <T> T sendRecv(Object o) {
		sendOne(o);
		return (T) recvOne();
	}
	
	public abstract void close();
	
	protected abstract Object read();
	
	protected abstract void write(Object o);
}
