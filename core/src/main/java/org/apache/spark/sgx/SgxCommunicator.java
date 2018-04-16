package org.apache.spark.sgx;

import java.util.logging.Logger;

public abstract class SgxCommunicator {
	
	/**
	 * Send one object.
	 * @param o the object to send
	 */
	public final void sendOne(Object o) {
		Logger.getLogger("debug").info("SgxCommunicator.sendOne o: " + o);
		write(o);
		Logger.getLogger("debug").info("SgxCommunicator.sendOne done: " + o);
	}
	
	/**
	 * Receive one object.
	 * @return the received object
	 */
	public final Object recvOne() {
		Logger.getLogger("debug").info("SgxCommunicator.recvOne");
		Object o = read();
		Logger.getLogger("debug").info("SgxCommunicator.recvOne: " + o); 
		return o; 
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
